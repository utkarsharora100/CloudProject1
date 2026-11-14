from common.observability import install as install_observability
"""Validated local Order API; order/event/outbox/idempotency commit atomically."""
import atexit
from copy import deepcopy
import re
import uuid
from flask import jsonify, request
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.exceptions import BadRequest, Conflict, HTTPException, NotFound, ServiceUnavailable
from order_service.dependencies import OrderDependencies
from order_service.events import append_event
from payment_service.models import Payment
from kitchen_workflow.models import Ticket as KitchenTicket
from delivery_assignment_service.models import Job as DeliveryJob
from order_service.domain import (EVENT_TYPES, create_request, idempotency_key, order_id,
                                  positive_int, request_hash, transition_request, validate_transition)
from order_service.models import Idempotency, Order, OrderEvent, Outbox, db, timestamp, utc_now
from order_service.storage import create_storage_app


def create_app(config=None, *, dependencies=None):
    app = create_storage_app(config)
    if dependencies is None:
        dependencies = OrderDependencies()
        atexit.register(dependencies.close)

    def rollback():
        try:
            db.session.rollback()
        except SQLAlchemyError:
            db.session.remove()

    @app.after_request
    def privacy_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        rollback()
        response = error.get_response()
        response.data = app.json.dumps({"error": error.description})
        response.content_type = "application/json"
        return response

    @app.errorhandler(SQLAlchemyError)
    def database_error(error):
        rollback()
        app.logger.error("Order database operation failed.")
        return jsonify({"error": "Order data is temporarily unavailable."}), 503

    @app.errorhandler(Exception)
    def unexpected_error(error):
        rollback()
        app.logger.error("Unexpected Order Service operation failure.")
        return jsonify({"error": "Order operation could not be completed."}), 500

    def cached(scope, key, digest, *, release=True):
        previous = db.session.get(Idempotency, (scope, key))
        if previous is None:
            if release:
                rollback()
            return None
        if previous.request_hash != digest:
            raise Conflict("This Idempotency-Key was already used with a different request.")
        saved = (deepcopy(previous.response), previous.status_code)
        rollback()
        return saved

    def saved_response(saved, replayed=False):
        body, status = saved
        response = jsonify(body)
        response.status_code = status
        response.headers["Location"] = f"/orders/{body['order_id']}"
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        response.set_etag(str(body["version"]))
        return response

    def recover_race(scope, key, digest):
        rollback()
        previous = cached(scope, key, digest)
        if previous is None:
            raise ServiceUnavailable("Order write could not be completed; retry with the same key.")
        return saved_response(previous, replayed=True)

    def find_order(value, *, lock=False):
        query = select(Order).where(Order.id == order_id(value))
        if lock:
            query = query.with_for_update()
        result = db.session.execute(query).scalar_one_or_none()
        if result is None:
            raise NotFound("Order not found.")
        return result

    @app.get("/health")
    def health():
        db.session.execute(select(Order.id).limit(1)).first()
        db.session.execute(select(Outbox.event_id).limit(1)).first()
        return jsonify({"status": "healthy", "service": "order-service", "publication": "transactional-outbox"})

    @app.post("/orders")
    def create_order():
        data = create_request(request.get_json())
        key = idempotency_key(request.headers.get("Idempotency-Key"))
        scope = f"create:{data['customer_id']}"
        digest = request_hash(data)
        previous = cached(scope, key, digest)
        if previous is not None:
            return saved_response(previous, replayed=True)
        quote = dependencies.quote(data)
        try:
            now = utc_now()
            order = Order(customer_id=data["customer_id"], address_id=data["address_id"],
                          restaurant_id=data["restaurant_id"], status="PENDING_PAYMENT", version=1,
                          created_at=now, updated_at=now, **deepcopy(quote))
            db.session.add(order)
            db.session.flush()
            append_event(order, "OrderCreated", None)
            body = order.summary()
            db.session.add(Idempotency(scope=scope, key=key, request_hash=digest,
                                       order_id=order.id, response=body, status_code=201))
            db.session.commit()
        except IntegrityError:
            return recover_race(scope, key, digest)
        return saved_response((body, 201))

    @app.get("/orders/<value>")
    def get_order(value):
        order = find_order(value)
        response = jsonify({"order": order.detail()})
        response.set_etag(str(order.version))
        return response

    @app.get("/orders")
    def list_orders():
        if set(request.args) - {"customer_id", "limit", "after"}:
            raise BadRequest("Unsupported order-list query parameters.")
        customer = request.args.get("customer_id", "")
        limit = request.args.get("limit", "20")
        if not re.fullmatch(r"[0-9]{1,10}", customer):
            raise BadRequest("customer_id is required for order listing.")
        customer_id = positive_int(int(customer), "customer_id")
        if not re.fullmatch(r"[0-9]{1,2}", limit) or not 1 <= int(limit) <= 50:
            raise BadRequest("limit must be between 1 and 50.")
        query = select(Order).where(Order.customer_id == customer_id)
        if request.args.get("after"):
            cursor = db.session.execute(select(Order).where(
                Order.id == order_id(request.args["after"]), Order.customer_id == customer_id)).scalar_one_or_none()
            if cursor is None:
                raise BadRequest("The order cursor does not belong to this customer.")
            query = query.where(or_(Order.created_at > cursor.created_at,
                                    and_(Order.created_at == cursor.created_at, Order.id > cursor.id)))
        rows = db.session.execute(query.order_by(Order.created_at, Order.id).limit(int(limit) + 1)).scalars().all()
        page = rows[:int(limit)]
        return jsonify({"orders": [order.summary() for order in page],
                        "next_after": page[-1].id if len(rows) > int(limit) else None})

    @app.post("/orders/<value>/transitions")
    def transition_order(value):
        target_id = order_id(value)
        data = transition_request(request.get_json())
        key = idempotency_key(request.headers.get("Idempotency-Key"))
        scope = "transition:" + target_id
        digest = request_hash(data)
        previous = cached(scope, key, digest)
        if previous is not None:
            return saved_response(previous, replayed=True)
        try:
            order = find_order(target_id, lock=True)
            concurrent = cached(scope, key, digest, release=False)
            if concurrent is not None:
                return saved_response(concurrent, replayed=True)
            if order.version != data["expected_version"]:
                raise Conflict("Order version changed; reload before retrying.")
            if data["to_state"] in {"PENDING_PAYMENT", "PAID", "PAYMENT_FAILED", "CANCELLED", "REFUND_PENDING", "REFUNDED"}:
                managed = db.session.execute(select(Payment.id).where(Payment.order_id == order.id)).first()
                if managed:
                    raise Conflict("Use the Payment API for this payment-managed order.")
            if data["to_state"] in {"KITCHEN_ACCEPTED", "PREPARING", "READY"}:
                kitchen = db.session.execute(select(KitchenTicket.order_id).where(
                    KitchenTicket.order_id == order.id, KitchenTicket.managed.is_(True))).first()
                if kitchen:
                    raise Conflict("Use the Kitchen API for this kitchen-managed order.")
            if data["to_state"] in {"COURIER_ASSIGNED", "PICKED_UP", "DELIVERED"}:
                delivery = db.session.get(DeliveryJob, order.id)
                kitchen = db.session.get(KitchenTicket, order.id)
                if delivery or (kitchen and kitchen.managed):
                    raise Conflict("Use the Delivery API for this managed order.")
            validate_transition(order.status, data["to_state"])
            old = order.status
            order.status = data["to_state"]
            order.version += 1
            order.updated_at = utc_now()
            append_event(order, EVENT_TYPES[order.status], old)
            body = order.summary()
            db.session.add(Idempotency(scope=scope, key=key, request_hash=digest,
                                       order_id=order.id, response=body, status_code=200))
            db.session.commit()
        except IntegrityError:
            return recover_race(scope, key, digest)
        return saved_response((body, 200))

    @app.get("/orders/<value>/events")
    def events(value):
        order = find_order(value)
        records = db.session.execute(select(OrderEvent, Outbox).join(
            Outbox, Outbox.event_id == OrderEvent.event_id).where(
            OrderEvent.order_id == order.id).order_by(OrderEvent.sequence)).all()
        return jsonify({"order_id": order.id, "events": [{
            "event_id": event.event_id, "sequence": event.sequence, "event_type": event.event_type,
            "from_state": event.from_state, "to_state": event.to_state, "occurred_at": timestamp(event.occurred_at),
            "publication": {"published": row.published_at is not None,
                "published_at": timestamp(row.published_at) if row.published_at else None,
                "attempts": row.attempts, "last_error_code": row.last_error_code,
                "next_attempt_at": timestamp(row.available_at) if row.published_at is None else None}}
            for event, row in records]})

    install_observability(app,"order")
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8081, debug=False, use_reloader=False)
