from common.observability import install as install_observability
"""Local Kitchen API with explicit actions and private worker ownership proofs."""
import atexit
import re
from flask import jsonify, request
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest, HTTPException
from order_service.domain import order_id, identifier
from order_service.models import Order, db
from order_service.storage import create_storage_app
from task_scheduler.coordination import Coordination
from task_scheduler.domain import lease_request, request_key
from kitchen_workflow.domain import action_request, STATES
from kitchen_workflow.models import Ticket, Event
from kitchen_workflow.store import KitchenStore, stamp

def create_app(config=None, *, coordination=None, clock=None):
    app = create_storage_app(config)
    if coordination is None:
        coordination = Coordination().start()
        atexit.register(coordination.close)
    store = KitchenStore(coordination, clock=clock)
    app.extensions["kitchen_store"] = store

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        db.session.rollback()
        response = error.get_response()
        response.data = app.json.dumps({"error": error.description})
        response.content_type = "application/json"
        return response

    @app.errorhandler(SQLAlchemyError)
    def sql_error(error):
        db.session.rollback()
        app.logger.error("Kitchen database operation failed.")
        return jsonify({"error": "Kitchen data is temporarily unavailable; retry the same request."}), 503

    @app.errorhandler(Exception)
    def unexpected(error):
        db.session.rollback()
        app.logger.error("Kitchen operation failed.")
        return jsonify({"error": "Kitchen operation could not be confirmed."}), 500

    @app.get("/health")
    def health():
        db.session.execute(select(Ticket.order_id).limit(1)).first()
        coordination.workers()
        return jsonify({"service": "kitchen-api", "status": "healthy",
                        "zookeeper_connected": coordination.live, "projection": "asynchronous"})

    @app.get("/kitchen/orders/<value>")
    def detail(value):
        return jsonify(store.detail(order_id(value)))

    @app.post("/kitchen/orders/<value>/actions")
    def action(value):
        body, replayed = store.action(order_id(value), action_request(request.get_json()),
                                      request_key(request.headers.get("Idempotency-Key")))
        response = jsonify(body)
        response.status_code = 202
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        response.headers["Location"] = "/kitchen/orders/"+body["ticket"]["order_id"]
        return response

    @app.get("/kitchen/restaurants/<value>/orders")
    def queue(value):
        restaurant = identifier(value, "restaurant_id")
        if set(request.args)-{"state", "limit", "after"}:
            raise BadRequest("Unsupported kitchen queue query.")
        state, limit = request.args.get("state"), request.args.get("limit", "20")
        if state is not None and state not in STATES:
            raise BadRequest("Choose a documented kitchen state.")
        if not re.fullmatch(r"[0-9]{1,2}", limit) or not 1 <= int(limit) <= 50:
            raise BadRequest("limit must be between 1 and 50.")
        query = select(Ticket, Order).join(Order, Order.id == Ticket.order_id).where(Ticket.restaurant_id == restaurant)
        if state is not None:
            query = query.where(Ticket.state == state)
        if request.args.get("after"):
            cursor = db.session.get(Ticket, order_id(request.args["after"]))
            if cursor is None or cursor.restaurant_id != restaurant:
                raise BadRequest("The queue cursor does not belong to this restaurant.")
            query = query.where(or_(Ticket.created_at > cursor.created_at,
                and_(Ticket.created_at == cursor.created_at, Ticket.order_id > cursor.order_id)))
        rows = db.session.execute(query.order_by(Ticket.created_at, Ticket.order_id).limit(int(limit)+1)).all()
        page = rows[:int(limit)]
        return jsonify({"tickets": [store.summary(ticket, order) for ticket, order in page],
                        "next_after": page[-1][0].order_id if len(rows)>int(limit) else None})

    @app.get("/kitchen/orders/<value>/events")
    def events(value):
        ticket = store.ticket(order_id(value))
        if set(request.args)-{"after", "limit"}:
            raise BadRequest("Unsupported kitchen history query.")
        after, limit = request.args.get("after", "0"), request.args.get("limit", "100")
        if not re.fullmatch(r"[0-9]{1,18}", after) or not re.fullmatch(r"[0-9]{1,3}", limit) or not 1 <= int(limit) <= 200:
            raise BadRequest("Kitchen history pagination is invalid.")
        rows = db.session.execute(select(Event).where(Event.order_id == ticket.order_id, Event.id > int(after))
            .order_by(Event.id).limit(int(limit)+1)).scalars().all()
        page = rows[:int(limit)]
        return jsonify({"events": [{"cursor": e.id, "event_id": e.event_id, "version": e.version,
            "event_type": e.event_type, "state": e.state, "order_status": e.order_status,
            "command_id": e.command_id, "reason": e.reason, "occurred_at": stamp(e.occurred_at)} for e in page],
            "next_after": page[-1].id if len(rows)>int(limit) else None})

    @app.post("/internal/tasks/<value>/process")
    def process(value):
        return jsonify(store.process(order_id(value), lease_request(request.get_json())))

    install_observability(app,"kitchen")
    return app

if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8087, debug=False, use_reloader=False)
