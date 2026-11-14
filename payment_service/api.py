from common.observability import install as install_observability
"""Local-only Payment API; private task proofs stay out of responses and logs."""
import atexit
import re
from flask import jsonify, request
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest, HTTPException, NotFound
from order_service.domain import order_id
from order_service.models import db, timestamp
from order_service.storage import create_storage_app
from payment_service.domain import action_request
from payment_service.models import PaymentEvent, ProviderOperation
from payment_service.provider import SimulatedProvider
from payment_service.store import PaymentStore
from task_scheduler.coordination import Coordination
from task_scheduler.domain import lease_request, request_key

def create_app(config=None, *, coordination=None, provider=None, clock=None):
    app = create_storage_app(config)
    if coordination is None:
        coordination = Coordination().start()
        atexit.register(coordination.close)
    if provider is None:
        with app.app_context():
            provider = SimulatedProvider(db.engine)
    store = PaymentStore(coordination, provider, clock=clock)
    app.extensions["payment_store"] = store

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
        app.logger.error("Payment database operation failed.")
        return jsonify({"error": "Payment data is temporarily unavailable; retry the same request."}), 503

    @app.errorhandler(Exception)
    def unexpected(error):
        db.session.rollback()
        app.logger.error("Payment operation failed.")
        return jsonify({"error": "Payment operation could not be confirmed."}), 500

    @app.get("/health")
    def health():
        db.session.execute(select(ProviderOperation.id).limit(1)).first()
        coordination.workers()
        return jsonify({"service": "payment-service", "status": "healthy",
                        "simulation": True, "zookeeper_connected": coordination.live})

    @app.get("/payments/orders/<value>")
    def detail(value):
        return jsonify(store.detail(order_id(value)))

    @app.post("/payments/orders/<value>")
    def create(value):
        return action(value, "create", create=True)

    @app.post("/payments/orders/<value>/retry")
    def retry(value):
        return action(value, "retry")

    @app.post("/payments/orders/<value>/cancel")
    def cancel(value):
        return action(value, "cancel")

    def action(value, kind, *, create=False):
        body, code, replayed = store.action(order_id(value), kind, action_request(request.get_json(), create=create),
                                           request_key(request.headers.get("Idempotency-Key")))
        response = jsonify(body)
        response.status_code = code
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        response.headers["Location"] = "/payments/orders/" + body["order_id"]
        return response

    @app.get("/payments/orders/<value>/events")
    def events(value):
        payment = store.payment(order_id(value))
        if payment is None:
            raise NotFound("Payment not found.")
        if set(request.args) - {"after", "limit"}:
            raise BadRequest("Unsupported payment history query.")
        after, limit = request.args.get("after", "0"), request.args.get("limit", "100")
        if not re.fullmatch(r"[0-9]{1,18}", after) or not re.fullmatch(r"[0-9]{1,3}", limit) or not 1 <= int(limit) <= 200:
            raise BadRequest("Payment history pagination is invalid.")
        rows = db.session.execute(select(PaymentEvent).where(PaymentEvent.payment_id == payment.id,
            PaymentEvent.id > int(after)).order_by(PaymentEvent.id).limit(int(limit) + 1)).scalars().all()
        page = rows[:int(limit)]
        return jsonify({"events": [{"event_id": r.id, "event_type": r.event_type,
            "attempt_number": r.attempt_number, "occurred_at": timestamp(r.occurred_at)} for r in page],
            "next_after": page[-1].id if len(rows) > int(limit) else None})

    @app.post("/internal/tasks/<value>/process")
    def process(value):
        return jsonify(store.process(order_id(value), lease_request(request.get_json())))

    install_observability(app,"payment")
    return app

if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8086, debug=False, use_reloader=False)
