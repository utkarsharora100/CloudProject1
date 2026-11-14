from common.observability import install as install_observability
"""Local-only API; identity selectors are scoping, cloud authentication is a later milestone."""
import atexit
from flask import jsonify, request
from sqlalchemy import select, and_, or_, func
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest, HTTPException
from order_service.models import db
from order_service.storage import create_storage_app
from task_scheduler.coordination import Coordination
from task_scheduler.domain import uuid_id, lease_request, request_key
from notification_service.domain import recipient, subscription_request
from notification_service.models import Message, Attempt, Subscription
from notification_service.store import NotificationStore, summary, stamp

def create_app(config=None, *, coordination=None, transport=None, clock=None):
    app=create_storage_app(config)
    if coordination is None:
        coordination=Coordination().start()
        atexit.register(coordination.close)
    store=NotificationStore(coordination,transport=transport,clock=clock)
    app.extensions["notification_store"]=store

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"]="no-store"
        response.headers["X-Content-Type-Options"]="nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        db.session.rollback()
        return jsonify({"error":error.description}),error.code

    @app.errorhandler(SQLAlchemyError)
    def sql_error(error):
        db.session.rollback()
        app.logger.error("Notification database operation failed.")
        return jsonify({"error":"Notification data is temporarily unavailable; retry the same request."}),503

    @app.errorhandler(Exception)
    def unexpected(error):
        db.session.rollback()
        app.logger.error("Notification operation failed.")
        return jsonify({"error":"Notification operation could not be confirmed."}),500

    def who():
        selectors=[key for key in ("customer_id","courier_id") if key in request.args]
        if len(selectors)!=1:
            raise BadRequest("Provide exactly one customer_id or courier_id.")
        name=selectors[0]
        return recipient("customer" if name=="customer_id" else "courier",request.args[name])

    def query_fields(extra=()):
        if set(request.args)-{"customer_id","courier_id",*extra}:
            raise BadRequest("Unsupported notification query.")

    @app.get("/health")
    def health():
        db.session.execute(select(Message.id).limit(1)).first()
        coordination.workers()
        return jsonify({"service":"notification-service","status":"healthy",
                        "simulation":True,"channel":"in_app","zookeeper_connected":coordination.live})

    @app.post("/notifications/orders/<value>/subscription")
    def subscribe(value):
        result,replay=store.subscribe(uuid_id(value),subscription_request(request.get_json()),
            request_key(request.headers.get("Idempotency-Key")))
        response=jsonify(result);response.status_code=201
        response.headers["Idempotency-Replayed"]=str(replay).lower()
        return response

    @app.get("/notifications/orders/<value>/subscription")
    def subscription(value):
        query_fields()
        kind,identity=who()
        row=db.session.get(Subscription,uuid_id(value))
        if row is None or kind!="customer" or str(row.customer_id)!=identity:
            from werkzeug.exceptions import NotFound
            raise NotFound("Subscription not found for this customer.")
        return jsonify({"order_id":row.order_id,"customer_id":row.customer_id,"scenario":row.scenario,
            "courier_updates":row.courier_updates,"simulation":True,"channel":"in_app"})

    @app.get("/notifications")
    def messages():
        query_fields({"order_id","state","limit","after"})
        kind,identity=who()
        query=select(Message).where(Message.recipient_type==kind,Message.recipient_id==identity)
        if request.args.get("order_id"):query=query.where(Message.order_id==uuid_id(request.args["order_id"]))
        state=request.args.get("state")
        if state:
            if state not in {"PENDING","SENDING","RETRY_WAIT","SENT","FAILED"}:raise BadRequest("Unknown notification state.")
            query=query.where(Message.state==state)
        limit=request.args.get("limit","20")
        if not limit.isascii() or not limit.isdigit() or not 1<=int(limit)<=50:
            raise BadRequest("limit must be between 1 and 50.")
        count=db.session.execute(select(func.count()).select_from(query.subquery())).scalar_one()
        if request.args.get("after"):
            cursor=store.message(uuid_id(request.args["after"]),(kind,identity))
            if request.args.get("order_id") and cursor.order_id!=request.args["order_id"]:
                raise BadRequest("Cursor is outside the requested order.")
            if state and cursor.state!=state:raise BadRequest("Cursor is outside the requested state.")
            query=query.where(or_(Message.created_at>cursor.created_at,
                and_(Message.created_at==cursor.created_at,Message.sequence>cursor.sequence),
                and_(Message.created_at==cursor.created_at,Message.sequence==cursor.sequence,Message.id>cursor.id)))
        rows=db.session.execute(query.order_by(Message.created_at,Message.sequence,Message.id).limit(int(limit)+1)).scalars().all()
        page=rows[:int(limit)]
        unread=db.session.execute(select(func.count()).select_from(Message).where(
            Message.recipient_type==kind,Message.recipient_id==identity,Message.state=="SENT",Message.read_at.is_(None))).scalar_one()
        return jsonify({"notifications":[summary(row) for row in page],"total_matching":count,
            "recipient_unread_count":unread,"next_after":page[-1].id if len(rows)>int(limit) else None})

    @app.get("/notifications/<value>")
    def detail(value):
        query_fields()
        return jsonify(summary(store.message(uuid_id(value),who())))

    @app.get("/notifications/<value>/attempts")
    def attempts(value):
        query_fields()
        message=store.message(uuid_id(value),who())
        rows=db.session.execute(select(Attempt).where(Attempt.notification_id==message.id).order_by(Attempt.number)).scalars().all()
        return jsonify({"notification_id":message.id,"attempts":[{
            "number":row.number,"retry_round":row.retry_round,"task_id":row.task_id,"generation":row.generation,
            "outcome":row.outcome,"started_at":stamp(row.started_at),"finished_at":stamp(row.finished_at)} for row in rows]})

    def command(value, action):
        result,replay=store.command(uuid_id(value),action,request.get_json(),request_key(request.headers.get("Idempotency-Key")))
        response=jsonify(result);response.headers["Idempotency-Replayed"]=str(replay).lower()
        return response

    app.add_url_rule("/notifications/<value>/read","read",lambda value:command(value,"read"),methods=["POST"])
    app.add_url_rule("/notifications/<value>/retry","retry",lambda value:command(value,"retry"),methods=["POST"])

    @app.post("/internal/tasks/<value>/process")
    def process(value):
        return jsonify(store.process(uuid_id(value),lease_request(request.get_json())))
    install_observability(app,"notification")
    return app

def main():
    create_app().run(host="0.0.0.0",port=8088,debug=False)

if __name__=="__main__":
    main()
