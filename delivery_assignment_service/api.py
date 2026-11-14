from common.observability import install as install_observability
"""Local Delivery API: synthetic fleet, durable assignments and private worker proofs."""
import atexit
import re
from flask import jsonify,request
from sqlalchemy import and_,or_,select
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest,HTTPException,Gone
from order_service.models import db
from order_service.storage import create_storage_app
from order_service.domain import order_id,identifier
from task_scheduler.coordination import Coordination
from task_scheduler.domain import lease_request,request_key
from delivery_assignment_service.domain import (zone_request,courier_request,presence_request,dispatch_request,action_request,fields)
from delivery_assignment_service.models import Zone,Courier,Job,Event
from delivery_assignment_service.schema import initialize
from delivery_assignment_service.store import DeliveryStore,stamp

def create_app(config=None,*,coordination=None,clock=None):
    app=create_storage_app(config)
    if app.testing and app.config.get("AUTO_CREATE_SCHEMA"):
        with app.app_context():initialize()
    if coordination is None:
        coordination=Coordination().start();atexit.register(coordination.close)
    store=DeliveryStore(coordination,clock=clock)
    app.extensions["delivery_store"]=store

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"]="no-store"
        response.headers["X-Content-Type-Options"]="nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        db.session.rollback()
        response=error.get_response();response.data=app.json.dumps({"error":error.description})
        response.content_type="application/json";return response

    @app.errorhandler(SQLAlchemyError)
    def sql_error(error):
        db.session.rollback();app.logger.error("Delivery database operation failed.")
        return jsonify({"error":"Delivery data is temporarily unavailable; retry the same request."}),503

    @app.errorhandler(Exception)
    def unexpected(error):
        db.session.rollback();app.logger.error("Delivery operation failed.")
        return jsonify({"error":"Delivery operation could not be confirmed."}),500

    def action(kind,value,data):
        body,code,replayed=store.public_action(kind,value,data,request_key(request.headers.get("Idempotency-Key")))
        response=jsonify(body);response.status_code=code
        response.headers["Idempotency-Replayed"]="true" if replayed else "false"
        return response

    @app.get("/health")
    def health():
        db.session.execute(select(Job.order_id).limit(1)).first();coordination.workers()
        return jsonify({"service":"delivery-assignment","status":"healthy","zookeeper_connected":coordination.live,
                        "publication":"transactional-outbox","simulation":True})

    @app.post("/assign")
    def retired():
        raise Gone("Use the persistent courier and /delivery/orders APIs; direct driver assignment is retired.")

    @app.post("/delivery/zones")
    def zone_create():return action("zone",None,zone_request(request.get_json()))

    @app.get("/delivery/zones/<value>")
    def zone_read(value):return jsonify(store.zone_data(store.zone(identifier(value,"zone"))))

    @app.get("/delivery/capacity")
    def capacity():
        if set(request.args)!={"zone"}:raise BadRequest("Provide one simulation zone.")
        return jsonify(store.capacity(identifier(request.args["zone"],"zone")))

    @app.post("/couriers")
    def courier_create():return action("courier",None,courier_request(request.get_json()))

    @app.get("/couriers/<value>")
    def courier_read(value):return jsonify({"courier":store.courier_data(store.courier(order_id(value)))})

    @app.post("/couriers/<value>/presence")
    def presence(value):return action("presence",order_id(value),presence_request(request.get_json()))

    @app.post("/couriers/<value>/heartbeat")
    def heartbeat(value):
        return action("heartbeat",order_id(value),fields(request.get_json(),set()))

    def pagination(allowed):
        if set(request.args)-set(allowed):raise BadRequest("Unsupported query parameters.")
        limit=request.args.get("limit","20")
        if not re.fullmatch(r"[0-9]{1,2}",limit) or not 1<=int(limit)<=50:
            raise BadRequest("limit must be between 1 and 50.")
        zone=identifier(request.args.get("zone"),"zone");store.zone(zone)
        return zone,int(limit),request.args.get("after")

    @app.get("/couriers")
    def courier_list():
        zone,limit,after=pagination({"zone","limit","after"})
        query=select(Courier).where(Courier.zone==zone)
        if after:
            cursor=store.courier(order_id(after))
            if cursor.zone!=zone:raise BadRequest("Courier cursor belongs to another zone.")
            query=query.where(or_(Courier.created_at>cursor.created_at,and_(Courier.created_at==cursor.created_at,Courier.id>cursor.id)))
        rows=db.session.execute(query.order_by(Courier.created_at,Courier.id).limit(limit+1)).scalars().all()
        page=rows[:limit]
        return jsonify({"couriers":[store.courier_data(c) for c in page],"next_after":page[-1].id if len(rows)>limit else None})

    @app.post("/delivery/orders/<value>")
    def dispatch(value):return action("dispatch",order_id(value),dispatch_request(request.get_json()))

    @app.get("/delivery/orders/<value>")
    def delivery_read(value):return jsonify(store.detail(order_id(value)))

    @app.post("/delivery/orders/<value>/actions")
    def handoff(value):return action("command",order_id(value),action_request(request.get_json()))

    @app.get("/delivery/orders")
    def delivery_list():
        zone,limit,after=pagination({"zone","state","limit","after"})
        state=request.args.get("state")
        if state is not None and state not in {"WAITING","ASSIGNED","PICKED_UP","DELIVERED"}:
            raise BadRequest("Choose a documented delivery state.")
        query=select(Job).where(Job.zone==zone)
        if state:query=query.where(Job.state==state)
        if after:
            cursor=store.job(order_id(after))
            if cursor.zone!=zone:raise BadRequest("Delivery cursor belongs to another zone.")
            query=query.where(or_(Job.requested_at>cursor.requested_at,and_(Job.requested_at==cursor.requested_at,Job.order_id>cursor.order_id)))
        rows=db.session.execute(query.order_by(Job.requested_at,Job.order_id).limit(limit+1)).scalars().all()
        page=rows[:limit]
        return jsonify({"deliveries":[store.job_data(j,store.order(j.order_id)) for j in page],
                        "next_after":page[-1].order_id if len(rows)>limit else None})

    @app.get("/delivery/events")
    def events():
        if set(request.args)-{"zone","after","limit"}:raise BadRequest("Unsupported delivery history query.")
        zone=identifier(request.args.get("zone"),"zone");store.zone(zone)
        after,limit=request.args.get("after","0"),request.args.get("limit","100")
        if not re.fullmatch(r"[0-9]{1,18}",after) or not re.fullmatch(r"[0-9]{1,3}",limit) or not 1<=int(limit)<=200:
            raise BadRequest("Invalid delivery history pagination.")
        rows=db.session.execute(select(Event).where(Event.zone==zone,Event.id>int(after)).order_by(Event.id).limit(int(limit)+1)).scalars().all()
        page=rows[:int(limit)]
        return jsonify({"events":[{"cursor":e.id,"event_id":e.event_id,"event_type":e.event_type,
            "order_id":e.order_id,"courier_id":e.courier_id,"assignment_id":e.assignment_id,
            "occurred_at":stamp(e.occurred_at),"data":e.data,"publication":{"published":e.published_at is not None,
            "attempts":e.attempts,"last_error_code":e.last_error_code}} for e in page],
            "next_after":page[-1].id if len(rows)>int(limit) else None})

    @app.post("/internal/tasks/<value>/process")
    def process(value):return jsonify(store.process(order_id(value),lease_request(request.get_json())))

    install_observability(app,"delivery")
    return app

if __name__=="__main__":
    create_app().run(host="0.0.0.0",port=8082,debug=False,use_reloader=False)
