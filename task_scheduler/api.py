from common.observability import install as install_observability
"""Local Scheduler HTTP API: graph data and private worker lease operations."""
import atexit
import re
from flask import jsonify, request
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest, HTTPException
from task_scheduler.coordination import Coordination
from task_scheduler.domain import graph_request, lease_request, request_key, uuid_id
from task_scheduler.models import LeaderState, TaskEvent, Workflow, db
from task_scheduler.storage import create_storage_app
from task_scheduler.store import TaskStore, public_task, stamp


def create_app(config=None, *, coordination=None, clock=None):
    app = create_storage_app(config)
    if coordination is None:
        coordination = Coordination().start()
        atexit.register(coordination.close)
    store = TaskStore(coordination, clock=clock)
    app.extensions["task_store"] = store

    @app.after_request
    def private_response(response):
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
        app.logger.error("Scheduler database operation failed.")
        return jsonify({"error": "Task data is temporarily unavailable."}), 503

    @app.errorhandler(Exception)
    def unexpected(error):
        db.session.rollback()
        app.logger.error("Scheduler operation failed.")
        return jsonify({"error": "Task operation could not be completed."}), 500

    def status():
        row = db.session.get(LeaderState, 1)
        live = coordination.leader()
        now = store.clock()
        available = bool(row and live and row.epoch == live.get("epoch") and row.leader_id == live.get("leader_id")
                         and row.expires_at and row.expires_at > now)
        return {"leader_available": available, "zookeeper_connected": coordination.live,
                "leader": {"instance_id": row.leader_id, "name": row.leader_name, "epoch": row.epoch,
                           "heartbeat_at": stamp(row.heartbeat_at)} if available else None}

    @app.get("/health")
    def health():
        db.session.execute(select(Workflow.id).limit(1)).first()
        return jsonify({"service": "task-api", "status": "healthy", **status()})

    @app.get("/scheduler")
    def scheduler_status():
        return jsonify(status())

    @app.get("/workers")
    def workers():
        return jsonify({"workers": [{key: worker[key] for key in ("worker_id", "name", "capabilities")}
                                    for worker in coordination.workers()]})

    @app.post("/workflows")
    def create_workflow():
        definition = graph_request(request.get_json())
        value, replayed = store.create_graph(definition, request_key(request.headers.get("Idempotency-Key")))
        response = jsonify(value)
        response.status_code = 201
        response.headers["Location"] = f"/workflows/{value['workflow_id']}/graph"
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        return response

    @app.get("/workflows")
    def workflows():
        if set(request.args) != {"order_id"}:
            raise BadRequest("An order_id is required to list its workflows.")
        order_id = uuid_id(request.args["order_id"])
        rows = db.session.execute(select(Workflow).where(Workflow.order_id == order_id).order_by(Workflow.created_at)).scalars().all()
        return jsonify({"workflows": [{"workflow_id": row.id, "order_id": row.order_id,
                                      "managed": row.managed_order_id is not None,
                                      "created_at": stamp(row.created_at)} for row in rows]})

    @app.get("/workflows/<value>/graph")
    def graph(value):
        return jsonify(store.graph(uuid_id(value)))

    @app.get("/tasks/<value>")
    def task(value):
        return jsonify({"task": public_task(store.task(uuid_id(value)))})

    @app.get("/tasks/<value>/events")
    def history(value):
        task = store.task(uuid_id(value))
        if set(request.args) - {"after", "limit"}:
            raise BadRequest("Unsupported history query parameters.")
        after, limit = request.args.get("after", "0"), request.args.get("limit", "100")
        if not re.fullmatch(r"[0-9]{1,18}", after) or not re.fullmatch(r"[0-9]{1,3}", limit) or not 1 <= int(limit) <= 200:
            raise BadRequest("History pagination is invalid.")
        rows = db.session.execute(select(TaskEvent).where(TaskEvent.task_id == task.id, TaskEvent.id > int(after))
                                  .order_by(TaskEvent.id).limit(int(limit) + 1)).scalars().all()
        page = rows[:int(limit)]
        return jsonify({"events": [{"event_id": row.id, "from_state": row.from_state, "to_state": row.to_state,
            "reason_code": row.reason, "generation": row.generation, "occurred_at": stamp(row.occurred_at)} for row in page],
            "next_after": page[-1].id if len(rows) > int(limit) else None})

    @app.get("/workers/<value>/assignments")
    def assignments(value):
        return jsonify({"assignments": store.assignments(uuid_id(value))})

    @app.post("/tasks/<value>/lease/start")
    def start(value):
        return jsonify(store.start(uuid_id(value), lease_request(request.get_json())))

    @app.post("/tasks/<value>/lease/renew")
    def renew(value):
        return jsonify(store.renew(uuid_id(value), lease_request(request.get_json())))

    @app.post("/tasks/<value>/complete")
    def complete(value):
        result, replayed = store.complete(uuid_id(value), lease_request(request.get_json(), completion=True))
        response = jsonify(result)
        response.headers["Idempotency-Replayed"] = "true" if replayed else "false"
        return response

    install_observability(app,"scheduler")
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8085, debug=False, use_reloader=False)
