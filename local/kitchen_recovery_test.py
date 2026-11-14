"""Opt-in kitchen worker crash and non-destructive Firestore pause recovery."""
import argparse
import hashlib
import json
from pathlib import Path
from local.environment import require_approved_recovery_root
import subprocess
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from local.payment_recovery_test import compose, wait_for

ROOT = Path(__file__).resolve().parents[1]
WORKERS = ["kitchen-stage-worker-a", "kitchen-stage-worker-b"]

def http(method, path, payload=None):
    req = Request("http://127.0.0.1:8087"+path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type":"application/json","Idempotency-Key":str(uuid.uuid4())})
    try:
        response = urlopen(req,timeout=10)
    except HTTPError as error:
        response = error
    with response:
        return response.status,json.load(response)

def state(value):
    code,data = http("GET","/kitchen/orders/"+value)
    if code != 200:
        raise RuntimeError("Recovery kitchen ticket is unavailable.")
    return data["ticket"]

def request_ready(value):
    current = state(value)
    code,_ = http("POST","/kitchen/orders/"+value+"/actions",
                 {"action":"ready","expected_version":current["order_version"]})
    if code != 202:
        raise RuntimeError("Recovery ready action was rejected.")

def fixture(preparing=False):
    from local.kitchen_smoke_test import paid_fixture, action, wait_state
    order,payment,ticket = paid_fixture()
    action(ticket,"accept")
    accepted = wait_state(order["order_id"],"ACCEPTED")
    if preparing:
        action(accepted,"start_preparation")
        accepted = wait_state(order["order_id"],"PREPARING")
    print(json.dumps({"order_id":order["order_id"],"order_version":accepted["order_version"]}),flush=True)

def actor(value, version):
    from task_scheduler.coordination import Coordination
    from task_scheduler.runtime import api_call
    from local.scheduler_smoke_test import call
    code,_ = call("POST","/kitchen/orders/"+value+"/actions",
        {"action":"start_preparation","expected_version":version}, key=str(uuid.uuid4()),base="http://kitchen-api:8087")
    if code != 202:
        raise RuntimeError("Recovery preparation action failed.")
    coordination = Coordination(strict_session=True).start()
    worker_id = str(uuid.uuid4())
    try:
        coordination.register_worker(worker_id,"kitchen-recovery-actor",["preparation"])
        def assigned():
            rows = api_call("http://task-api:8085","GET",f"/workers/{worker_id}/assignments")["assignments"]
            return next((r for r in rows if r["order_id"] == value),None)
        job = wait_for(assigned)
        task_id,generation = job["task_id"],job["generation"]
        proof = {"worker_id":worker_id,"generation":generation,"lease_token":str(uuid.uuid4())}
        coordination.create_lease(task_id,generation,worker_id,proof["lease_token"])
        api_call("http://task-api:8085","POST",f"/tasks/{task_id}/lease/start",proof)
        result = api_call("http://kitchen-api:8087","POST",f"/internal/tasks/{task_id}/process",proof)
        assert result["task_outcome"] == "deferred"
        print(json.dumps({"marker":"preparation_saved","order_id":value,"task_id":task_id,"generation":generation}),flush=True)
        while True:
            coordination.guard()
            time.sleep(0.25)
    finally:
        coordination.close()

def probe(value, task_id=None, generation=None, *, stale=False):
    from sqlalchemy import select
    from order_service.models import Order, OrderEvent, db
    from order_service.storage import create_storage_app
    from kitchen_workflow.models import Ticket, Command, Event
    from task_scheduler.models import Attempt, Task
    app = create_storage_app()
    with app.app_context():
        order = db.session.get(Order,value)
        ticket = db.session.get(Ticket,value)
        assert ticket.state == "READY" and order.status == "READY"
        history = db.session.execute(select(OrderEvent.to_state).where(OrderEvent.order_id == value)).scalars().all()
        assert history.count("PREPARING") == history.count("READY") == 1
        if stale:
            table = Attempt.__table__
            previous = db.session.execute(select(table).where(table.c.task_id == task_id,table.c.generation == generation)).mappings().one()
            proof = {"worker_id":previous["worker_id"],"generation":generation,"lease_token":previous["lease_token"]}
            for base,path in (("http://kitchen-api:8087","/internal/tasks/"+task_id+"/process"),
                              ("http://task-api:8085","/tasks/"+task_id+"/lease/renew")):
                req = Request(base+path,method="POST",data=json.dumps(proof).encode(),headers={"Content-Type":"application/json"})
                try:
                    with urlopen(req,timeout=10) as response:
                        code = response.status
                except HTTPError as error:
                    code = error.code
                assert code == 409,"An old kitchen owner update was accepted."
            current = db.session.execute(select(Task.__table__.c.generation).where(Task.__table__.c.id == task_id)).scalar_one()
            assert current > generation
            assert previous["outcome"] == "ABANDONED"
        rows = {}
        for model,column in ((Order,Order.id),(OrderEvent,OrderEvent.order_id),
                             (Ticket,Ticket.order_id),(Command,Command.order_id),(Event,Event.order_id)):
            table = model.__table__
            values = [dict(r) for r in db.session.execute(select(table).where(column == value).order_by(*table.primary_key.columns)).mappings()]
            if model is Ticket:
                values = [{k:v for k,v in row.items() if not k.startswith(("projection_","projected_"))} for row in values]
            rows[table.name] = values
        print(json.dumps({"result":"PASS","digest":hashlib.sha256(json.dumps(rows,default=str,sort_keys=True).encode()).hexdigest()}),flush=True)

def helper(mode, *, data=None, preparing=False):
    args = ["run","--rm","--no-deps","-T","kitchen-smoke-test","python","-m","local.kitchen_recovery_test","--"+mode]
    if preparing:
        args.append("--preparing")
    if data:
        args += ["--order-id",data["order_id"]]
        if "task_id" in data:
            args += ["--task-id",data["task_id"],"--generation",str(data["generation"]),"--stale"]
    output = compose(args)
    return json.loads(next(line for line in reversed(output.splitlines()) if line.startswith("{")))

def docker(args):
    result = subprocess.run(["docker",*args],capture_output=True,text=True,timeout=30)
    if result.returncode:
        raise RuntimeError("A scoped kitchen recovery container operation failed.")
    return result.stdout

def main():
    parser = argparse.ArgumentParser()
    for flag in ("allow-local-kitchen-failures","fixture","preparing","actor","probe","stale"):
        parser.add_argument("--"+flag,action="store_true")
    parser.add_argument("--order-id");parser.add_argument("--task-id")
    parser.add_argument("--generation",type=int);parser.add_argument("--expected-version",type=int)
    args = parser.parse_args()
    if args.fixture:
        return fixture(args.preparing)
    if args.actor:
        return actor(args.order_id,args.expected_version)
    if args.probe:
        return probe(args.order_id,args.task_id,args.generation,stale=args.stale)
    if not args.allow_local_kitchen_failures:
        raise SystemExit("Use --allow-local-kitchen-failures from the approved project.")
    require_approved_recovery_root(ROOT)
    if json.loads(compose(["config","--format","json"]))["name"] != "cloudproject1-local":
        raise RuntimeError("Unexpected Compose project.")
    actor_name = "cloudproject1-local-kitchen-recovery-"+uuid.uuid4().hex[:8]
    actor_created,paused = False,False
    try:
        setup = helper("fixture")
        compose(["stop",*WORKERS])
        compose(["run","-d","--no-deps","--name",actor_name,"kitchen-stage-worker-a","python","-m","local.kitchen_recovery_test",
                 "--actor","--order-id",setup["order_id"],"--expected-version",str(setup["order_version"])])
        actor_created = True
        def marker():
            for line in docker(["logs",actor_name]).splitlines():
                if line.startswith("{"):
                    record = json.loads(line)
                    if record.get("marker") == "preparation_saved":
                        return record
            return None
        data = wait_for(marker)
        docker(["kill","--signal","KILL",actor_name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90",*WORKERS])
        wait_for(lambda: state(data["order_id"])["state"] == "PREPARING")
        request_ready(data["order_id"])
        wait_for(lambda: state(data["order_id"])["state"] == "READY")
        before = helper("probe",data=data)
        print("Committed preparation survives worker SIGKILL; ready uses a new generation; stale writes rejected with no duplicate stages: PASS",flush=True)

        view_fixture = helper("fixture",preparing=True)
        view_id = view_fixture["order_id"]
        wait_for(lambda: state(view_id)["projection"]["current"])
        docker(["pause","cloudproject1-local-firestore-1"])
        paused = True
        request_ready(view_id)
        wait_for(lambda: state(view_id)["state"] == "READY")
        wait_for(lambda: state(view_id)["projection"]["last_error_code"] == "firestore_projection_unavailable")
        assert not state(view_id)["projection"]["current"]
        docker(["unpause","cloudproject1-local-firestore-1"])
        paused = False
        wait_for(lambda: state(view_id)["projection"]["current"])
        print("SQL kitchen/Order progress survives a Firestore pause; the versioned view catches up after unpause without resetting emulator data: PASS",flush=True)

        compose(["up","-d","--no-deps","--force-recreate","--wait","--wait-timeout","90","kitchen-api","kitchen-sync",*WORKERS])
        after = helper("probe",data=data)
        assert before["digest"] == after["digest"]
        print("Kitchen tickets, commands, event history and Order stages survive API/sync/worker recreation: PASS",flush=True)
        print(json.dumps({"result":"PASS","order_id":data["order_id"],"projection_order_id":view_id,"services_restored":True}),flush=True)
    finally:
        if paused:
            docker(["unpause","cloudproject1-local-firestore-1"])
        if actor_created:
            docker(["rm","-f",actor_name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90","kitchen-api","kitchen-sync",*WORKERS])

if __name__ == "__main__":
    main()
