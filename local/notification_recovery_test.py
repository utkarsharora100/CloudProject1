"""Opt-in local SIGKILL, stale-proof, recreation and broker-outage verification."""
import argparse
import json
from pathlib import Path
from local.environment import require_approved_recovery_root
import subprocess
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from local.payment_recovery_test import compose,wait_for

ROOT=Path(__file__).resolve().parents[1]
WORKERS=["notification-worker-a","notification-worker-b"]

def http(path):
    with urlopen("http://127.0.0.1:8088"+path,timeout=10) as response:return json.load(response)

def docker(args):
    result=subprocess.run(["docker",*args],capture_output=True,text=True,timeout=30)
    if result.returncode:raise RuntimeError("A scoped notification recovery operation failed.")
    return result.stdout

def fixture(scenario):
    from local.scheduler_smoke_test import order_fixture
    from local.notification_smoke_test import subscribe
    order=order_fixture();subscribe(order,scenario)
    print(json.dumps({"order_id":order["order_id"],"customer_id":order["customer_id"]}),flush=True)

def actor(value):
    from local.scheduler_smoke_test import call
    from task_scheduler.coordination import Coordination
    from task_scheduler.runtime import api_call,WorkerHTTPError
    coordination=Coordination(strict_session=True).start();worker_id=str(uuid.uuid4())
    try:
        coordination.register_worker(worker_id,"notification-recovery-actor",["notification"])
        def assignment():
            rows=api_call("http://task-api:8085","GET",f"/workers/{worker_id}/assignments")["assignments"]
            return next((row for row in rows if row["order_id"]==value),None)
        job=wait_for(assignment);task_id,generation=job["task_id"],job["generation"]
        proof={"worker_id":worker_id,"generation":generation,"lease_token":str(uuid.uuid4())}
        coordination.create_lease(task_id,generation,worker_id,proof["lease_token"])
        api_call("http://task-api:8085","POST",f"/tasks/{task_id}/lease/start",proof)
        try:
            api_call("http://notification-api:8088","POST",f"/internal/tasks/{task_id}/process",proof)
        except WorkerHTTPError as error:
            assert error.code==503
        else:raise AssertionError("The controlled response-loss scenario did not occur.")
        print(json.dumps({"marker":"transport_saved","order_id":value,"task_id":task_id,"generation":generation}),flush=True)
        while True:coordination.guard();time.sleep(0.25)
    finally:coordination.close()

def probe(value,task_id=None,generation=None,*,stale=False):
    import hashlib
    from sqlalchemy import select
    from order_service.models import db,Outbox
    from order_service.storage import create_storage_app
    from notification_service.models import Subscription,Message,Attempt,TransportReceipt,StreamReceipt
    from task_scheduler.models import Attempt as LeaseAttempt
    app=create_storage_app()
    with app.app_context():
        message=db.session.execute(select(Message).where(Message.order_id==value)).scalar_one()
        receipts=db.session.execute(select(TransportReceipt).where(TransportReceipt.notification_id==message.id)).scalars().all()
        assert len(receipts)==1
        if stale:
            table=LeaseAttempt.__table__
            old=db.session.execute(select(table).where(table.c.task_id==task_id,table.c.generation==generation)).mappings().one()
            proof={"worker_id":old["worker_id"],"generation":generation,"lease_token":old["lease_token"]}
            for base,path in (("http://notification-api:8088","/internal/tasks/"+task_id+"/process"),
                              ("http://task-api:8085","/tasks/"+task_id+"/lease/renew")):
                request=Request(base+path,method="POST",data=json.dumps(proof).encode(),headers={"Content-Type":"application/json"})
                try:
                    with urlopen(request,timeout=10) as response:code=response.status
                except HTTPError as error:code=error.code
                assert code==409,"An obsolete notification lease was accepted."
        rows={}
        for model,column,key in ((Subscription,Subscription.order_id,value),(Message,Message.id,message.id),
            (Attempt,Attempt.notification_id,message.id),(TransportReceipt,TransportReceipt.notification_id,message.id)):
            table=model.__table__
            rows[table.name]=[dict(row) for row in db.session.execute(select(table).where(column==key).order_by(*table.primary_key.columns)).mappings()]
        outbox=db.session.execute(select(Outbox).where(Outbox.order_id==value).order_by(Outbox.sequence)).scalars().first()
        fingerprint=hashlib.sha256(json.dumps(outbox.payload,sort_keys=True,separators=(",",":")).encode()).hexdigest()
        seen=db.session.execute(select(StreamReceipt).where(StreamReceipt.topic=="orders",
            StreamReceipt.payload_digest==fingerprint,StreamReceipt.outcome=="MATERIALIZED")).scalars().first()
        print(json.dumps({"result":"PASS","state":message.state,"attempt_count":message.attempts,
            "receipt_count":len(receipts),"consumed":seen is not None,
            "digest":hashlib.sha256(json.dumps(rows,sort_keys=True,default=str).encode()).hexdigest()}),flush=True)

def helper(mode,data=None,scenario=None):
    args=["run","--rm","--no-deps","-T","notification-smoke-test","python","-m","local.notification_recovery_test","--"+mode]
    if scenario:args+=["--scenario",scenario]
    if data:
        args+=["--order-id",data["order_id"]]
        if data.get("task_id"):args+=["--task-id",data["task_id"],"--generation",str(data["generation"]),"--stale"]
    output=compose(args)
    return json.loads(next(line for line in reversed(output.splitlines()) if line.startswith("{")))

def main():
    parser=argparse.ArgumentParser()
    for flag in ("allow-local-notification-failures","fixture","actor","probe","stale"):parser.add_argument("--"+flag,action="store_true")
    parser.add_argument("--order-id");parser.add_argument("--task-id");parser.add_argument("--generation",type=int)
    parser.add_argument("--scenario",default="success")
    args=parser.parse_args()
    if args.fixture:return fixture(args.scenario)
    if args.actor:return actor(args.order_id)
    if args.probe:return probe(args.order_id,args.task_id,args.generation,stale=args.stale)
    if not args.allow_local_notification_failures:
        raise SystemExit("Use --allow-local-notification-failures from the approved local project.")
    require_approved_recovery_root(ROOT)
    assert json.loads(compose(["config","--format","json"]))["name"]=="cloudproject1-local"
    name="cloudproject1-local-notification-recovery-"+uuid.uuid4().hex[:8];created=paused=False
    try:
        compose(["stop",*WORKERS])
        setup=helper("fixture",scenario="response_loss_once")
        compose(["run","-d","--no-deps","--name",name,"notification-worker-a","python","-m",
            "local.notification_recovery_test","--actor","--order-id",setup["order_id"]]);created=True
        def marker():
            for line in docker(["logs",name]).splitlines():
                if line.startswith("{"):
                    record=json.loads(line)
                    if record.get("marker")=="transport_saved":return record
            return None
        data={**setup,**wait_for(marker)}
        docker(["kill","--signal","KILL",name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90",*WORKERS])
        wait_for(lambda:rows if (rows:=http("/notifications?customer_id="+str(data["customer_id"])))["notifications"]
            and rows["notifications"][0]["state"]=="SENT" else None)
        before=helper("probe",data)
        assert before["state"]=="SENT" and before["attempt_count"]==before["receipt_count"]==1
        compose(["up","-d","--no-deps","--force-recreate","--wait","--wait-timeout","90",
            "notification-api","notification-consumer","notification-reconcile",*WORKERS])
        after=helper("probe",data);assert before["digest"]==after["digest"]
        print("Actual worker SIGKILL after transport commit, stale-proof rejection and exact notification persistence after recreation: PASS",flush=True)
        docker(["pause","cloudproject1-local-kafka-1"]);paused=True
        outage=helper("fixture",scenario="success")
        wait_for(lambda:rows if (rows:=http("/notifications?customer_id="+str(outage["customer_id"])))["notifications"]
            and rows["notifications"][0]["state"]=="SENT" else None)
        assert helper("probe",outage)["receipt_count"]==1
        docker(["unpause","cloudproject1-local-kafka-1"]);paused=False
        wait_for(lambda:helper("probe",outage)["consumed"],seconds=120)
        assert helper("probe",outage)["receipt_count"]==1
        print("SQL notification reconciliation continues during broker pause; Kafka catches up without redelivery: PASS",flush=True)
        print(json.dumps({"result":"PASS","order_id":data["order_id"],"services_restored":True}),flush=True)
    finally:
        if paused:docker(["unpause","cloudproject1-local-kafka-1"])
        if created:docker(["rm","-f",name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90",
            "notification-api","notification-consumer","notification-reconcile",*WORKERS])

if __name__=="__main__":
    main()
