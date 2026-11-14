"""Opt-in local worker crash after a simulated capture; private proofs stay in memory."""
import argparse
import json
from pathlib import Path
from local.environment import require_approved_recovery_root
import subprocess
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT=Path(__file__).resolve().parents[1]
WORKERS=["payment-worker-a","payment-worker-b"]

def compose(args):
    result=subprocess.run(["docker","compose","--env-file",".env.local.example",*args],cwd=ROOT,
                          capture_output=True,text=True,timeout=180)
    if result.returncode:
        raise RuntimeError("A scoped payment recovery operation failed; inspect local service health.")
    return result.stdout

def wait_for(predicate,seconds=90):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        value=predicate()
        if value:return value
        time.sleep(0.25)
    raise RuntimeError("Timed out waiting for confirmed local payment recovery.")

def public_state(order_id):
    with urlopen("http://127.0.0.1:8086/payments/orders/"+order_id,timeout=10) as r:
        return json.load(r)["payment"]

def actor():
    from local.scheduler_smoke_test import call, order_fixture
    from task_scheduler.coordination import Coordination
    from task_scheduler.runtime import api_call, WorkerHTTPError
    order=order_fixture()
    order_id=order["order_id"]
    code,_=call("POST","/payments/orders/"+order_id,{"expected_version":1,"scenario":"timeout_once"},
                key=str(uuid.uuid4()),base="http://payment-service:8086")
    if code!=202:raise RuntimeError("Recovery fixture payment request failed.")
    coordination=Coordination(strict_session=True).start()
    worker_id=str(uuid.uuid4())
    try:
        coordination.register_worker(worker_id,"payment-recovery-actor",["payment"])
        def assignment():
            rows=api_call("http://task-api:8085","GET",f"/workers/{worker_id}/assignments")["assignments"]
            return next((r for r in rows if r["order_id"]==order_id),None)
        job=wait_for(assignment)
        task_id,generation=job["task_id"],job["generation"]
        proof={"worker_id":worker_id,"generation":generation,"lease_token":str(uuid.uuid4())}
        coordination.create_lease(task_id,generation,worker_id,proof["lease_token"])
        api_call("http://task-api:8085","POST",f"/tasks/{task_id}/lease/start",proof)
        try:
            api_call("http://payment-service:8086","POST",f"/internal/tasks/{task_id}/process",proof)
        except WorkerHTTPError as error:
            if error.code!=503:raise
        else:
            raise RuntimeError("Expected the controlled simulated provider response interruption.")
        print(json.dumps({"marker":"capture_saved","order_id":order_id,"task_id":task_id,"generation":generation}),flush=True)
        # The host deliberately kills this worker before completion/lease cleanup.
        while True:
            coordination.guard()
            time.sleep(0.25)
    finally:
        coordination.close()

def probe(order_id,task_id,generation,*,pending=False,stale=False):
    import hashlib
    from sqlalchemy import select
    from order_service.models import Order, OrderEvent, db
    from order_service.storage import create_storage_app
    from payment_service.models import Payment, PaymentAttempt, PaymentEvent, PaymentRequest, ProviderOperation
    from task_scheduler.models import Attempt, Task
    app=create_storage_app()
    with app.app_context():
        payment=db.session.execute(select(Payment).where(Payment.order_id==order_id)).scalar_one()
        ops=db.session.execute(select(ProviderOperation).where(ProviderOperation.payment_id==payment.id)).scalars().all()
        assert len(ops)==1 and ops[0].kind=="CAPTURE" and ops[0].outcome=="SUCCEEDED"
        order=db.session.get(Order,order_id)
        assert order.status==("PENDING_PAYMENT" if pending else "PAID")
        if stale:
            table=Attempt.__table__
            old=db.session.execute(select(table).where(table.c.task_id==task_id,table.c.generation==generation)).mappings().one()
            proof={"worker_id":old["worker_id"],"generation":generation,"lease_token":old["lease_token"]}
            for base,path in (("http://payment-service:8086","/internal/tasks/"+task_id+"/process"),
                              ("http://task-api:8085","/tasks/"+task_id+"/lease/renew")):
                req=Request(base+path,method="POST",data=json.dumps(proof).encode(),headers={"Content-Type":"application/json"})
                try:
                    with urlopen(req,timeout=10) as response:code=response.status
                except HTTPError as error:code=error.code
                assert code==409,"A stale worker update was accepted."
            current=db.session.execute(select(Task.__table__.c.generation).where(Task.__table__.c.id==task_id)).scalar_one()
            assert current>generation
        rows={}
        for model,column,value in ((Order,Order.id,order_id),(OrderEvent,OrderEvent.order_id,order_id),
                                   (Payment,Payment.id,payment.id),(PaymentAttempt,PaymentAttempt.payment_id,payment.id),
                                   (PaymentEvent,PaymentEvent.payment_id,payment.id),(ProviderOperation,ProviderOperation.payment_id,payment.id)):
            table=model.__table__
            rows[table.name]=[dict(r) for r in db.session.execute(select(table).where(column==value).order_by(*table.primary_key.columns)).mappings()]
        print(json.dumps({"result":"PASS","digest":hashlib.sha256(json.dumps(rows,default=str,sort_keys=True).encode()).hexdigest()}),flush=True)

def helper(data,*,pending=False,stale=False):
    args=["run","--rm","--no-deps","-T","payment-smoke-test","python","-m","local.payment_recovery_test","--probe",
          "--order-id",data["order_id"],"--task-id",data["task_id"],"--generation",str(data["generation"])]
    if pending:args.append("--pending")
    if stale:args.append("--stale")
    output=compose(args)
    return json.loads(next(line for line in reversed(output.splitlines()) if line.startswith("{")))

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--allow-local-payment-failures",action="store_true")
    parser.add_argument("--actor",action="store_true")
    parser.add_argument("--probe",action="store_true")
    parser.add_argument("--pending",action="store_true")
    parser.add_argument("--stale",action="store_true")
    parser.add_argument("--order-id")
    parser.add_argument("--task-id")
    parser.add_argument("--generation",type=int)
    args=parser.parse_args()
    if args.actor:return actor()
    if args.probe:return probe(args.order_id,args.task_id,args.generation,pending=args.pending,stale=args.stale)
    if not args.allow_local_payment_failures:
        raise SystemExit("Use --allow-local-payment-failures from the approved local project.")
    require_approved_recovery_root(ROOT)
    assert compose(["config","--format","json"]).lstrip().startswith("{")
    # config contains local dummy settings: capture and parse it, never print it.
    configuration=json.loads(compose(["config","--format","json"]))
    if configuration["name"]!="cloudproject1-local":raise RuntimeError("Unexpected Compose project.")
    actor_name="cloudproject1-local-payment-recovery-"+uuid.uuid4().hex[:8]
    actor_created=False
    try:
        compose(["stop",*WORKERS])
        compose(["run","-d","--no-deps","--name",actor_name,"payment-worker-a","python","-m","local.payment_recovery_test","--actor"])
        actor_created=True
        def marker():
            result=subprocess.run(["docker","logs",actor_name],capture_output=True,text=True,timeout=15)
            for line in result.stdout.splitlines():
                if line.startswith("{"):
                    data=json.loads(line)
                    if data.get("marker")=="capture_saved":return data
            return None
        data=wait_for(marker)
        helper(data,pending=True)
        result=subprocess.run(["docker","kill","--signal","KILL",actor_name],capture_output=True,text=True,timeout=15)
        assert result.returncode==0
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90",*WORKERS])
        wait_for(lambda: public_state(data["order_id"])["state"]=="CAPTURED")
        before=helper(data,stale=True)
        print("Committed simulated capture survives worker SIGKILL; replacement reuses its receipt; stale destination writes are rejected: PASS",flush=True)
        compose(["up","-d","--no-deps","--force-recreate","--wait","--wait-timeout","90","payment-service",*WORKERS])
        after=helper(data,stale=True)
        assert before["digest"]==after["digest"]
        print("Payment ledger, receipt, Order history and payment events survive API/worker recreation: PASS",flush=True)
        print(json.dumps({"result":"PASS","order_id":data["order_id"],"services_restored":True}),flush=True)
    finally:
        if actor_created:
            subprocess.run(["docker","rm","-f",actor_name],capture_output=True,text=True,timeout=20)
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90","payment-service",*WORKERS])

if __name__=="__main__":
    main()
