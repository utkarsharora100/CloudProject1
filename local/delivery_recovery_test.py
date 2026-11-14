"""Opt-in delivery worker crash, retained physical capacity and Kafka pause recovery."""
import argparse
import hashlib
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
WORKERS=["delivery-worker-a","delivery-worker-b"]

def http(method,path,body=None):
    request=Request("http://127.0.0.1:8082"+path,method=method,data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type":"application/json","Idempotency-Key":str(uuid.uuid4())})
    try:response=urlopen(request,timeout=10)
    except HTTPError as error:response=error
    with response:return response.status,json.load(response)

def state(value):
    code,data=http("GET","/delivery/orders/"+value)
    if code!=200:raise RuntimeError("Delivery recovery record is unavailable.")
    return data["delivery"]

def presence(value,online):
    code,data=http("GET","/couriers/"+value);assert code==200
    code,data=http("POST","/couriers/"+value+"/presence",{"expected_version":data["courier"]["version"],
        "online":online,"accepting_work":online})
    assert code==200
    return data["courier"]

def handoff(value,action):
    data=state(value)
    code,_=http("POST","/delivery/orders/"+value+"/actions",{"action":action,"expected_version":data["order_version"],
        "courier_id":data["assignment"]["courier_id"],"assignment_id":data["assignment"]["assignment_id"]})
    assert code==202
    wait_for(lambda:state(value)["state"]==("PICKED_UP" if action=="pickup" else "DELIVERED"))

def fixture(zone=None):
    from local.delivery_smoke_test import new_zone,ready_fixture,courier,dispatch
    zone_data=new_zone("recovery") if zone is None else {"zone":zone}
    ready=ready_fixture()
    actor=courier(zone_data["zone"],"Recovery Courier A") if zone is None else None
    dispatch(ready,zone_data["zone"])
    print(json.dumps({"order_id":ready["order_id"],"zone":zone_data["zone"],"courier_id":actor["courier_id"] if actor else None}),flush=True)

def actor(value):
    from task_scheduler.coordination import Coordination
    from task_scheduler.runtime import api_call
    coordination=Coordination(strict_session=True).start();worker_id=str(uuid.uuid4())
    try:
        coordination.register_worker(worker_id,"delivery-recovery-actor",["courier_assignment"])
        def assigned():
            rows=api_call("http://task-api:8085","GET",f"/workers/{worker_id}/assignments")["assignments"]
            return next((r for r in rows if r["order_id"]==value),None)
        job=wait_for(assigned);task_id,generation=job["task_id"],job["generation"]
        proof={"worker_id":worker_id,"generation":generation,"lease_token":str(uuid.uuid4())}
        coordination.create_lease(task_id,generation,worker_id,proof["lease_token"])
        api_call("http://task-api:8085","POST",f"/tasks/{task_id}/lease/start",proof)
        result=api_call("http://delivery-service:8082","POST",f"/internal/tasks/{task_id}/process",proof)
        assert result["task_outcome"]=="succeeded"
        print(json.dumps({"marker":"assignment_saved","order_id":value,"task_id":task_id,"generation":generation}),flush=True)
        while True:coordination.guard();time.sleep(0.25)
    finally:coordination.close()

def probe(zone,value,task_id=None,generation=None,*,stale=False):
    from sqlalchemy import select
    from order_service.models import Order,OrderEvent,db
    from order_service.storage import create_storage_app
    from delivery_assignment_service.models import Zone,Courier,Job,Assignment,ActiveSlot,Command,Receipt,Event
    from task_scheduler.models import Attempt
    app=create_storage_app()
    with app.app_context():
        assignments=db.session.execute(select(Assignment).join(Job,Job.order_id==Assignment.order_id).where(Job.zone==zone)).scalars().all()
        assert len({a.order_id for a in assignments})==len(assignments)
        slots=db.session.execute(select(ActiveSlot).join(Job,Job.order_id==ActiveSlot.order_id).where(Job.zone==zone)).scalars().all()
        assert len({s.courier_id for s in slots})==len({s.order_id for s in slots})==len(slots)
        if stale:
            table=Attempt.__table__
            old=db.session.execute(select(table).where(table.c.task_id==task_id,table.c.generation==generation)).mappings().one()
            proof={"worker_id":old["worker_id"],"generation":generation,"lease_token":old["lease_token"]}
            for base,path in (("http://delivery-service:8082","/internal/tasks/"+task_id+"/process"),
                              ("http://task-api:8085","/tasks/"+task_id+"/lease/renew")):
                req=Request(base+path,method="POST",data=json.dumps(proof).encode(),headers={"Content-Type":"application/json"})
                try:
                    with urlopen(req,timeout=10) as response:code=response.status
                except HTTPError as error:code=error.code
                assert code==409,"An old delivery worker write was accepted."
        rows={}
        for model,column in ((Courier,Courier.zone),(Job,Job.zone),(Event,Event.zone)):
            table=model.__table__
            values=[dict(r) for r in db.session.execute(select(table).where(column==zone).order_by(*table.primary_key.columns)).mappings()]
            if model is Event:
                values=[{k:v for k,v in row.items() if k not in {"available_at","attempts","claimed_by","lease_until","published_at","last_error_code"}} for row in values]
            rows[table.name]=values
        ids=[j["order_id"] for j in rows[Job.__tablename__]]
        for model,column in ((Assignment,Assignment.order_id),(ActiveSlot,ActiveSlot.order_id),(Command,Command.order_id),
                             (Order,Order.id),(OrderEvent,OrderEvent.order_id)):
            table=model.__table__
            rows[table.name]=[dict(r) for r in db.session.execute(select(table).where(column.in_(ids)).order_by(*table.primary_key.columns)).mappings()]
        events=db.session.execute(select(Event).where(Event.zone==zone)).scalars().all()
        print(json.dumps({"result":"PASS","digest":hashlib.sha256(json.dumps(rows,default=str,sort_keys=True).encode()).hexdigest(),
            "pending":sum(e.published_at is None for e in events),"retried":sum(e.last_error_code=="delivery_unconfirmed" for e in events),
            "slot_count":len(slots),"assignment_count":len(assignments)}),flush=True)

def helper(mode,*,data=None,zone=None):
    args=["run","--rm","--no-deps","-T","delivery-smoke-test","python","-m","local.delivery_recovery_test","--"+mode]
    if zone:args+=["--zone",zone]
    if data:
        args+=["--zone",data["zone"],"--order-id",data["order_id"]]
        if "task_id" in data:
            args+=["--task-id",data["task_id"],"--generation",str(data["generation"]),"--stale"]
    output=compose(args)
    return json.loads(next(line for line in reversed(output.splitlines()) if line.startswith("{")))

def docker(args):
    result=subprocess.run(["docker",*args],capture_output=True,text=True,timeout=30)
    if result.returncode:raise RuntimeError("A scoped delivery recovery container operation failed.")
    return result.stdout

def main():
    p=argparse.ArgumentParser()
    for flag in ("allow-local-delivery-failures","fixture","actor","probe","stale"):p.add_argument("--"+flag,action="store_true")
    p.add_argument("--zone");p.add_argument("--order-id");p.add_argument("--task-id");p.add_argument("--generation",type=int)
    a=p.parse_args()
    if a.fixture:return fixture(a.zone)
    if a.actor:return actor(a.order_id)
    if a.probe:return probe(a.zone,a.order_id,a.task_id,a.generation,stale=a.stale)
    if not a.allow_local_delivery_failures:
        raise SystemExit("Use --allow-local-delivery-failures from the approved local project.")
    require_approved_recovery_root(ROOT)
    assert json.loads(compose(["config","--format","json"]))["name"]=="cloudproject1-local"
    name="cloudproject1-local-delivery-recovery-"+uuid.uuid4().hex[:8];created=paused=False
    try:
        compose(["stop",*WORKERS])
        setup=helper("fixture")
        compose(["run","-d","--no-deps","--name",name,"delivery-worker-a","python","-m","local.delivery_recovery_test",
                 "--actor","--order-id",setup["order_id"]]);created=True
        def marker():
            for line in docker(["logs",name]).splitlines():
                if line.startswith("{"):
                    record=json.loads(line)
                    if record.get("marker")=="assignment_saved":return record
            return None
        data={**setup,**wait_for(marker)}
        docker(["kill","--signal","KILL",name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90",*WORKERS])
        original=state(data["order_id"])["assignment"]["assignment_id"]
        presence(data["courier_id"],False)
        before=helper("probe",data=data)
        assert before["slot_count"]==before["assignment_count"]==1
        compose(["up","-d","--no-deps","--force-recreate","--wait","--wait-timeout","90","delivery-service",*WORKERS])
        after=helper("probe",data=data)
        assert before["digest"]==after["digest"]
        assert state(data["order_id"])["assignment"]["assignment_id"]==original
        print("Worker SIGKILL/API recreation preserves the exact physical assignment and occupied capacity; stale updates rejected: PASS",flush=True)

        other=helper("fixture",zone=data["zone"])
        assert state(other["order_id"])["state"]=="WAITING"
        presence(data["courier_id"],True);handoff(data["order_id"],"pickup")
        docker(["pause","cloudproject1-local-kafka-1"]);paused=True
        handoff(data["order_id"],"deliver")
        wait_for(lambda:state(other["order_id"])["state"]=="ASSIGNED")
        wait_for(lambda:helper("probe",data=data)["retried"]>=1,seconds=90)
        docker(["unpause","cloudproject1-local-kafka-1"]);paused=False
        presence(data["courier_id"],True)
        handoff(other["order_id"],"pickup");handoff(other["order_id"],"deliver")
        presence(data["courier_id"],False)
        wait_for(lambda:helper("probe",data=data)["pending"]==0,seconds=120)
        print("Broker pause does not block confirmed delivery/capacity release; durable delivery events retry after unpause: PASS",flush=True)
        final=helper("probe",data=data)
        assert final["slot_count"]==0 and final["assignment_count"]==2
        compose(["up","-d","--no-deps","--force-recreate","--wait","--wait-timeout","90","delivery-service","delivery-outbox",*WORKERS])
        assert helper("probe",data=data)["digest"]==final["digest"]
        print("Courier, assignment, handoff and Order histories persist across API/worker/relay recreation: PASS",flush=True)
        print(json.dumps({"result":"PASS","zone":data["zone"],"order_id":data["order_id"],"services_restored":True}),flush=True)
    finally:
        if paused:docker(["unpause","cloudproject1-local-kafka-1"])
        if created:docker(["rm","-f",name])
        compose(["up","-d","--no-deps","--wait","--wait-timeout","90","delivery-service","delivery-outbox",*WORKERS])

if __name__=="__main__":
    main()
