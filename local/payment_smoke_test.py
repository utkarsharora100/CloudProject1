"""Real local Payment API, worker, MySQL and Order/outbox integration checks."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
import uuid
import pymysql
from google.cloud import firestore
from local.environment import require_local_emulator
from local.scheduler_smoke_test import call, managed_graph, order_fixture, wait_for

BASE = "http://payment-service:8086"

def request(method, path, body=None, key=None):
    return call(method, path, body, key=key, base=BASE)

def read(order_id):
    code, data = request("GET", "/payments/orders/"+order_id)
    assert code == 200
    return data["payment"]

def wait_state(order_id, state):
    return wait_for(lambda: (value if value["state"] == state else None) if (value := read(order_id)) else None)

def ops(payment_id):
    with pymysql.connect(host=os.environ["DB_PRIVATE_IP"], port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"]) as c:
        with c.cursor() as q:
            q.execute("SELECT kind, outcome, amount_minor, currency FROM payment_provider_operations_v1 WHERE payment_id=%s ORDER BY kind", (payment_id,))
            return q.fetchall()

def post(order, suffix="", *, scenario="success", refund="success", concurrent=False):
    path="/payments/orders/"+order["order_id"]+suffix
    body={"expected_version":order.get("order_version",order.get("version"))}
    if not suffix:
        body.update(scenario=scenario,refund_scenario=refund)
    key=str(uuid.uuid4())
    if concurrent:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:request("POST",path,body,key),(1,2)))
        assert all(code==202 for code,_ in results), "Concurrent payment request did not replay."
        assert results[0][1]==results[1][1]
        return results[0][1]
    code,data=request("POST",path,body,key)
    assert code==202,"Payment action was rejected."
    return data

def exercise_payments():
    project=require_local_emulator()
    first=order_fixture()
    order_id=first["order_id"]
    def gated():
        graph=managed_graph(order_id)
        if not graph:return None
        pay=next(n for n in graph["nodes"] if n["key"]=="payment")
        return graph if pay["state"]=="READY" and pay["reason_code"]=="payment_request_required" else None
    wait_for(gated)
    assert request("GET","/payments/orders/"+order_id)[0]==404
    code,_=request("POST","/payments/orders/"+order_id,{"expected_version":1,"scenario":"success","amount_minor":1},str(uuid.uuid4()))
    assert code==400
    created=post(first,concurrent=True)
    captured=wait_state(order_id,"CAPTURED")
    assert captured["amount_minor"]==first["total_minor"] and captured["order_status"]=="PAID"
    assert ops(created["payment_id"])==(("CAPTURE","SUCCEEDED",first["total_minor"],"INR"),)
    wait_for(lambda: next((n for n in (managed_graph(order_id) or {}).get("nodes",[]) if n["key"]=="payment" and n["state"]=="SUCCEEDED" and n["attempt_count"]>=1),None))
    code,history=call("GET","/orders/"+order_id+"/events",base="http://order-service:8081")
    assert code==200 and [e["to_state"] for e in history["events"]]==["PENDING_PAYMENT","PAID"]
    code,_=call("POST","/orders/"+order_id+"/transitions",{"expected_version":captured["order_version"],"to_state":"PAYMENT_FAILED"},key=str(uuid.uuid4()),base="http://order-service:8081")
    assert code==409
    database=firestore.Client(project=project)
    ticket=database.collection("restaurants").document(first["restaurant_id"]).collection("kitchen_tickets").document(order_id)
    wait_for(lambda: ticket.get(retry=None,timeout=3).exists)
    assert ticket.get(retry=None,timeout=3).to_dict()["status"]=="Awaiting_Kitchen_Acceptance"
    print("Explicit payment opt-in, server totals, concurrent replay, real worker capture and payment-gated kitchen event: PASS",flush=True)

    declined_order=order_fixture()
    declined_id=declined_order["order_id"]
    post(declined_order,scenario="decline_then_success")
    declined=wait_state(declined_id,"DECLINED")
    assert declined["order_status"]=="PAYMENT_FAILED"
    time.sleep(1)
    assert read(declined_id)["attempt_number"]==1
    graph=managed_graph(declined_id)
    assert next(n for n in graph["nodes"] if n["key"]=="kitchen_accept")["state"]=="BLOCKED"
    post(declined,"/retry",concurrent=True)
    recovered=wait_state(declined_id,"CAPTURED")
    assert [a["state"] for a in recovered["attempts"]]==["DECLINED","SUCCEEDED"]
    assert sum(row[1]=="SUCCEEDED" for row in ops(recovered["payment_id"]))==1
    print("Confirmed decline blocks kitchen; explicit concurrent retry produces one successful charge: PASS",flush=True)

    uncertain_order=order_fixture()
    uncertain_id=uncertain_order["order_id"]
    post(uncertain_order,scenario="timeout_once",refund="timeout_once")
    uncertain=wait_state(uncertain_id,"CAPTURED")
    assert len(ops(uncertain["payment_id"]))==1 and len(uncertain["attempts"])==1
    post(uncertain,"/cancel",concurrent=True)
    refunded=wait_state(uncertain_id,"REFUNDED")
    receipts=ops(refunded["payment_id"])
    assert sorted(row[0] for row in receipts)==["CAPTURE","REFUND"]
    assert all(row[1]=="SUCCEEDED" and row[2]==uncertain_order["total_minor"] for row in receipts)
    assert refunded["order_status"]=="REFUNDED" and refunded["refund"]["state"]=="SUCCEEDED"
    code,events=request("GET","/payments/orders/"+uncertain_id+"/events")
    assert code==200
    assert [e["event_type"] for e in events["events"]].count("PaymentCaptured")==1
    assert [e["event_type"] for e in events["events"]].count("PaymentRefunded")==1
    serialized=json.dumps(refunded)+json.dumps(events)
    assert all(word not in serialized for word in ("lease_token","owner_session","DB_PASSWORD","delivery_snapshot"))
    print("Lost simulated capture/refund responses recover stable receipts without duplicate monetary effects: PASS",flush=True)

    unpaid=order_fixture()
    cancelled=post(unpaid,"/cancel")
    assert cancelled["state"]=="CANCELLED" and ops(cancelled["payment_id"])==()
    assert read(unpaid["order_id"])["attempts"]==[]
    permanent=order_fixture()
    post(permanent,scenario="decline")
    failed=wait_state(permanent["order_id"],"DECLINED")
    post(failed,"/cancel")
    assert read(permanent["order_id"])["state"]=="CANCELLED"
    assert all(r[1]=="DECLINED" for r in ops(failed["payment_id"]))
    print("Unpaid and declined cancellation persist without a capture or fictitious refund: PASS",flush=True)

    raced=order_fixture()
    post(raced)
    code,cancelled=request("POST","/payments/orders/"+raced["order_id"]+"/cancel",{"expected_version":1},str(uuid.uuid4()))
    if code==409:
        current=wait_state(raced["order_id"],"CAPTURED")
        post(current,"/cancel")
    else:
        assert code==202
    final=wait_for(lambda: p if (p:=read(raced["order_id"]))["state"] in {"CANCELLED","REFUNDED"} else None)
    records=ops(final["payment_id"])
    charges=sum(r[0]=="CAPTURE" and r[1]=="SUCCEEDED" for r in records)
    refunds=sum(r[0]=="REFUND" and r[1]=="SUCCEEDED" for r in records)
    assert charges==refunds and charges<=1
    print("Capture/cancellation race converges to no charge or exactly one capture and matching refund: PASS",flush=True)
    return {"payment_order_id":order_id,"refunded_order_id":uncertain_id,"payment_id":created["payment_id"]}

def main():
    print(json.dumps({"result":"PASS",**exercise_payments()}),flush=True)

if __name__=="__main__":
    main()
