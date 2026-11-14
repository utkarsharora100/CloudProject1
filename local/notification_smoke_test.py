"""Real local in-app messages, Payment/Kitchen/Delivery progress and independent failures."""
import json
import os
import uuid
import pymysql
from local.scheduler_smoke_test import call, order_fixture, managed_graph, wait_for
from local.payment_smoke_test import post as pay, wait_state as payment_wait
from local.kitchen_smoke_test import action, wait_state as kitchen_wait
from local.delivery_smoke_test import new_zone, courier, dispatch, wait_state as delivery_wait, handoff, presence
from order_service.outbox import create_producer

BASE="http://notification-api:8088"

def request(method,path,body=None,key=None):
    return call(method,path,body,key=key,base=BASE)

def sql(query,args=()):
    with pymysql.connect(host=os.environ["DB_PRIVATE_IP"],port=int(os.environ["DB_PORT"]),user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],database=os.environ["DB_NAME"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query,args);return cursor.fetchall()

def subscribe(order,scenario="success"):
    body={"customer_id":order["customer_id"],"scenario":scenario,"courier_updates":True}
    key=str(uuid.uuid4());path="/notifications/orders/"+order["order_id"]+"/subscription"
    first=request("POST",path,body,key);second=request("POST",path,body,key)
    assert first[0]==second[0]==201 and first[1]==second[1]
    return first[1]

def messages(order,*,courier_id=None):
    identity="courier_id="+courier_id if courier_id else "customer_id="+str(order["customer_id"])
    code,data=request("GET","/notifications?"+identity+"&order_id="+order["order_id"]+"&limit=50")
    assert code==200
    return data["notifications"]

def wait_messages(order,count,*,courier_id=None,state="SENT"):
    return wait_for(lambda: rows if len(rows:=messages(order,courier_id=courier_id))==count
        and all(row["state"]==state for row in rows) else None,seconds=300)

def delivered_order(scenario="success"):
    order=order_fixture();subscribe(order,scenario)
    pay(order);payment_wait(order["order_id"],"CAPTURED")
    ticket=kitchen_wait(order["order_id"],"WAITING")
    action(ticket,"accept");ticket=kitchen_wait(order["order_id"],"ACCEPTED")
    action(ticket,"start_preparation");ticket=kitchen_wait(order["order_id"],"PREPARING")
    action(ticket,"ready");ticket=kitchen_wait(order["order_id"],"READY")
    zone=new_zone()["zone"];actor=courier(zone,"Notification Demo Courier")
    try:
        dispatch(ticket,zone);delivery_wait(order["order_id"],"ASSIGNED")
        handoff(order["order_id"],"pickup");handoff(order["order_id"],"deliver")
    finally:
        presence(actor["courier_id"],False,False)
    return order,actor

def exercise_notifications():
    order,actor=delivered_order("response_loss_once")
    event_count=sql("SELECT COUNT(*) FROM order_events_v2 WHERE order_id=%s",(order["order_id"],))[0][0]
    rows=wait_messages(order,event_count)
    courier_rows=wait_messages(order,3,courier_id=actor["courier_id"])
    assert {row["stage"] for row in rows}=={"PENDING_PAYMENT","PAID","KITCHEN_ACCEPTED","PREPARING","READY",
        "COURIER_ASSIGNED","PICKED_UP","DELIVERED"}
    # Source history is the authority; compare exact events, not guessed counts.
    events=sql("SELECT event_id,to_state FROM order_events_v2 WHERE order_id=%s ORDER BY sequence",(order["order_id"],))
    assert {row["source"]["event_id"] for row in rows}=={row[0] for row in events}
    assert len(rows)==len(events) and all(row["source"]["topic"]=="orders" for row in rows)
    assert all(row["recipient_type"]=="courier" and row["source"]["topic"]=="delivery_assignments" for row in courier_rows)
    assert all(row["attempt_count"]==1 for row in rows+courier_rows)
    counts=sql("SELECT COUNT(*),COUNT(DISTINCT t.notification_id) FROM notification_transport_v1 t JOIN notifications_v1 n ON n.id=t.notification_id WHERE n.order_id=%s",(order["order_id"],))[0]
    assert counts==(len(rows)+len(courier_rows),)*2
    print("Real order/kitchen/delivery facts, separate recipient streams and lost-response recovery with one transport receipt: PASS",flush=True)
    def confirmed_graph():
        graph=managed_graph(order["order_id"])
        nodes={node["key"]:node for node in graph["nodes"]} if graph else {}
        chosen=[nodes.get(key,{}) for key in ("notify_paid","notify_ready","notify_delivered")]
        return chosen if all(node.get("state")=="SUCCEEDED" and node.get("completion_source")=="notification_receipt" for node in chosen) else None
    wait_for(confirmed_graph)

    selected=rows[0];path="/notifications/"+selected["notification_id"]
    code,_=request("GET",path+"?customer_id="+str(order["customer_id"]+1000000));assert code==404
    body={"recipient_type":"customer","recipient_id":order["customer_id"],"expected_version":selected["version"]}
    key=str(uuid.uuid4());first=request("POST",path+"/read",body,key);second=request("POST",path+"/read",body,key)
    assert first[0]==second[0]==200 and first[1]==second[1] and first[1]["read_at"] and not first[1]["unread"]
    code,history=request("GET",path+"/attempts?customer_id="+str(order["customer_id"]))
    assert code==200 and history["attempts"][0]["outcome"]=="RECEIPT_RECOVERED"
    assert "lease_token" not in json.dumps(history)
    print("Receipt-confirmed graph tasks, recipient scoping, versioned read state and private-proof-free history: PASS",flush=True)

    # Same source event on another Kafka offset cannot create another message.
    payload=sql("SELECT payload FROM order_outbox_v2 WHERE order_id=%s ORDER BY sequence LIMIT 1",(order["order_id"],))[0][0]
    producer=create_producer("notification-smoke")
    coords=[]
    def ack(error,message):
        assert error is None
        coords.append((message.partition(),message.offset()))
    producer.produce("orders",key=order["order_id"],partition=0,value=b'{"invalid":',on_delivery=ack)
    producer.flush(6);assert len(coords)==1
    producer.produce("orders",key=order["order_id"],partition=0,value=payload,on_delivery=ack)
    producer.flush(6);assert len(coords)==2
    def consumed():
        states=[sql("SELECT outcome FROM notification_stream_v1 WHERE topic='orders' AND `partition`=%s AND `offset`=%s",point) for point in coords]
        return states if all(states) else None
    states=wait_for(consumed)
    assert states[0][0][0]=="QUARANTINED" and states[1][0][0]=="MATERIALIZED"
    assert len(messages(order))==len(rows)
    print("Durable malformed-record quarantine, subsequent Kafka progress and source-event deduplication: PASS",flush=True)

    failed,failed_actor=delivered_order("always_fail")
    event_count=sql("SELECT COUNT(*) FROM order_events_v2 WHERE order_id=%s",(failed["order_id"],))[0][0]
    failures=wait_messages(failed,event_count,state="FAILED")
    assert all(row["round_attempts"]==3 for row in failures)
    assert delivery_wait(failed["order_id"],"DELIVERED")["state"]=="DELIVERED"
    target=next(row for row in failures if row["stage"]=="PAID")
    key=str(uuid.uuid4());body={"recipient_type":"customer","recipient_id":failed["customer_id"],
        "expected_version":target["version"],"scenario":"success"}
    path="/notifications/"+target["notification_id"]+"/retry"
    first=request("POST",path,body,key);second=request("POST",path,body,key)
    assert first[0]==second[0]==200 and first[1]==second[1]
    recovered=wait_for(lambda:row if (row:=request("GET","/notifications/"+target["notification_id"]+
        "?customer_id="+str(failed["customer_id"]))[1])["state"]=="SENT" else None)
    assert recovered["notification_id"]==target["notification_id"] and recovered["retry_round"]==2 and recovered["attempt_count"]==4
    print("Bounded failures leave food delivery independent; explicit retry preserves identity and attempt history: PASS",flush=True)
    return {"notification_order_id":order["order_id"],"notification_failure_order_id":failed["order_id"]}

def main():
    print(json.dumps({"result":"PASS",**exercise_notifications()}),flush=True)

if __name__=="__main__":
    main()
