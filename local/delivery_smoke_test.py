"""Real paid/kitchen-ready orders, physical courier simulation and measured delivery flow."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
import uuid
import pymysql
from confluent_kafka import Consumer
from common.kafka import kafka_config
from local.scheduler_smoke_test import call,managed_graph,wait_for
from local.kitchen_smoke_test import paid_fixture,action as kitchen_action,wait_state as kitchen_wait

BASE="http://delivery-service:8082"

def request(method,path,body=None,key=None):
    return call(method,path,body,key=key,base=BASE)

def new_zone(scenario="manual"):
    zone="demo-"+uuid.uuid4().hex
    code,data=request("POST","/delivery/zones",{"zone":zone,"scenario":scenario},str(uuid.uuid4()))
    assert code==201
    return data

def ready_fixture():
    order,payment,ticket=paid_fixture()
    kitchen_action(ticket,"accept")
    accepted=kitchen_wait(order["order_id"],"ACCEPTED")
    kitchen_action(accepted,"start_preparation")
    preparing=kitchen_wait(order["order_id"],"PREPARING")
    kitchen_action(preparing,"ready")
    return kitchen_wait(order["order_id"],"READY")

def courier(zone,name="Courier A",online=True):
    code,data=request("POST","/couriers",{"name":name,"zone":zone},str(uuid.uuid4()))
    assert code==201
    result=data["courier"]
    return presence(result["courier_id"],True,True) if online else result

def read_courier(value):
    code,data=request("GET","/couriers/"+value);assert code==200
    return data["courier"]

def presence(value,online=True,accepting=True):
    current=read_courier(value)
    code,data=request("POST","/couriers/"+value+"/presence",{"expected_version":current["version"],
        "online":online,"accepting_work":accepting},str(uuid.uuid4()))
    assert code==200
    return data["courier"]

def dispatch(ticket,zone,concurrent=False):
    path="/delivery/orders/"+ticket["order_id"];body={"expected_version":ticket["order_version"],"zone":zone};key=str(uuid.uuid4())
    if concurrent:
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:request("POST",path,body,key),(1,2)))
        assert all(code==202 for code,_ in results) and results[0][1]==results[1][1]
        return results[0][1]["delivery"]
    code,data=request("POST",path,body,key);assert code==202
    return data["delivery"]

def read(value):
    code,data=request("GET","/delivery/orders/"+value);assert code==200
    return data["delivery"]

def wait_state(value,state):
    return wait_for(lambda:d if (d:=read(value))["state"]==state else None)

def handoff(value,action,concurrent=False):
    current=read(value)
    body={"action":action,"expected_version":current["order_version"],
        "courier_id":current["assignment"]["courier_id"],"assignment_id":current["assignment"]["assignment_id"]}
    key=str(uuid.uuid4());path="/delivery/orders/"+value+"/actions"
    if concurrent:
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:request("POST",path,body,key),(1,2)))
        assert all(code==202 for code,_ in results) and results[0][1]==results[1][1]
    else:
        code,_=request("POST",path,body,key);assert code==202
    return wait_state(value,"PICKED_UP" if action=="pickup" else "DELIVERED")

def capacity(zone):
    code,data=request("GET","/delivery/capacity?zone="+zone);assert code==200
    assert sum(data["counts"].values())==data["total"]
    return data

def slot_counts(zone):
    with pymysql.connect(host=os.environ["DB_PRIVATE_IP"],port=int(os.environ["DB_PORT"]),user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],database=os.environ["DB_NAME"]) as connection:
        with connection.cursor() as q:
            q.execute("SELECT COUNT(*),COUNT(DISTINCT s.courier_id),COUNT(DISTINCT s.order_id) FROM delivery_active_slots_v1 s JOIN delivery_couriers_v1 c ON c.id=s.courier_id WHERE c.zone=%s",(zone,))
            values=q.fetchone();assert len(set(values))==1
            return values[0]

def exercise_delivery():
    zone_data=new_zone("capacity_release");zone=zone_data["zone"]
    config=kafka_config("delivery-smoke-observer")
    config.update({"group.id":"delivery-smoke-"+uuid.uuid4().hex,"auto.offset.reset":"earliest","enable.auto.commit":False,"log_level":0})
    observer=Consumer(config);observer.subscribe(["delivery_assignments"])
    seen={}
    try:
        first=ready_fixture();second=ready_fixture()
        first_id,second_id=first["order_id"],second["order_id"]
        dispatch(first,zone,concurrent=True);dispatch(second,zone,concurrent=True)
        def waiting():
            graph=managed_graph(first_id)
            node=next((n for n in graph["nodes"] if n["key"]=="assign"),None) if graph else None
            return node if node and node["reason_code"]=="courier_unavailable" else None
        node=wait_for(waiting)
        assert node["attempt_count"]==0 and not node["ready_for_assignment"]
        assert read(first_id)["state"]==read(second_id)["state"]=="WAITING" and slot_counts(zone)==0
        print("Explicit ready-order dispatch waits for real courier capacity without failed-attempt loops: PASS",flush=True)

        a=courier(zone,"Courier A");b=courier(zone,"Courier B",False);c=courier(zone,"Courier C",False)
        assigned=wait_state(first_id,"ASSIGNED")
        assert assigned["assignment"]["courier_id"]==a["courier_id"]
        assert read(second_id)["state"]=="WAITING" and slot_counts(zone)==1
        assert capacity(zone)["counts"]["ASSIGNED"]==1
        code,_=request("POST","/delivery/orders/"+first_id+"/actions",{"action":"deliver",
            "expected_version":assigned["order_version"],"courier_id":a["courier_id"],"assignment_id":assigned["assignment"]["assignment_id"]},str(uuid.uuid4()))
        assert code==409
        offline=presence(a["courier_id"],False,False)
        assert offline["state"]=="RECONCILIATION_REQUIRED" and offline["occupied"] and slot_counts(zone)==1
        assert read(second_id)["state"]=="WAITING"
        print("Competing orders reserve one courier exclusively; offline/stale busy couriers retain their assignment: PASS",flush=True)

        presence(a["courier_id"],True,False)
        picked=handoff(first_id,"pickup",True)
        assert capacity(zone)["counts"]["DELIVERING"]==1 and slot_counts(zone)==1
        delivered=handoff(first_id,"deliver",True)
        assert delivered["delivery_seconds"]>=0 and slot_counts(zone)==0
        assert read(second_id)["state"]=="WAITING"
        presence(b["courier_id"],True,True)
        second_assignment=wait_state(second_id,"ASSIGNED")
        assert second_assignment["assignment"]["courier_id"]==b["courier_id"]
        presence(a["courier_id"],True,True)
        counts=capacity(zone)["counts"]
        assert (counts["AVAILABLE"],counts["ASSIGNED"],counts["OFFLINE"])==(1,1,1)
        handoff(second_id,"pickup",True);handoff(second_id,"deliver",True)
        assert slot_counts(zone)==0
        for row in (a,b):presence(row["courier_id"],False,False)
        print("Explicit pickup/delivery and idempotent retries release capacity atomically and unblock the next order: PASS",flush=True)

        def graph_complete():
            graph=managed_graph(first_id)
            nodes={n["key"]:n for n in graph["nodes"]} if graph else {}
            return nodes if all(nodes.get(k,{}).get("state")=="SUCCEEDED" for k in ("assign","pickup","deliver")) else None
        nodes=wait_for(graph_complete)
        assert nodes["pickup"]["prerequisites_complete"]==2
        for value in (first_id,second_id):
            code,events=call("GET","/orders/"+value+"/events",base="http://order-service:8081")
            assert code==200 and [e["to_state"] for e in events["events"]]==[
                "PENDING_PAYMENT","PAID","KITCHEN_ACCEPTED","PREPARING","READY","COURIER_ASSIGNED","PICKED_UP","DELIVERED"]
        def all_published():
            code,data=request("GET","/delivery/events?zone="+zone+"&limit=200");assert code==200
            if all(e["publication"]["published"] for e in data["events"]):return data["events"]
            return None
        events=wait_for(all_published)
        expected={e["event_id"] for e in events}
        def observed():
            for index in range(100):
                message=observer.poll(0.1 if index==0 else 0)
                if message is None:break
                assert not message.error()
                try:payload=json.loads(message.value())
                except (ValueError,UnicodeError):continue
                if isinstance(payload,dict) and payload.get("zone")==zone:seen[payload["event_id"]]=payload
            return expected<=set(seen)
        wait_for(observed)
        data=json.dumps(events)+json.dumps(seen)+json.dumps(capacity(zone))
        assert all(word not in data for word in ("lease_token","owner_session","delivery_snapshot","DB_PASSWORD","phone"))
        assert sum(e["event_type"]=="CourierAssigned" for e in events)==2
        assert sum(e["event_type"]=="OrderDelivered" for e in events)==2
        print("Actual Payment -> Kitchen -> Delivery flow, pickup AND-join, persisted capacity history and acknowledged Kafka events: PASS",flush=True)
        return {"delivery_order_id":first_id,"second_delivery_order_id":second_id,"zone":zone,"run_id":zone_data["run_id"],"delivery_state":"DELIVERED"}
    finally:
        observer.close()

def main():
    print(json.dumps({"result":"PASS",**exercise_delivery()}),flush=True)

if __name__=="__main__":
    main()
