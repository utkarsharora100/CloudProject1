"""Real paid-order kitchen actions, refunds, immutable snapshots and Firestore views."""
from concurrent.futures import ThreadPoolExecutor
import json
import time
import uuid
from google.cloud import firestore
from local.environment import require_local_emulator
from local.scheduler_smoke_test import call, managed_graph, order_fixture, wait_for
from local.payment_smoke_test import post as payment_post, wait_state as payment_wait, ops
from kitchen_workflow.projection import ProjectionRepository
from order_service.event_consumer import KitchenBridge

BASE = "http://kitchen-api:8087"

def read(order_id):
    code, value = call("GET", "/kitchen/orders/"+order_id, base=BASE)
    return value["ticket"] if code == 200 else None

def wait_state(order_id, state):
    return wait_for(lambda: value if (value := read(order_id)) and value["state"] == state else None)

def paid_fixture():
    order = order_fixture()
    payment_post(order)
    payment = payment_wait(order["order_id"], "CAPTURED")
    ticket = wait_state(order["order_id"], "WAITING")
    return order, payment, ticket

def action(ticket, name, *, reason=None, concurrent=False):
    body = {"action": name, "expected_version": ticket["order_version"]}
    if reason is not None:
        body["reason"] = reason
    path = "/kitchen/orders/"+ticket["order_id"]+"/actions"
    key = str(uuid.uuid4())
    if concurrent:
        with ThreadPoolExecutor(max_workers=2) as pool:
            values = list(pool.map(lambda _: call("POST", path, body, key=key, base=BASE), (1,2)))
        assert all(code == 202 for code, _ in values), "Concurrent kitchen action did not replay."
        assert values[0][1] == values[1][1]
        return values[0][1]
    code, value = call("POST", path, body, key=key, base=BASE)
    assert code == 202, "Kitchen action was rejected."
    return value

def exercise_kitchen():
    project = require_local_emulator()
    database = firestore.Client(project=project)
    unpaid = order_fixture()
    code,_ = call("POST", "/kitchen/orders/"+unpaid["order_id"]+"/actions",
        {"action":"accept","expected_version":1}, key=str(uuid.uuid4()), base=BASE)
    assert code == 409
    assert read(unpaid["order_id"]) is None

    order,payment,ticket = paid_fixture()
    order_id = order["order_id"]
    assert ticket["items"] == [{"item_id":"meal","name":"Demo meal","quantity":1}]
    code,_ = call("PATCH", "/restaurants/"+order["restaurant_id"], {"estimated_prep_time_mins":99},
                  base="http://restaurant-api:8084")
    assert code == 200
    code,_ = call("PUT", "/restaurants/"+order["restaurant_id"]+"/menu/meal",
        {"name":"Changed menu meal","price_minor":19999}, base="http://restaurant-api:8084")
    assert code == 200
    assert read(order_id)["estimated_prep_time_mins"] == 2
    assert read(order_id)["items"][0]["name"] == "Demo meal"
    action(ticket,"accept",concurrent=True)
    accepted = wait_state(order_id,"ACCEPTED")
    assert accepted["order_status"] == "KITCHEN_ACCEPTED"
    code,_ = call("POST", "/kitchen/orders/"+order_id+"/actions",
        {"action":"ready","expected_version":accepted["order_version"]},key=str(uuid.uuid4()),base=BASE)
    assert code == 409
    code,_ = call("POST", "/orders/"+order_id+"/transitions",
        {"to_state":"PREPARING","expected_version":accepted["order_version"]},key=str(uuid.uuid4()),base="http://order-service:8081")
    assert code == 409
    action(accepted,"start_preparation",concurrent=True)
    preparing = wait_state(order_id,"PREPARING")
    time.sleep(1)
    assert read(order_id)["state"] == "PREPARING" and read(order_id)["ready_at"] is None
    action(preparing,"ready",concurrent=True)
    ready = wait_state(order_id,"READY")
    assert ready["accepted_at"] <= ready["preparation_started_at"] < ready["ready_at"]
    assert all(c["state"] == "APPLIED" and c["task_id"] and c["applied_by"] == "worker" for c in ready["commands"])
    def complete_graph():
        graph = managed_graph(order_id)
        nodes = {n["key"]:n for n in graph["nodes"]} if graph else {}
        return nodes if nodes.get("prepare",{}).get("state") == "SUCCEEDED" else None
    nodes = wait_for(complete_graph)
    assert nodes["prepare"]["attempt_count"] >= 2
    assert nodes["pickup"]["state"] == "BLOCKED" and nodes["pickup"]["prerequisites_complete"] == 1
    code,history = call("GET", "/orders/"+order_id+"/events",base="http://order-service:8081")
    assert code == 200
    assert [e["to_state"] for e in history["events"]] == ["PENDING_PAYMENT","PAID","KITCHEN_ACCEPTED","PREPARING","READY"]
    print("Payment-gated saved kitchen snapshots, concurrent action replay, explicit preparation/ready and real worker dependencies: PASS",flush=True)

    ref = database.collection("restaurants").document(order["restaurant_id"]).collection("kitchen_tickets").document(order_id)
    wait_for(lambda: (snapshot.to_dict() if (snapshot := ref.get(retry=None,timeout=3)).exists else {}).get("workflow_state") == "READY")
    current = ref.get(retry=None,timeout=3).to_dict()
    older = {**current,"restaurant_id":order["restaurant_id"],"kitchen_version":1,"status":"Awaiting_Kitchen_Acceptance","workflow_state":"WAITING"}
    assert ProjectionRepository(database).put(older) is False
    assert ref.get(retry=None,timeout=3).to_dict() == current
    paid_event = next(e for e in history["events"] if e["to_state"] == "PAID")
    inbox = database.collection("restaurants").document(order["restaurant_id"]).collection("kitchen_bridge_inbox").document(paid_event["event_id"])
    wait_for(lambda: (snapshot.to_dict() if (snapshot := inbox.get(retry=None,timeout=3)).exists else {}).get("published") is True)
    class NoDuplicate:
        def produce(self, **kwargs):
            raise AssertionError("Duplicate paid event attempted another queue notification.")
    assert KitchenBridge(database,NoDuplicate()).process({"schema_version":2,"event_type":"OrderPaymentSucceeded",
        "status":"PAID","event_id":paid_event["event_id"],"order_id":order_id,"restaurant_id":order["restaurant_id"],
        "estimated_prep_time_mins":2,"customer_id":order["customer_id"],"items":ready["items"]})
    assert ref.get(retry=None,timeout=3).to_dict() == current
    code,queue = call("GET","/kitchen/restaurants/"+order["restaurant_id"]+"/orders?state=READY&limit=1",base=BASE)
    assert code == 200 and queue["tickets"][0]["order_id"] == order_id
    code,events = call("GET","/kitchen/orders/"+order_id+"/events?limit=2",base=BASE)
    assert code == 200 and events["next_after"] is not None
    assert all(word not in json.dumps(ready)+json.dumps(events) for word in ("lease_token","owner_session","delivery_snapshot","DB_PASSWORD"))
    print("Versioned Firestore view, stable source identity and duplicate/delayed-event non-regression; safe queue/history APIs: PASS",flush=True)

    rejected_order,rejected_payment,waiting = paid_fixture()
    action(waiting,"reject",reason="capacity",concurrent=True)
    payment_wait(rejected_order["order_id"],"REFUNDED")
    rejected = wait_state(rejected_order["order_id"],"REJECTED")
    assert rejected["commands"][0]["applied_by"] == "human" and rejected["commands"][0]["task_id"] is None
    receipts = ops(rejected_payment["payment_id"])
    assert sorted(r[0] for r in receipts) == ["CAPTURE","REFUND"]
    assert len(receipts) == 2 and all(r[1] == "SUCCEEDED" for r in receipts)
    accepted_order,accepted_payment,waiting = paid_fixture()
    action(waiting,"accept")
    accepted = wait_state(accepted_order["order_id"],"ACCEPTED")
    action(accepted,"reject",reason="closing")
    payment_wait(accepted_order["order_id"],"REFUNDED")
    assert read(accepted_order["order_id"])["state"] == "REJECTED"
    print("Rejection before cooking, including after acceptance, uses Payment-owned full refund with no duplicate refund: PASS",flush=True)

    raced_order,raced_payment,waiting = paid_fixture()
    action(waiting,"accept")
    accepted = wait_state(raced_order["order_id"],"ACCEPTED")
    race_id = raced_order["order_id"]
    version = accepted["order_version"]
    def start():
        return call("POST","/kitchen/orders/"+race_id+"/actions",
            {"action":"start_preparation","expected_version":version},key=str(uuid.uuid4()),base=BASE)
    def cancel():
        return call("POST","/payments/orders/"+race_id+"/cancel",
            {"expected_version":version},key=str(uuid.uuid4()),base="http://payment-service:8086")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(start),pool.submit(cancel)]
        results = [f.result() for f in futures]
    assert all(code in {202,409} for code,_ in results) and any(code == 202 for code,_ in results)
    def resolved():
        current = read(race_id)
        return current if current and current["state"] in {"PREPARING","CANCELLED"} else None
    resolved_ticket = wait_for(resolved)
    if resolved_ticket["state"] == "CANCELLED":
        payment_wait(race_id,"REFUNDED")
    else:
        code,_ = call("POST","/kitchen/orders/"+race_id+"/actions",
            {"action":"reject","expected_version":resolved_ticket["order_version"]},key=str(uuid.uuid4()),base=BASE)
        assert code == 409
        assert len(ops(raced_payment["payment_id"])) == 1
        action(resolved_ticket,"ready")
        wait_state(race_id,"READY")
    print("Preparation/cancellation race preserves the cooking/refund policy and cancels obsolete kitchen commands: PASS",flush=True)
    return {"kitchen_order_id":order_id,"kitchen_rejected_order_id":rejected_order["order_id"],"kitchen_state":"READY"}

def main():
    print(json.dumps({"result":"PASS",**exercise_kitchen()}),flush=True)

if __name__ == "__main__":
    main()
