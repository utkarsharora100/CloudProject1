"""Real MySQL/ZooKeeper scheduler checks; demo tasks never advance business state."""
import json
import os
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pymysql
from local.environment import require_local_emulator


BASE = "http://task-api:8085"


def call(method, path, payload=None, *, key=None, base=BASE):
    request = Request(base + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **({"Idempotency-Key": key} if key else {})})
    try:
        response = urlopen(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        content = response.read()
        return response.status, json.loads(content) if content else None


def wait_for(predicate, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise AssertionError("Timed out waiting for verified scheduler progress.")


def create_graph(tasks):
    key = str(uuid.uuid4())
    payload = {"tasks": tasks}
    code, value = call("POST", "/workflows", payload, key=key)
    assert code == 201
    code, repeated = call("POST", "/workflows", payload, key=key)
    assert code == 201 and repeated == value
    return value


def graph(value):
    code, result = call("GET", f"/workflows/{value['workflow_id']}/graph")
    assert code == 200
    return result


def finished(value, expected):
    result = graph(value)
    nodes = {node["key"]: node for node in result["nodes"]}
    return nodes if all(nodes[name]["state"] == state for name, state in expected.items()) else None


def managed_graph(order_id):
    code, result = call("GET", "/workflows?order_id=" + order_id)
    assert code == 200
    selected = [workflow for workflow in result["workflows"] if workflow["managed"]]
    assert len(selected) <= 1, "Order workflow was materialized more than once."
    if not selected:
        return None
    return graph(selected[0])


def order_fixture():
    code, customer = call("POST", "/customers", {"name": "Scheduler Demo", "email": "scheduler-" + uuid.uuid4().hex + "@example.test"}, base="http://customer-service:8083")
    assert code == 201
    customer_id = customer["customer"]["id"]
    code, address = call("POST", f"/customers/{customer_id}/addresses", {"label": "Demo home", "line1": "Synthetic scheduler block", "city": "Demo City", "postal_code": "000000", "country_code": "IN"}, base="http://customer-service:8083")
    assert code == 201
    code, restaurant = call("POST", "/restaurants", {"name": "Scheduler Demo Kitchen", "is_open": True, "estimated_prep_time_mins": 2}, base="http://restaurant-api:8084")
    assert code == 201
    restaurant_id = restaurant["restaurant"]["restaurant_id"]
    code, _ = call("PUT", f"/restaurants/{restaurant_id}/menu/meal", {"name": "Demo meal", "price_minor": 12999}, base="http://restaurant-api:8084")
    assert code == 201
    code, order = call("POST", "/orders", {"customer_id": customer_id, "address_id": address["address"]["id"], "restaurant_id": restaurant_id, "items": [{"item_id": "meal", "quantity": 1}]}, key=str(uuid.uuid4()), base="http://order-service:8081")
    assert code == 201
    return order


def exercise_scheduler(order_result=None):
    require_local_emulator()
    def ready():
        code, status = call("GET", "/scheduler")
        code2, workers = call("GET", "/workers")
        return status if code == 200 and status["leader_available"] and code2 == 200 and len(workers["workers"]) >= 2 else None
    wait_for(ready)
    code, _ = call("POST", "/workflows", {"tasks": [{"key": "cycle", "kind": "demo", "depends_on": ["cycle"]}]}, key=str(uuid.uuid4()))
    assert code == 400
    dag = create_graph([{"key": "left", "kind": "demo", "params": {"duration_ms": 1500}},
                        {"key": "right", "kind": "demo", "params": {"duration_ms": 1500}},
                        {"key": "join", "kind": "demo", "depends_on": ["left", "right"]}])
    nodes = wait_for(lambda: finished(dag, {"left": "SUCCEEDED", "right": "SUCCEEDED", "join": "SUCCEEDED"}))
    left, right, join = (nodes[key] for key in ("left", "right", "join"))
    assert left["attempts"][0]["worker_id"] != right["attempts"][0]["worker_id"]
    assert left["attempts"][0]["started_at"] < right["attempts"][0]["finished_at"]
    assert right["attempts"][0]["started_at"] < left["attempts"][0]["finished_at"]
    assert join["started_at"] >= max(left["completed_at"], right["completed_at"])
    assert join["prerequisites_complete"] == 2 and len(join["prerequisites"]) == 2
    serialized = json.dumps(graph(dag))
    assert "lease_token" not in serialized and "owner_session" not in serialized
    print("Real ZooKeeper worker ownership, parallel execution, AND dependencies and safe graph inspection: PASS", flush=True)

    retry = create_graph([{"key": "retry", "kind": "demo", "params": {"fail_attempts": 1}}])
    nodes = wait_for(lambda: finished(retry, {"retry": "SUCCEEDED"}))
    retried = nodes["retry"]
    assert retried["attempt_count"] == 2 and retried["failure_count"] == 1
    assert [attempt["outcome"] for attempt in retried["attempts"]] == ["FAILED", "SUCCEEDED"]
    code, events = call("GET", f"/tasks/{retried['task_id']}/events")
    assert code == 200 and any(event["to_state"] == "RETRY_WAIT" for event in events["events"])
    failure = create_graph([{"key": "fail", "kind": "demo", "max_failures": 1, "params": {"fail_attempts": 1}},
        {"key": "blocked", "kind": "demo", "depends_on": ["fail"]}, {"key": "parallel", "kind": "demo"}])
    wait_for(lambda: finished(failure, {"fail": "FAILED", "blocked": "SKIPPED", "parallel": "SUCCEEDED"}))
    print("Durable retry history, bounded failure budget and independent parallel branches: PASS", flush=True)

    order = order_fixture()
    def payment_waiting():
        result = managed_graph(order["order_id"])
        if not result:
            return None
        nodes = {node["key"]: node for node in result["nodes"]}
        return result if nodes["payment"]["state"] == "READY" and nodes["kitchen_accept"]["state"] == "BLOCKED" else None
    generated = wait_for(payment_waiting)
    generated_id = generated["workflow_id"]
    def transition(state):
        nonlocal order
        code, order = call("POST", f"/orders/{order['order_id']}/transitions", {"to_state": state, "expected_version": order["version"]}, key=str(uuid.uuid4()), base="http://order-service:8081")
        assert code == 200
    transition("PAID")
    def paid():
        nodes = {node["key"]: node for node in graph({"workflow_id": generated_id})["nodes"]}
        return nodes if nodes["payment"]["state"] == "SUCCEEDED" and nodes["kitchen_accept"]["state"] == "READY" else None
    nodes = wait_for(paid)
    assert nodes["payment"]["completion_source"] == "order_history" and nodes["payment"]["attempt_count"] == 0
    assert nodes["payment"]["confirmed_event_id"]
    transition("REFUND_PENDING")
    wait_for(lambda: finished({"workflow_id": generated_id}, {"refund": "READY", "kitchen_accept": "CANCELLED"}))
    transition("REFUNDED")
    nodes = wait_for(lambda: finished({"workflow_id": generated_id}, {"refund": "SUCCEEDED"}))
    assert nodes["refund"]["completion_source"] == "order_history"
    code, saved = call("GET", "/orders/" + order["order_id"], base="http://order-service:8081")
    assert code == 200 and saved["order"]["status"] == "REFUNDED"
    print("Order workflow materialization, payment gating, conditional refund branch and honest order-history confirmation: PASS", flush=True)

    if order_result is not None:
        def delivered():
            result = managed_graph(order_result["order_id"])
            if result is None:
                return None
            nodes = {node["key"]: node for node in result["nodes"]}
            return result if all(nodes[key]["state"] == "SUCCEEDED" for key in ("payment", "kitchen_accept", "prepare", "assign", "pickup", "deliver")) else None
        completed = wait_for(delivered)
        pickup = next(node for node in completed["nodes"] if node["key"] == "pickup")
        assert pickup["prerequisites_complete"] == 2
        assert all(node["completion_source"] == "order_history" for node in completed["nodes"] if node["key"] in {"payment", "kitchen_accept", "prepare", "assign", "pickup", "deliver"})
        print("Completed real order states feed its dependency graph without fabricated worker attempts: PASS", flush=True)
    with pymysql.connect(host=os.environ["DB_PRIVATE_IP"], port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), SUM(state='SUCCEEDED') FROM scheduler_tasks WHERE workflow_id=%s", (dag["workflow_id"],))
            assert tuple(map(int, cursor.fetchone())) == (3, 3)
            cursor.execute("SELECT COUNT(*) FROM scheduler_dependencies WHERE task_id=%s", (join["task_id"],))
            assert cursor.fetchone()[0] == 2
    return {"workflow_id": dag["workflow_id"], "order_workflow_id": generated_id, "order_id": order["order_id"]}


def main():
    print(json.dumps({"result": "PASS", **exercise_scheduler()}), flush=True)


if __name__ == "__main__":
    main()
