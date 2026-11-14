"""Real local order validation, concurrency, snapshots and outbox verification."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pymysql
from confluent_kafka import Consumer
from google.cloud import firestore
from kazoo.client import KazooClient
from common.kafka import kafka_config
from local.customer_smoke_test import call as customer_call, exercise_customer_service
from local.environment import require_local_emulator
from local.restaurant_smoke_test import call as restaurant_call, exercise_restaurant_service
from order_service.event_consumer import KitchenBridge


BASE = "http://order-service:8081"


def call(method, path, payload=None, key=None):
    request = Request(BASE + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **({"Idempotency-Key": key} if key else {})})
    try:
        response = urlopen(request, timeout=20)
    except HTTPError as error:
        response = error
    with response:
        content = response.read()
        return response.status, json.loads(content) if content else None, response.headers


def wait_until(predicate, *, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("Timed out waiting for confirmed local workflow progress.")


def exercise_order_service(customer=None, restaurant=None):
    project = require_local_emulator()
    customer = exercise_customer_service() if customer is None else customer
    restaurant = exercise_restaurant_service() if restaurant is None else restaurant
    customer_id, address_id = customer["customer_id"], customer["address_id"]
    restaurant_id = restaurant["restaurant_id"]
    payload = {"customer_id": customer_id, "address_id": address_id, "restaurant_id": restaurant_id,
               "items": [{"item_id": "veg-meal", "quantity": 2}]}
    key = "MixedCase-" + str(uuid.uuid4())
    config = kafka_config("order-smoke-observer")
    config.update({"group.id": "order-smoke-" + uuid.uuid4().hex, "auto.offset.reset": "earliest",
                   "enable.auto.commit": False, "log_level": 0})
    observer = Consumer(config)
    observer.subscribe(["orders", "kitchen_status"])
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: call("POST", "/orders", payload, key), (1, 2)))
        assert all(code == 201 for code, _, _ in results), "Concurrent create did not replay successfully."
        current = results[0][1]
        assert current == results[1][1] and current["total_minor"] == 25998
        assert current["status"] == "PENDING_PAYMENT" and current["version"] == 1
        order_id = current["order_id"]
        path = "/orders/" + order_id
        code, snapshot, _ = call("GET", path)
        assert code == 200
        original = snapshot["order"]
        code, case_variant, _ = call("POST", "/orders", payload, key.lower())
        assert code == 201 and case_variant["order_id"] != order_id, "Distinct case-sensitive keys were merged."
        code, _, _ = call("POST", "/orders/" + case_variant["order_id"] + "/transitions",
            {"to_state": "CANCELLED", "expected_version": 1}, str(uuid.uuid4()))
        assert code == 200
        orders_seen = {}
        ordered_ids = []
        topic_events = {}

        def poll():
            # Drain a bounded batch so retained history does not consume the
            # business timeout one record per sleep.
            for index in range(100):
                message = observer.poll(0.1 if index==0 else 0)
                if message is None:
                    return
                assert not message.error(), "Kafka observer error."
                try:
                    event = json.loads(message.value())
                except (ValueError, UnicodeError):
                    continue
                if not isinstance(event, dict) or str(event.get("order_id")) != order_id:
                    continue
                topic_events[message.topic()] = event
                if message.topic() == "orders" and event["event_id"] not in orders_seen:
                    orders_seen[event["event_id"]] = event
                    ordered_ids.append(event["event_id"])

        def all_published():
            poll()
            code, history, _ = call("GET", path + "/events")
            assert code == 200
            return all(event["publication"]["published"] for event in history["events"])

        wait_until(all_published)
        database = firestore.Client(project=project)
        restaurant_ref = database.collection("restaurants").document(restaurant_id)
        ticket = restaurant_ref.collection("kitchen_tickets").document(order_id)
        assert not ticket.get(retry=None, timeout=3).exists, "Unpaid order entered the kitchen."
        print("Validated UUID order, integer totals, atomic MySQL/outbox writes and case-sensitive/concurrent create replay: PASS", flush=True)

        for invalid in ({**payload, "total_minor": 1}, {**payload, "items": [{"item_id": "veg-meal", "quantity": True}]}):
            code, _, _ = call("POST", "/orders", invalid, str(uuid.uuid4()))
            assert code == 400
        code, _, _ = call("POST", "/orders", {**payload, "items": [{"item_id": "paneer-wrap", "quantity": 1}]}, str(uuid.uuid4()))
        assert code == 409
        code, _, _ = call("POST", "/orders", {**payload, "items": [{"item_id": "missing", "quantity": 1}]}, str(uuid.uuid4()))
        assert code == 422
        code, other = customer_call("POST", "/customers", {"name": "Other Demo", "email": "other-" + uuid.uuid4().hex + "@example.test"})
        assert code == 201
        code, _, _ = call("POST", "/orders", {**payload, "customer_id": other["customer"]["id"]}, str(uuid.uuid4()))
        assert code == 422
        code, _, _ = restaurant_call("PATCH", "/restaurants/" + restaurant_id, {"is_open": False})
        assert code == 200
        try:
            code, _, _ = call("POST", "/orders", payload, str(uuid.uuid4()))
            assert code == 409
        finally:
            code, _, _ = restaurant_call("PATCH", "/restaurants/" + restaurant_id, {"is_open": True})
            assert code == 200
        print("Missing/wrong-owner addresses, closed restaurants, unavailable items and price tampering are rejected: PASS", flush=True)

        code, _ = customer_call("PATCH", f"/customers/{customer_id}/addresses/{address_id}", {"line1": "Changed synthetic demo block"})
        assert code == 200
        code, _, _ = restaurant_call("PUT", f"/restaurants/{restaurant_id}/menu/veg-meal", {"name": "Updated demo meal", "price_minor": 19999})
        assert code == 200
        code, _, _ = restaurant_call("PATCH", "/restaurants/" + restaurant_id, {"estimated_prep_time_mins": 99})
        assert code == 200
        code, replay, headers = call("POST", "/orders", payload, key)
        assert code == 201 and replay == current and headers["Idempotency-Replayed"] == "true"
        code, unchanged, _ = call("GET", path)
        assert code == 200 and unchanged["order"] == original
        code, _, _ = call("POST", "/orders", {**payload, "items": [{"item_id": "veg-meal", "quantity": 3}]}, key)
        assert code == 409
        print("Committed menu/address/preparation snapshots remain unchanged after upstream edits and retries: PASS", flush=True)

        def transition(state, *, shared_key=None, concurrent=False):
            nonlocal current
            body = {"to_state": state, "expected_version": current["version"]}
            transition_key = shared_key or str(uuid.uuid4())
            if concurrent:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    values = list(pool.map(lambda _: call("POST", path + "/transitions", body, transition_key), (1, 2)))
                assert all(code == 200 for code, _, _ in values), "Concurrent transition replay failed."
                assert values[0][1] == values[1][1]
                result = values[0][1]
            else:
                code, result, _ = call("POST", path + "/transitions", body, transition_key)
                assert code == 200, "Valid state transition failed."
            current = result
            return current

        code, _, _ = call("POST", path + "/transitions", {"to_state": "READY", "expected_version": 1}, str(uuid.uuid4()))
        assert code == 409
        transition("PAYMENT_FAILED")
        transition("PENDING_PAYMENT")
        transition("PAID", concurrent=True)
        code, history, _ = call("GET", path + "/events")
        paid_event_id = history["events"][-1]["event_id"]
        inbox = restaurant_ref.collection("kitchen_bridge_inbox").document(paid_event_id)

        def kitchen_queued():
            poll()
            record = inbox.get(retry=None, timeout=3)
            return (paid_event_id in orders_seen and record.exists and record.to_dict()["published"]
                    and "kitchen_status" in topic_events)
        wait_until(kitchen_queued)
        queued = ticket.get(retry=None, timeout=3).to_dict()
        assert queued["status"] == "Awaiting_Kitchen_Acceptance" and queued["estimated_prep_time_mins"] == 18
        class NoDuplicateProducer:
            def produce(self, **kwargs):
                raise AssertionError("Duplicate bridge input attempted another notification.")
        assert KitchenBridge(database, NoDuplicateProducer()).process(orders_seen[paid_event_id])
        assert ticket.get(retry=None, timeout=3).to_dict() == queued
        print("Payment gating, failed-payment retry, concurrent transition replay and deduplicated kitchen queue bridge: PASS", flush=True)

        # These are explicit local demo actor actions. Payment/Kitchen/Delivery
        # services will own the corresponding transitions in later milestones.
        for state in ("KITCHEN_ACCEPTED", "PREPARING", "READY"):
            transition(state)
        for state in ("COURIER_ASSIGNED", "PICKED_UP", "DELIVERED"):
            transition(state)
        code, _, _ = call("POST", path + "/transitions", {"to_state": "CANCELLED", "expected_version": current["version"]}, str(uuid.uuid4()))
        assert code == 409
        def complete():
            published = all_published()
            return published and len(orders_seen) == 10 and set(topic_events) == {"orders", "kitchen_status"}
        wait_until(complete)
        assert [orders_seen[key]["sequence"] for key in ordered_ids] == list(range(1, 11))
        print("Persisted lifecycle/history and per-order Kafka event ordering with explicit simulated actor actions: PASS", flush=True)

        with pymysql.connect(host=os.environ["DB_PRIVATE_IP"], port=int(os.environ["DB_PORT"]),
                             user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                             database=os.environ["DB_NAME"], connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT customer_id, restaurant_id, total_minor, status, version FROM orders_v2 WHERE id=%s", (order_id,))
                assert cursor.fetchone() == (customer_id, restaurant_id, 25998, "DELIVERED", 10)
                cursor.execute("SELECT COUNT(*) FROM order_requests_v2 WHERE scope=%s AND `key`=%s", (f"create:{customer_id}", key))
                assert cursor.fetchone()[0] == 1
                cursor.execute("SELECT COUNT(*), SUM(published_at IS NOT NULL) FROM order_outbox_v2 WHERE order_id=%s", (order_id,))
                assert tuple(map(int, cursor.fetchone())) == (10, 10)
        print("MySQL order/idempotency/outbox consistency with explicit unmanaged compatibility actors: PASS", flush=True)
        return {"order_id": order_id, "customer_id": customer_id, "address_id": address_id,
                "restaurant_id": restaurant_id, "total_minor": current["total_minor"],
                "status": current["status"], "version": current["version"]}
    finally:
        observer.close()


def main():
    print(json.dumps({"result": "PASS", **exercise_order_service()}), flush=True)


if __name__ == "__main__":
    main()
