"""Opt-in local Kafka outage + publisher recreation test; restores Kafka in finally."""
import argparse
import json
from pathlib import Path
from local.environment import require_approved_recovery_root
import subprocess
import time
import uuid
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def http(method, port, path, payload=None, key=None):
    request = Request(f"http://127.0.0.1:{port}" + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **({"Idempotency-Key": key} if key else {})})
    with urlopen(request, timeout=15) as response:
        return response.status, json.load(response)


def compose(arguments):
    result = subprocess.run(["docker", "compose", "--env-file", ".env.local.example", *arguments],
                            cwd=ROOT, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("A scoped local Docker Compose operation failed; review local service status.")
    return result.stdout


def wait_for(predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.5)
    raise RuntimeError("Timed out waiting for confirmed local recovery progress.")


def observe(event_id, order_id):
    # This mode runs in the existing local application image, not on the host.
    from confluent_kafka import Consumer
    from common.kafka import kafka_config
    from local.environment import require_local_emulator
    require_local_emulator()
    config = kafka_config("outbox-recovery-observer")
    config.update({"group.id": "recovery-" + uuid.uuid4().hex, "auto.offset.reset": "earliest",
                   "enable.auto.commit": False, "log_level": 0})
    consumer = Consumer(config)
    consumer.subscribe(["orders"])
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None or message.error():
                continue
            try:
                event = json.loads(message.value())
            except (ValueError, UnicodeError):
                continue
            if isinstance(event, dict) and event.get("event_id") == event_id and event.get("order_id") == order_id:
                if event.get("event_type") != "OrderCreated" or event.get("status") != "PENDING_PAYMENT":
                    raise RuntimeError("Recovered event has unexpected state.")
                print("Recovered stable event ID is present on the real local Kafka topic: PASS", flush=True)
                return
        raise RuntimeError("Recovered event was not observed on Kafka.")
    finally:
        consumer.close()


def run_outage_test():
    require_approved_recovery_root(ROOT)
    assert http("GET", 8081, "/health")[1]["publication"] == "transactional-outbox"
    code, customer = http("POST", 8083, "/customers", {"name": "Recovery Demo", "email": "recovery-" + uuid.uuid4().hex + "@example.test"})
    assert code == 201
    customer_id = customer["customer"]["id"]
    code, address = http("POST", 8083, f"/customers/{customer_id}/addresses", {
        "label": "Demo home", "line1": "Synthetic recovery block", "city": "Demo City", "postal_code": "000000", "country_code": "IN"})
    assert code == 201
    code, restaurant = http("POST", 8084, "/restaurants", {"name": "Recovery Demo Kitchen", "is_open": True, "estimated_prep_time_mins": 2})
    assert code == 201
    restaurant_id = restaurant["restaurant"]["restaurant_id"]
    code, _ = http("PUT", 8084, f"/restaurants/{restaurant_id}/menu/meal", {"name": "Demo meal", "price_minor": 12999})
    assert code == 201
    payload = {"customer_id": customer_id, "address_id": address["address"]["id"], "restaurant_id": restaurant_id,
               "items": [{"item_id": "meal", "quantity": 1}]}
    key = str(uuid.uuid4())
    print("Starting scoped local Kafka outage; fictional fixtures are ready.", flush=True)
    created = None
    event_id = None
    try:
        compose(["stop", "kafka"])
        code, created = http("POST", 8081, "/orders", payload, key)
        assert code == 201 and created["status"] == "PENDING_PAYMENT" and created["total_minor"] == 12999
        order_id = created["order_id"]
        path = f"/orders/{order_id}/events"
        def failed_attempt():
            events = http("GET", 8081, path)[1]["events"]
            assert len(events) == 1
            event = events[0]
            publication = event["publication"]
            assert not publication["published"]
            return event if publication["attempts"] >= 1 and publication["last_error_code"] == "delivery_unconfirmed" else None
        failed = wait_for(failed_attempt, seconds=40)
        event_id = failed["event_id"]
        print("Order creation stayed available; unacknowledged event persisted with retry metadata: PASS", flush=True)
        compose(["up", "-d", "--force-recreate", "--no-deps", "--wait", "--wait-timeout", "90", "order-outbox"])
        code, replay = http("POST", 8081, "/orders", payload, key)
        assert code == 201 and replay == created
        assert http("GET", 8081, path)[1]["events"][0]["event_id"] == event_id
        print("Publisher container recreated during outage; order replay and stable pending event survived: PASS", flush=True)
    finally:
        compose(["up", "-d", "--wait", "--wait-timeout", "120", "kafka"])
        print("Local Kafka restored and healthy.", flush=True)
    def published():
        events = http("GET", 8081, f"/orders/{created['order_id']}/events")[1]["events"]
        assert len(events) == 1 and events[0]["event_id"] == event_id
        return events[0] if events[0]["publication"]["published"] else None
    recovered = wait_for(published)
    assert recovered["publication"]["attempts"] >= 2
    observed = compose(["run", "--rm", "--no-deps", "-T", "smoke-test", "python", "-m", "local.order_recovery_test",
                        "--observe-event", event_id, "--order-id", created["order_id"]])
    if "Recovered stable event ID is present" not in observed:
        raise RuntimeError("Local Kafka observer did not confirm delivery.")
    print("Persisted event published after Kafka recovery and observed on Kafka: PASS", flush=True)
    print(json.dumps({"result": "PASS", "order_id": created["order_id"], "event_id": event_id,
                      "attempts": recovered["publication"]["attempts"], "kafka_restored": True}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-local-kafka-outage", action="store_true")
    parser.add_argument("--observe-event", type=str)
    parser.add_argument("--order-id", type=str)
    args = parser.parse_args()
    if args.observe_event:
        if not args.order_id:
            parser.error("--order-id is required for event observation")
        observe(str(uuid.UUID(args.observe_event)), str(uuid.UUID(args.order_id)))
    elif args.allow_local_kafka_outage:
        run_outage_test()
    else:
        parser.error("Use --allow-local-kafka-outage only for the documented isolated local recovery test")


if __name__ == "__main__":
    main()
