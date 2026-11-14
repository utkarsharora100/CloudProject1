from common.deployment import firestore_client
"""Minimal kitchen compatibility bridge; v2 orders queue only after payment."""
from datetime import datetime, timezone
import hashlib
import json
import time
import uuid
from confluent_kafka import Consumer, TopicPartition
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from common.kafka import kafka_config
from local.environment import require_local_emulator
from order_service.outbox import confirmed_publish, create_producer


def kitchen_trigger(event):
    return (isinstance(event, dict) and event.get("schema_version") == 2
            and event.get("event_type") == "OrderPaymentSucceeded" and event.get("status") == "PAID")


class KitchenBridge:
    def __init__(self, database, producer):
        self.database = database
        self.producer = producer

    def process_record(self, topic, partition, offset, raw):
        try:
            if not isinstance(raw, bytes) or len(raw)>65536:
                raise ValueError()
            event=json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            key=hashlib.sha256(f"{topic}:{partition}:{offset}".encode()).hexdigest()
            record={"topic":topic,"partition":partition,"offset":offset,
                "payload_digest":hashlib.sha256(raw or b"").hexdigest(),"reason":"invalid_json"}
            # Persist metadata before the caller commits this broker offset.
            self.database.collection("kitchen_bridge_quarantine").document(key).set(record,retry=None,timeout=3)
            return False
        return self.process(event)

    def process(self, event):
        if not kitchen_trigger(event):
            return False
        try:
            source_id = str(uuid.UUID(event["event_id"]))
            order_id = str(uuid.UUID(event["order_id"]))
            restaurant_id = event["restaurant_id"]
            if not isinstance(restaurant_id, str) or "/" in restaurant_id or not restaurant_id:
                raise ValueError
            preparation = event["estimated_prep_time_mins"]
            if type(preparation) is not int or not 1 <= preparation <= 240:
                raise ValueError
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ValueError("Invalid kitchen bridge event.") from None
        restaurant = self.database.collection("restaurants").document(restaurant_id)
        ticket = restaurant.collection("kitchen_tickets").document(order_id)
        inbox = restaurant.collection("kitchen_bridge_inbox").document(source_id)

        @firestore.transactional
        def prepare(transaction):
            prior = inbox.get(transaction=transaction, retry=None, timeout=3)
            if prior.exists:
                return prior.to_dict()
            existing = ticket.get(transaction=transaction, retry=None, timeout=3)
            if existing.exists and existing.to_dict().get("source_event_id") != source_id:
                raise ValueError("Kitchen ticket ownership requires reconciliation.")
            stamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            payload = {"schema_version": 2, "event_id": str(uuid.uuid5(uuid.NAMESPACE_OID, source_id + ":kitchen-queued")),
                       "source_event_id": source_id, "order_id": order_id, "restaurant_id": restaurant_id,
                       "status": "Awaiting_Kitchen_Acceptance", "event_type": "KitchenQueued", "timestamp": stamp}
            if not existing.exists:
                transaction.create(ticket, {"order_id": order_id, "source_event_id": source_id,
                    "customer_id": event["customer_id"], "status": "Awaiting_Kitchen_Acceptance",
                    "received_at": stamp, "estimated_prep_time_mins": preparation,
                    "items": event.get("items", [])})
            record = {"published": False, "payload": payload}
            transaction.create(inbox, record)
            return record

        record = prepare(self.database.transaction(max_attempts=5))
        if not record["published"]:
            confirmed_publish(self.producer, "kitchen_status", order_id, record["payload"])
            inbox.update({"published": True}, retry=None, timeout=3)
        return True


def main():
    database_client = firestore_client()
    config = kafka_config("kitchen-payment-bridge")
    config.update({"group.id": "kitchen-workflow-group", "auto.offset.reset": "earliest",
                   "enable.auto.commit": False, "log_level": 0})
    consumer = Consumer(config)
    producer = create_producer("kitchen-payment-bridge-producer")
    database = database_client
    bridge = KitchenBridge(database, producer)
    consumer.subscribe(["orders"])
    print("Kitchen bridge waits for confirmed paid-order events; explicit kitchen stages are handled by Kitchen API workers.", flush=True)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                time.sleep(0.5)
                continue
            try:
                bridge.process_record(message.topic(), message.partition(), message.offset(), message.value())
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                # Rewind this partition instead of committing past unfinished work.
                # No event contents, addresses, credentials or exception text logged.
                print("Kitchen bridge processing deferred; retrying the same event.", flush=True)
                consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
