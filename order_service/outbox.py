"""At-least-once Kafka relay with durable retries and per-order event ordering."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import time
import uuid
from confluent_kafka import Producer
from sqlalchemy import exists, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased
from common.kafka import kafka_config
from order_service.models import Outbox, db, utc_now
from order_service.storage import create_storage_app


HEARTBEAT = Path("/tmp/order-outbox-heartbeat")
LEASE_SECONDS = 30


class DeliveryUnconfirmed(Exception):
    pass


def create_producer(client_id="order-outbox"):
    config = kafka_config(client_id)
    config.update({"enable.idempotence": True, "acks": "all", "message.timeout.ms": 5000,
                   "request.timeout.ms": 3000, "retry.backoff.ms": 250,
                   "log_level": 0, "error_cb": lambda error: None})
    return Producer(config)


def confirmed_publish(producer, topic, key, payload):
    outcome = []
    def delivered(error, message):
        outcome.append(error is None)
    try:
        producer.produce(topic=topic, key=str(key),
                         value=json.dumps(payload, sort_keys=True, separators=(",", ":")),
                         on_delivery=delivered)
        remaining = producer.flush(timeout=6)
    except Exception:
        raise DeliveryUnconfirmed("Kafka acknowledgement was not confirmed.") from None
    if remaining or outcome != [True]:
        raise DeliveryUnconfirmed("Kafka acknowledgement was not confirmed.")


def claim_next(now=None):
    now = utc_now() if now is None else now
    db.session.rollback()
    prior = aliased(Outbox)
    blocked = exists().where(prior.order_id == Outbox.order_id,
                             prior.sequence < Outbox.sequence, prior.published_at.is_(None))
    query = select(Outbox).where(Outbox.published_at.is_(None), Outbox.available_at <= now,
                                 or_(Outbox.lease_until.is_(None), Outbox.lease_until <= now),
                                 ~blocked).order_by(Outbox.created_at, Outbox.sequence, Outbox.event_id).limit(1)
    row = db.session.execute(query.with_for_update(skip_locked=True)).scalar_one_or_none()
    if row is None:
        db.session.rollback()
        return None
    row.claimed_by = str(uuid.uuid4())
    row.lease_until = now + timedelta(seconds=LEASE_SECONDS)
    row.attempts += 1
    claim = {"event_id": row.event_id, "order_id": row.order_id, "topic": row.topic,
             "payload": deepcopy(row.payload), "token": row.claimed_by, "attempts": row.attempts}
    db.session.commit()
    return claim


def finish_claim(claim, *, published, now=None):
    now = utc_now() if now is None else now
    db.session.rollback()
    row = db.session.execute(select(Outbox).where(
        Outbox.event_id == claim["event_id"]).with_for_update()).scalar_one_or_none()
    if row is None or row.claimed_by != claim["token"] or row.lease_until is None or row.lease_until <= now:
        db.session.rollback()
        return False
    if published:
        row.published_at = now
        row.last_error_code = None
    else:
        row.last_error_code = "delivery_unconfirmed"
        row.available_at = now + timedelta(seconds=min(2 ** min(row.attempts, 5), 30))
    row.claimed_by = None
    row.lease_until = None
    db.session.commit()
    return True


def dispatch_once(producer):
    claim = claim_next()
    if claim is None:
        return "idle"
    try:
        confirmed_publish(producer, claim["topic"], claim["order_id"], claim["payload"])
    except DeliveryUnconfirmed:
        finish_claim(claim, published=False)
        return "retry"
    return "published" if finish_claim(claim, published=True) else "claim_expired"


def main():
    app = create_storage_app()
    producer = create_producer()
    print("Local order outbox relay started; persisted events retry until acknowledged.", flush=True)
    try:
        while True:
            HEARTBEAT.touch()
            with app.app_context():
                try:
                    result = dispatch_once(producer)
                except SQLAlchemyError:
                    db.session.rollback()
                    app.logger.error("Order outbox database operation failed; retrying.")
                    result = "retry"
                except Exception:
                    db.session.rollback()
                    app.logger.error("Order outbox operation failed; retrying.")
                    result = "retry"
            HEARTBEAT.touch()
            if result != "published":
                time.sleep(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
