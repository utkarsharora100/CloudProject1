"""Canonical SQL facts, idempotent materialization and durable poison-message metadata."""
import hashlib
import json
import uuid
from sqlalchemy import select
from order_service.models import Order, OrderEvent, db, utc_now
from delivery_assignment_service.models import Assignment, Event as DeliveryEvent
from notification_service.domain import CUSTOMER_TEMPLATES, COURIER_STAGES
from notification_service.models import Subscription, Message, EventInbox, StreamReceipt

def add_message(subscription, topic, event_id, sequence, occurred_at, kind, recipient_id, stage, now):
    identity = ":".join([topic, event_id, kind, str(recipient_id), "in_app"])
    value = str(uuid.uuid5(uuid.NAMESPACE_URL, "food-notification:" + identity))
    if db.session.get(Message, value) is None:
        db.session.add(Message(id=value, order_id=subscription.order_id, source_topic=topic,
            source_event_id=event_id, sequence=sequence, source_at=occurred_at,
            recipient_type=kind, recipient_id=str(recipient_id), stage=stage,
            scenario=subscription.scenario, available_at=now, created_at=now))
    if db.session.get(EventInbox, (topic, event_id)) is None:
        db.session.add(EventInbox(topic=topic, event_id=event_id, processed_at=now))

def materialize(subscription, now):
    # One subscription lock serializes both Kafka delivery and SQL reconciliation.
    events = db.session.execute(select(OrderEvent).where(OrderEvent.order_id==subscription.order_id)
        .order_by(OrderEvent.sequence)).scalars().all()
    for event in events:
        if event.to_state in CUSTOMER_TEMPLATES:
            add_message(subscription, "orders", event.event_id, event.sequence, event.occurred_at,
                        "customer", subscription.customer_id, event.to_state, now)
    if subscription.courier_updates:
        events = db.session.execute(select(DeliveryEvent).where(DeliveryEvent.order_id==subscription.order_id,
            DeliveryEvent.event_type.in_(COURIER_STAGES)).order_by(DeliveryEvent.id)).scalars().all()
        for event in events:
            assignment = db.session.get(Assignment, event.assignment_id) if event.assignment_id else None
            if (assignment and assignment.order_id==subscription.order_id
                    and assignment.courier_id==event.courier_id):
                add_message(subscription, "delivery_assignments", event.event_id, event.id,
                    event.occurred_at, "courier", event.courier_id, COURIER_STAGES[event.event_type], now)
    db.session.flush()

def reconcile_order(order_id, *, now=None):
    db.session.rollback()
    subscription = db.session.execute(select(Subscription).where(Subscription.order_id==order_id)
        .with_for_update()).scalar_one_or_none()
    if subscription:
        materialize(subscription, now or utc_now())
    db.session.commit()

def validate_source(topic, value):
    if not isinstance(value, dict) or value.get("schema_version") != 2:
        return None, "invalid_schema"
    try:
        event_id = str(uuid.UUID(value["event_id"]))
    except (KeyError, ValueError, TypeError, AttributeError):
        return None, "invalid_event_id"
    if topic=="orders":
        event = db.session.get(OrderEvent, event_id)
        order = db.session.get(Order, event.order_id) if event else None
        if (not event or not order or value.get("order_id")!=event.order_id
                or value.get("sequence")!=event.sequence or value.get("event_type")!=event.event_type
                or value.get("status")!=event.to_state or value.get("customer_id")!=order.customer_id):
            return None, "source_mismatch"
        return event.order_id, None
    if topic=="delivery_assignments":
        event = db.session.execute(select(DeliveryEvent).where(DeliveryEvent.event_id==event_id)).scalar_one_or_none()
        if (not event or value.get("order_id")!=event.order_id or value.get("event_type")!=event.event_type
                or value.get("courier_id")!=event.courier_id or value.get("assignment_id")!=event.assignment_id):
            return None, "source_mismatch"
        return event.order_id, None
    return None, "unsupported_topic"

def consume_record(topic, partition, offset, raw, *, now=None):
    now = now or utc_now()
    raw = raw if isinstance(raw, bytes) else b""
    db.session.rollback()
    prior = db.session.get(StreamReceipt, (topic, partition, offset))
    if prior:
        result = prior.outcome
        db.session.rollback()
        return result
    reason = None
    try:
        if not raw or len(raw)>65536:
            raise ValueError()
        value = json.loads(raw)
        order_id, reason = validate_source(topic, value)
    except (ValueError, UnicodeError, RecursionError):
        order_id, reason = None, "invalid_json"
    if reason:
        outcome = "QUARANTINED"
    else:
        subscription = db.session.execute(select(Subscription).where(Subscription.order_id==order_id)
            .with_for_update()).scalar_one_or_none() if order_id else None
        if subscription:
            materialize(subscription, now)
            outcome = "MATERIALIZED"
        else:
            outcome = "NO_SUBSCRIPTION"
    db.session.add(StreamReceipt(topic=topic, partition=partition, offset=offset,
        payload_digest=hashlib.sha256(raw).hexdigest(), outcome=outcome, reason=reason, received_at=now))
    db.session.commit()
    return outcome

def recover_exhausted_tasks(*, now=None):
    """Close an exhausted worker task from receipts, or expose an explicit retry."""
    from task_scheduler.models import Task
    from notification_service.models import Attempt, TransportReceipt
    from notification_service.domain import content_digest
    task_table=Task.__table__
    current=now or utc_now()
    db.session.rollback()
    ids=db.session.execute(select(Message.id).join(task_table,task_table.c.id==Message.task_id).where(
        task_table.c.state=="FAILED",Message.state.in_(["PENDING","SENDING","RETRY_WAIT"])).limit(30)).scalars().all()
    db.session.rollback()
    for value in ids:
        task_id=db.session.execute(select(Message.task_id).where(Message.id==value)).scalar_one()
        task=db.session.execute(select(task_table).where(task_table.c.id==task_id).with_for_update()).mappings().one()
        message=db.session.execute(select(Message).where(Message.id==value).with_for_update()).scalar_one()
        if task["state"]!="FAILED" or message.state not in {"PENDING","SENDING","RETRY_WAIT"}:
            db.session.rollback();continue
        # A newer manual round is waiting for scheduler acknowledgement.
        # Do not classify its PENDING state using the old terminal task.
        acknowledged_round=(task["result"] or {}).get("retry_round",1)
        if message.retry_round>acknowledged_round:
            db.session.rollback();continue
        receipt=db.session.get(TransportReceipt,message.id)
        attempt=db.session.get(Attempt,(message.id,message.attempts)) if message.attempts else None
        if receipt and receipt.payload_digest==content_digest(message):
            message.state="SENT";message.sent_at=receipt.delivered_at;message.last_error_code=None
            outcome="RECEIPT_RECOVERED"
        else:
            message.state="FAILED";message.last_error_code="worker_budget_exhausted"
            outcome="WORKER_BUDGET_EXHAUSTED"
        message.version+=1
        if attempt and attempt.outcome=="STARTED":
            attempt.outcome=outcome;attempt.finished_at=current
        db.session.commit()
