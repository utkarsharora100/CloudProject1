"""Shared atomic Order event and outbox writer."""
import uuid
from order_service.models import OrderEvent, Outbox, db, timestamp, utc_now


def append_event(order, event_type, previous):
    event_id = str(uuid.uuid4())
    occurred = utc_now()
    event = OrderEvent(event_id=event_id, order_id=order.id, sequence=order.version,
                       event_type=event_type, from_state=previous, to_state=order.status, occurred_at=occurred)
    db.session.add(event)
    db.session.flush()
    payload = {"schema_version": 2, "event_id": event_id, "event_type": event_type,
               "order_id": order.id, "customer_id": order.customer_id,
               "restaurant_id": order.restaurant_id, "status": order.status,
               "sequence": order.version, "timestamp": timestamp(occurred),
               "total_minor": order.total_minor, "currency": order.currency,
               "estimated_prep_time_mins": order.restaurant_snapshot["estimated_prep_time_mins"],
               "items": [{key: item[key] for key in ("item_id", "name", "quantity")} for item in order.items_snapshot]}
    db.session.add(Outbox(event_id=event_id, order_id=order.id, sequence=order.version,
                         topic="orders", payload=payload, available_at=occurred))
