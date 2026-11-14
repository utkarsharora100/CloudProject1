"""Kitchen-owned durable records; existing Firestore tickets are projections."""
import uuid
from order_service.models import db, UTC_DATETIME, utc_now

class Ticket(db.Model):
    __tablename__ = "kitchen_tickets_v1"
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), primary_key=True)
    restaurant_id = db.Column(db.String(64), nullable=False, index=True)
    source_event_id = db.Column(db.String(36), unique=True, nullable=False)
    state = db.Column(db.String(24), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=0)
    managed = db.Column(db.Boolean, nullable=False, default=False)
    items = db.Column(db.JSON, nullable=False)
    estimated_prep_time_mins = db.Column(db.Integer, nullable=False)
    paid_at = db.Column(UTC_DATETIME, nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    updated_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    accepted_at = db.Column(UTC_DATETIME, nullable=True)
    preparation_started_at = db.Column(UTC_DATETIME, nullable=True)
    ready_at = db.Column(UTC_DATETIME, nullable=True)
    rejected_at = db.Column(UTC_DATETIME, nullable=True)
    cancelled_at = db.Column(UTC_DATETIME, nullable=True)
    rejection_reason = db.Column(db.String(32), nullable=True)
    projected_version = db.Column(db.Integer, nullable=False, default=0)
    projected_at = db.Column(UTC_DATETIME, nullable=True)
    projection_due_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    projection_attempts = db.Column(db.Integer, nullable=False, default=0)
    projection_error = db.Column(db.String(40), nullable=True)

class Command(db.Model):
    __tablename__ = "kitchen_commands_v1"
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = db.Column(db.String(36), db.ForeignKey("kitchen_tickets_v1.order_id"), nullable=False, index=True)
    action = db.Column(db.String(24), nullable=False)
    expected_version = db.Column(db.Integer, nullable=False)
    state = db.Column(db.String(24), nullable=False)
    reason = db.Column(db.String(32), nullable=True)
    task_id = db.Column(db.String(36), nullable=True)
    generation = db.Column(db.Integer, nullable=True)
    applied_by = db.Column(db.String(16), nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    applied_at = db.Column(UTC_DATETIME, nullable=True)
    __table_args__ = (db.UniqueConstraint("order_id", "action", name="uq_kitchen_logical_action"),)

class RequestReceipt(db.Model):
    __tablename__ = "kitchen_requests_v1"
    order_id = db.Column(db.String(36), db.ForeignKey("kitchen_tickets_v1.order_id"), primary_key=True)
    key_digest = db.Column(db.String(64), primary_key=True)
    request_hash = db.Column(db.String(64), nullable=False)
    response = db.Column(db.JSON, nullable=False)

class Event(db.Model):
    __tablename__ = "kitchen_events_v1"
    id = db.Column(db.BigInteger().with_variant(db.Integer, "sqlite"), primary_key=True, autoincrement=True)
    event_id = db.Column(db.String(36), nullable=False, unique=True, default=lambda: str(uuid.uuid4()))
    order_id = db.Column(db.String(36), db.ForeignKey("kitchen_tickets_v1.order_id"), nullable=False, index=True)
    version = db.Column(db.Integer, nullable=False)
    event_type = db.Column(db.String(40), nullable=False)
    state = db.Column(db.String(24), nullable=False)
    order_status = db.Column(db.String(32), nullable=False)
    command_id = db.Column(db.String(36), nullable=True)
    reason = db.Column(db.String(32), nullable=True)
    occurred_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    __table_args__ = (db.UniqueConstraint("order_id", "version", name="uq_kitchen_event_version"),)
