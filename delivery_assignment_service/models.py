"""Delivery-owned SQL records; physical capacity is independent of worker leases."""
import uuid
from order_service.models import db, UTC_DATETIME, utc_now

def new_id():
    return str(uuid.uuid4())

class Fleet(db.Model):
    __tablename__ = "delivery_fleet_lock_v1"
    id = db.Column(db.Integer, primary_key=True)

class Zone(db.Model):
    __tablename__ = "delivery_zones_v1"
    id = db.Column(db.String(64), primary_key=True)
    run_id = db.Column(db.String(36), unique=True, nullable=False, default=new_id)
    scenario = db.Column(db.String(32), nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)

class Courier(db.Model):
    __tablename__ = "delivery_couriers_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    zone = db.Column(db.String(64), db.ForeignKey("delivery_zones_v1.id"), nullable=False, index=True)
    name = db.Column(db.String(64), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    online = db.Column(db.Boolean, nullable=False, default=False)
    accepting_work = db.Column(db.Boolean, nullable=False, default=False)
    last_seen_at = db.Column(UTC_DATETIME, nullable=True)
    last_assigned_at = db.Column(UTC_DATETIME, nullable=True)
    last_delivery_at = db.Column(UTC_DATETIME, nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    updated_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)

class Job(db.Model):
    __tablename__ = "delivery_jobs_v1"
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), primary_key=True)
    zone = db.Column(db.String(64), db.ForeignKey("delivery_zones_v1.id"), nullable=False, index=True)
    state = db.Column(db.String(24), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    source_event_id = db.Column(db.String(36), nullable=False)
    ready_at = db.Column(UTC_DATETIME, nullable=False)
    requested_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    assigned_at = db.Column(UTC_DATETIME, nullable=True)
    picked_up_at = db.Column(UTC_DATETIME, nullable=True)
    delivered_at = db.Column(UTC_DATETIME, nullable=True)

class Assignment(db.Model):
    __tablename__ = "delivery_assignments_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    order_id = db.Column(db.String(36), db.ForeignKey("delivery_jobs_v1.order_id"), unique=True, nullable=False)
    courier_id = db.Column(db.String(36), db.ForeignKey("delivery_couriers_v1.id"), nullable=False, index=True)
    state = db.Column(db.String(24), nullable=False)
    assigned_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    picked_up_at = db.Column(UTC_DATETIME, nullable=True)
    delivered_at = db.Column(UTC_DATETIME, nullable=True)
    task_id = db.Column(db.String(36), nullable=False)
    generation = db.Column(db.Integer, nullable=False)

class ActiveSlot(db.Model):
    __tablename__ = "delivery_active_slots_v1"
    courier_id = db.Column(db.String(36), db.ForeignKey("delivery_couriers_v1.id"), primary_key=True)
    order_id = db.Column(db.String(36), db.ForeignKey("delivery_jobs_v1.order_id"), unique=True, nullable=False)
    assignment_id = db.Column(db.String(36), db.ForeignKey("delivery_assignments_v1.id"), unique=True, nullable=False)

class Command(db.Model):
    __tablename__ = "delivery_commands_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    order_id = db.Column(db.String(36), db.ForeignKey("delivery_jobs_v1.order_id"), nullable=False, index=True)
    assignment_id = db.Column(db.String(36), db.ForeignKey("delivery_assignments_v1.id"), nullable=False)
    courier_id = db.Column(db.String(36), db.ForeignKey("delivery_couriers_v1.id"), nullable=False)
    action = db.Column(db.String(16), nullable=False)
    expected_version = db.Column(db.Integer, nullable=False)
    state = db.Column(db.String(16), nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    applied_at = db.Column(UTC_DATETIME, nullable=True)
    task_id = db.Column(db.String(36), nullable=True)
    generation = db.Column(db.Integer, nullable=True)
    __table_args__ = (db.UniqueConstraint("order_id", "action", name="uq_delivery_logical_action"),)

class Receipt(db.Model):
    __tablename__ = "delivery_requests_v1"
    scope = db.Column(db.String(64), primary_key=True)
    key_digest = db.Column(db.String(64), primary_key=True)
    request_hash = db.Column(db.String(64), nullable=False)
    response = db.Column(db.JSON, nullable=False)
    status_code = db.Column(db.Integer, nullable=False)

class Event(db.Model):
    __tablename__ = "delivery_events_v1"
    id = db.Column(db.BigInteger().with_variant(db.Integer, "sqlite"), primary_key=True, autoincrement=True)
    event_id = db.Column(db.String(36), unique=True, nullable=False, default=new_id)
    zone = db.Column(db.String(64), db.ForeignKey("delivery_zones_v1.id"), nullable=False, index=True)
    order_id = db.Column(db.String(36), nullable=True, index=True)
    courier_id = db.Column(db.String(36), nullable=True)
    assignment_id = db.Column(db.String(36), nullable=True)
    event_type = db.Column(db.String(40), nullable=False)
    data = db.Column(db.JSON, nullable=False)
    occurred_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    available_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    claimed_by = db.Column(db.String(36), nullable=True)
    lease_until = db.Column(UTC_DATETIME, nullable=True)
    published_at = db.Column(UTC_DATETIME, nullable=True)
    last_error_code = db.Column(db.String(40), nullable=True)
