"""Notification-owned ledgers; no contact details, credentials or business writes."""
from order_service.models import db, UTC_DATETIME, utc_now

class Subscription(db.Model):
    __tablename__ = "notification_subscriptions_v1"
    order_id = db.Column(db.String(36), primary_key=True)
    customer_id = db.Column(db.Integer, nullable=False, index=True)
    scenario = db.Column(db.String(24), nullable=False)
    courier_updates = db.Column(db.Boolean, nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)

class Message(db.Model):
    __tablename__ = "notifications_v1"
    id = db.Column(db.String(36), primary_key=True)
    order_id = db.Column(db.String(36), nullable=False, index=True)
    source_topic = db.Column(db.String(32), nullable=False)
    source_event_id = db.Column(db.String(36), nullable=False)
    sequence = db.Column(db.Integer, nullable=False)
    source_at = db.Column(UTC_DATETIME, nullable=False)
    recipient_type = db.Column(db.String(12), nullable=False)
    recipient_id = db.Column(db.String(36), nullable=False, index=True)
    stage = db.Column(db.String(32), nullable=False)
    channel = db.Column(db.String(16), nullable=False, default="in_app")
    state = db.Column(db.String(16), nullable=False, default="PENDING")
    scenario = db.Column(db.String(24), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    round_attempts = db.Column(db.Integer, nullable=False, default=0)
    retry_round = db.Column(db.Integer, nullable=False, default=1)
    available_at = db.Column(UTC_DATETIME, nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False)
    sent_at = db.Column(UTC_DATETIME, nullable=True)
    read_at = db.Column(UTC_DATETIME, nullable=True)
    last_error_code = db.Column(db.String(32), nullable=True)
    task_id = db.Column(db.String(36), unique=True, nullable=True)
    workflow_id = db.Column(db.String(36), nullable=True)
    __table_args__ = (db.UniqueConstraint("source_topic", "source_event_id", "recipient_type",
        "recipient_id", "channel", name="uq_notification_logical_recipient"),)

class Attempt(db.Model):
    __tablename__ = "notification_attempts_v1"
    notification_id = db.Column(db.String(36), primary_key=True)
    number = db.Column(db.Integer, primary_key=True)
    retry_round = db.Column(db.Integer, nullable=False)
    task_id = db.Column(db.String(36), nullable=False)
    generation = db.Column(db.Integer, nullable=False)
    outcome = db.Column(db.String(24), nullable=False)
    started_at = db.Column(UTC_DATETIME, nullable=False)
    finished_at = db.Column(UTC_DATETIME, nullable=True)

class TransportReceipt(db.Model):
    # No FK: the provider commits independently while the application row is locked.
    __tablename__ = "notification_transport_v1"
    notification_id = db.Column(db.String(36), primary_key=True)
    payload_digest = db.Column(db.String(64), nullable=False)
    delivered_at = db.Column(UTC_DATETIME, nullable=False)

class EventInbox(db.Model):
    __tablename__ = "notification_inbox_v1"
    topic = db.Column(db.String(32), primary_key=True)
    event_id = db.Column(db.String(36), primary_key=True)
    processed_at = db.Column(UTC_DATETIME, nullable=False)

class StreamReceipt(db.Model):
    __tablename__ = "notification_stream_v1"
    topic = db.Column(db.String(32), primary_key=True)
    partition = db.Column(db.Integer, primary_key=True)
    offset = db.Column(db.BigInteger, primary_key=True)
    payload_digest = db.Column(db.String(64), nullable=False)
    outcome = db.Column(db.String(24), nullable=False)
    reason = db.Column(db.String(32), nullable=True)
    received_at = db.Column(UTC_DATETIME, nullable=False)

class RequestReceipt(db.Model):
    __tablename__ = "notification_requests_v1"
    scope = db.Column(db.String(100), primary_key=True)
    key_digest = db.Column(db.String(64), primary_key=True)
    request_hash = db.Column(db.String(64), nullable=False)
    response = db.Column(db.JSON, nullable=False)

MODELS = (Subscription, Message, Attempt, TransportReceipt, EventInbox, StreamReceipt, RequestReceipt)
