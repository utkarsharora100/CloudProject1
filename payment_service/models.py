"""Payment-owned tables; simulated provider receipts commit independently."""
import uuid
from order_service.models import db, UTC_DATETIME, utc_now

def new_id():
    return str(uuid.uuid4())

class Payment(db.Model):
    __tablename__ = "payment_intents_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), unique=True, nullable=False)
    amount_minor = db.Column(db.BigInteger, nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    scenario = db.Column(db.String(32), nullable=False)
    refund_scenario = db.Column(db.String(32), nullable=False)
    state = db.Column(db.String(32), nullable=False)
    attempt_number = db.Column(db.Integer, nullable=False, default=0)
    version = db.Column(db.Integer, nullable=False, default=1)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    updated_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)

class PaymentAttempt(db.Model):
    __tablename__ = "payment_attempts_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    payment_id = db.Column(db.String(36), db.ForeignKey("payment_intents_v1.id"), nullable=False)
    number = db.Column(db.Integer, nullable=False)
    state = db.Column(db.String(32), nullable=False)
    task_id = db.Column(db.String(36), nullable=True)
    generation = db.Column(db.Integer, nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    completed_at = db.Column(UTC_DATETIME, nullable=True)
    __table_args__ = (db.UniqueConstraint("payment_id", "number", name="uq_payment_attempt_number"),)

class Refund(db.Model):
    __tablename__ = "payment_refunds_v1"
    id = db.Column(db.String(36), primary_key=True, default=new_id)
    payment_id = db.Column(db.String(36), db.ForeignKey("payment_intents_v1.id"), unique=True, nullable=False)
    amount_minor = db.Column(db.BigInteger, nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    state = db.Column(db.String(32), nullable=False)
    task_id = db.Column(db.String(36), nullable=True)
    generation = db.Column(db.Integer, nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    completed_at = db.Column(UTC_DATETIME, nullable=True)

class ProviderOperation(db.Model):
    __tablename__ = "payment_provider_operations_v1"
    # No FK locks on app rows: receipts survive an uncertain application response.
    id = db.Column(db.String(36), primary_key=True)
    payment_id = db.Column(db.String(36), nullable=False, index=True)
    kind = db.Column(db.String(16), nullable=False)
    amount_minor = db.Column(db.BigInteger, nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    outcome = db.Column(db.String(16), nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)

class PaymentRequest(db.Model):
    __tablename__ = "payment_requests_v1"
    scope = db.Column(db.String(64), primary_key=True)
    key_digest = db.Column(db.String(64), primary_key=True)
    request_hash = db.Column(db.String(64), nullable=False)
    response = db.Column(db.JSON, nullable=False)
    status_code = db.Column(db.Integer, nullable=False)

class PaymentEvent(db.Model):
    __tablename__ = "payment_events_v1"
    id = db.Column(db.BigInteger().with_variant(db.Integer, "sqlite"), primary_key=True, autoincrement=True)
    payment_id = db.Column(db.String(36), db.ForeignKey("payment_intents_v1.id"), nullable=False, index=True)
    event_type = db.Column(db.String(40), nullable=False)
    attempt_number = db.Column(db.Integer, nullable=False)
    occurred_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
