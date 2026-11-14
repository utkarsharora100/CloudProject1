"""Additive v2 tables; the original order/idempotency_key tables are untouched."""
from datetime import datetime, timezone
import uuid
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR


db = SQLAlchemy()
UTC_DATETIME = db.DateTime().with_variant(DATETIME(fsp=6), "mysql")


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def timestamp(value):
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


class Order(db.Model):
    __tablename__ = "orders_v2"
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = db.Column(db.Integer, nullable=False)
    address_id = db.Column(db.Integer, nullable=False)
    restaurant_id = db.Column(db.String(64), nullable=False)
    total_minor = db.Column(db.BigInteger, nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    status = db.Column(db.String(32), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    delivery_snapshot = db.Column(db.JSON, nullable=False)
    restaurant_snapshot = db.Column(db.JSON, nullable=False)
    items_snapshot = db.Column(db.JSON, nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    updated_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now, onupdate=utc_now)
    __table_args__ = (db.Index("ix_orders_v2_customer_created", "customer_id", "created_at", "id"),
                      db.CheckConstraint("total_minor > 0", name="ck_orders_v2_total"))

    def summary(self):
        return {"order_id": self.id, "customer_id": self.customer_id,
                "restaurant_id": self.restaurant_id, "status": self.status,
                "total_minor": self.total_minor, "currency": self.currency,
                "version": self.version, "created_at": timestamp(self.created_at),
                "updated_at": timestamp(self.updated_at)}

    def detail(self):
        return {**self.summary(), "address_id": self.address_id,
                "delivery_snapshot": self.delivery_snapshot,
                "restaurant_snapshot": self.restaurant_snapshot, "items": self.items_snapshot}


class Idempotency(db.Model):
    __tablename__ = "order_requests_v2"
    scope = db.Column(db.String(64), primary_key=True)
    key = db.Column(db.String(128).with_variant(VARCHAR(128, charset="ascii", collation="ascii_bin"), "mysql"), primary_key=True)
    request_hash = db.Column(db.String(64), nullable=False)
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), nullable=False)
    response = db.Column(db.JSON, nullable=False)
    status_code = db.Column(db.Integer, nullable=False)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)


class OrderEvent(db.Model):
    __tablename__ = "order_events_v2"
    event_id = db.Column(db.String(36), primary_key=True)
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), nullable=False)
    sequence = db.Column(db.Integer, nullable=False)
    event_type = db.Column(db.String(64), nullable=False)
    from_state = db.Column(db.String(32), nullable=True)
    to_state = db.Column(db.String(32), nullable=False)
    occurred_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    __table_args__ = (db.UniqueConstraint("order_id", "sequence", name="uq_order_events_v2_sequence"),)


class Outbox(db.Model):
    __tablename__ = "order_outbox_v2"
    event_id = db.Column(db.String(36), db.ForeignKey("order_events_v2.event_id"), primary_key=True)
    order_id = db.Column(db.String(36), db.ForeignKey("orders_v2.id"), nullable=False)
    sequence = db.Column(db.Integer, nullable=False)
    topic = db.Column(db.String(64), nullable=False, default="orders")
    payload = db.Column(db.JSON, nullable=False)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    available_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    claimed_by = db.Column(db.String(36), nullable=True)
    lease_until = db.Column(UTC_DATETIME, nullable=True)
    published_at = db.Column(UTC_DATETIME, nullable=True)
    last_error_code = db.Column(db.String(32), nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    __table_args__ = (db.UniqueConstraint("order_id", "sequence", name="uq_order_outbox_v2_sequence"),
                      db.Index("ix_order_outbox_v2_pending", "published_at", "available_at"))
