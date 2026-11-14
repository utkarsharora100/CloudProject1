"""Customer-owned records, independent of the Order Service's tables."""
from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


def utc_now():
    # MySQL DATETIME values are stored as naive UTC, serialized explicitly as UTC.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def timestamp(value):
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


class Customer(db.Model):
    __tablename__ = "customers"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(254), nullable=False, unique=True)
    phone = db.Column(db.String(32), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)
    updated_at = db.Column(db.DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    def as_dict(self):
        return {"id": self.id, "name": self.name, "email": self.email,
                "phone": self.phone, "created_at": timestamp(self.created_at),
                "updated_at": timestamp(self.updated_at)}


class Address(db.Model):
    __tablename__ = "customer_addresses"

    id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.Integer, db.ForeignKey("customers.id"), nullable=False, index=True)
    label = db.Column(db.String(40), nullable=False)
    line1 = db.Column(db.String(200), nullable=False)
    line2 = db.Column(db.String(200), nullable=True)
    city = db.Column(db.String(80), nullable=False)
    region = db.Column(db.String(80), nullable=True)
    postal_code = db.Column(db.String(20), nullable=False)
    country_code = db.Column(db.String(2), nullable=False)
    is_default = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)
    updated_at = db.Column(db.DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    def as_dict(self):
        return {"id": self.id, "customer_id": self.customer_id,
                "label": self.label, "line1": self.line1, "line2": self.line2,
                "city": self.city, "region": self.region, "postal_code": self.postal_code,
                "country_code": self.country_code, "is_default": self.is_default,
                "created_at": timestamp(self.created_at), "updated_at": timestamp(self.updated_at)}
