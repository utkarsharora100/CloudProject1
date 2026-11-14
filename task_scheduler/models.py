"""Scheduler-owned tables; Order Service records are read, never advanced here."""
import uuid
from flask_sqlalchemy import SQLAlchemy
from order_service.models import Order, UTC_DATETIME, utc_now


db = SQLAlchemy()


class Workflow(db.Model):
    __tablename__ = "scheduler_workflows"
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    request_digest = db.Column(db.String(64), unique=True, nullable=False)
    request_hash = db.Column(db.String(64), nullable=False)
    order_id = db.Column(db.String(36), db.ForeignKey(Order.__table__.c.id), nullable=True, index=True)
    managed_order_id = db.Column(db.String(36), unique=True, nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)


class Task(db.Model):
    __tablename__ = "scheduler_tasks"
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_id = db.Column(db.String(36), db.ForeignKey("scheduler_workflows.id"), nullable=False, index=True)
    key = db.Column(db.String(64), nullable=False)
    label = db.Column(db.String(100), nullable=False)
    kind = db.Column(db.String(32), nullable=False)
    condition = db.Column(db.String(32), nullable=False, default="always")
    params = db.Column(db.JSON, nullable=False)
    state = db.Column(db.String(24), nullable=False, default="BLOCKED")
    reason = db.Column(db.String(40), nullable=True)
    generation = db.Column(db.Integer, nullable=False, default=0)
    revision = db.Column(db.Integer, nullable=False, default=1)
    assignment_epoch = db.Column(db.BigInteger, nullable=True)
    owner_id = db.Column(db.String(36), nullable=True, index=True)
    owner_name = db.Column(db.String(64), nullable=True)
    owner_session = db.Column(db.String(32), nullable=True)
    last_worker_id = db.Column(db.String(36), nullable=True)
    lease_until = db.Column(UTC_DATETIME, nullable=True)
    assignment_until = db.Column(UTC_DATETIME, nullable=True)
    available_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    failures = db.Column(db.Integer, nullable=False, default=0)
    max_failures = db.Column(db.Integer, nullable=False, default=3)
    completion_source = db.Column(db.String(32), nullable=True)
    proof_event_id = db.Column(db.String(36), nullable=True)
    confirmed_at = db.Column(UTC_DATETIME, nullable=True)
    result = db.Column(db.JSON, nullable=True)
    created_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    updated_at = db.Column(UTC_DATETIME, nullable=False, default=utc_now)
    ready_at = db.Column(UTC_DATETIME, nullable=True)
    started_at = db.Column(UTC_DATETIME, nullable=True)
    completed_at = db.Column(UTC_DATETIME, nullable=True)
    __table_args__ = (db.UniqueConstraint("workflow_id", "key", name="uq_scheduler_task_key"),
                      db.Index("ix_scheduler_task_due", "state", "available_at"))


class Dependency(db.Model):
    __tablename__ = "scheduler_dependencies"
    task_id = db.Column(db.String(36), db.ForeignKey("scheduler_tasks.id"), primary_key=True)
    prerequisite_id = db.Column(db.String(36), db.ForeignKey("scheduler_tasks.id"), primary_key=True)


class Attempt(db.Model):
    __tablename__ = "scheduler_attempts"
    task_id = db.Column(db.String(36), db.ForeignKey("scheduler_tasks.id"), primary_key=True)
    generation = db.Column(db.Integer, primary_key=True)
    lease_token = db.Column(db.String(36), nullable=False)
    worker_id = db.Column(db.String(36), nullable=False)
    worker_name = db.Column(db.String(64), nullable=False)
    worker_session = db.Column(db.String(32), nullable=False)
    outcome = db.Column(db.String(40), nullable=False, default="RUNNING")
    started_at = db.Column(UTC_DATETIME, nullable=False)
    finished_at = db.Column(UTC_DATETIME, nullable=True)
    response = db.Column(db.JSON, nullable=True)


class TaskEvent(db.Model):
    __tablename__ = "scheduler_events"
    id = db.Column(db.BigInteger().with_variant(db.Integer, "sqlite"), primary_key=True, autoincrement=True)
    workflow_id = db.Column(db.String(36), db.ForeignKey("scheduler_workflows.id"), nullable=False)
    task_id = db.Column(db.String(36), db.ForeignKey("scheduler_tasks.id"), nullable=False, index=True)
    from_state = db.Column(db.String(24), nullable=True)
    to_state = db.Column(db.String(24), nullable=False)
    reason = db.Column(db.String(40), nullable=True)
    generation = db.Column(db.Integer, nullable=False)
    occurred_at = db.Column(UTC_DATETIME, nullable=False)


class LeaderState(db.Model):
    __tablename__ = "scheduler_leader_epoch"
    id = db.Column(db.Integer, primary_key=True)
    epoch = db.Column(db.BigInteger, nullable=False, default=0)
    leader_id = db.Column(db.String(36), nullable=True)
    leader_name = db.Column(db.String(64), nullable=True)
    heartbeat_at = db.Column(UTC_DATETIME, nullable=True)
    expires_at = db.Column(UTC_DATETIME, nullable=True)
