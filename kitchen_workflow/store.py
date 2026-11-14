"""Durable explicit kitchen actions; worker stages and Order/outbox commit together."""
from copy import deepcopy
from datetime import timedelta
from sqlalchemy import select, func
from werkzeug.exceptions import Conflict, NotFound
from order_service.models import Order, OrderEvent, db, timestamp, utc_now
from order_service.events import append_event
from order_service.domain import EVENT_TYPES, request_hash, validate_transition
from payment_service.models import Payment
from payment_service.store import PaymentStore
from task_scheduler.fencing import check_live, lock_task
from kitchen_workflow.domain import ALLOWED, LEGACY_STATUS
from kitchen_workflow.models import Ticket, Command, RequestReceipt, Event

def stamp(value):
    return timestamp(value) if value is not None else None

class KitchenStore:
    def __init__(self, coordination=None, *, clock=None):
        self.coordination = coordination
        self.clock = clock or (lambda: db.session.execute(select(func.utc_timestamp(6))).scalar_one()
                               if db.engine.dialect.name == "mysql" else utc_now())

    def order(self, value, *, lock=False):
        query = select(Order).where(Order.id == value)
        if lock:
            query = query.with_for_update()
        row = db.session.execute(query).scalar_one_or_none()
        if row is None:
            raise NotFound("Order not found.")
        return row

    def ticket(self, value):
        ticket = db.session.get(Ticket, value)
        if ticket is None:
            raise NotFound("Kitchen ticket not found; a confirmed captured payment is required.")
        return ticket

    def payment(self, order_id):
        return db.session.execute(select(Payment).where(Payment.order_id == order_id)).scalar_one_or_none()

    def event(self, ticket, order, kind, command=None, reason=None):
        ticket.version += 1
        ticket.updated_at = self.clock()
        ticket.projection_due_at = ticket.updated_at
        db.session.add(Event(order_id=ticket.order_id, version=ticket.version, event_type=kind,
            state=ticket.state, order_status=order.status, command_id=command.id if command else None,
            reason=reason, occurred_at=ticket.updated_at))

    def seed(self, order):
        previous = db.session.get(Ticket, order.id)
        if previous is not None:
            return previous
        payment = self.payment(order.id)
        if order.status != "PAID" or payment is None or payment.state != "CAPTURED":
            raise Conflict("Kitchen work requires a confirmed captured payment.")
        paid = db.session.execute(select(OrderEvent).where(OrderEvent.order_id == order.id,
            OrderEvent.to_state == "PAID").order_by(OrderEvent.sequence)).scalars().first()
        if paid is None:
            raise Conflict("Confirmed payment history is unavailable.")
        now = self.clock()
        ticket = Ticket(order_id=order.id, restaurant_id=order.restaurant_id, source_event_id=paid.event_id,
            state="WAITING", version=0, managed=False,
            items=[{key: item[key] for key in ("item_id", "name", "quantity")} for item in order.items_snapshot],
            estimated_prep_time_mins=order.restaurant_snapshot["estimated_prep_time_mins"],
            paid_at=paid.occurred_at, created_at=now, updated_at=now, projected_version=0,
            projection_due_at=now, projection_attempts=0)
        db.session.add(ticket)
        db.session.flush()
        self.event(ticket, order, "KitchenQueued")
        return ticket

    def materialize(self):
        db.session.rollback()
        ids = db.session.execute(select(Order.id).join(Payment, Payment.order_id == Order.id)
            .outerjoin(Ticket, Ticket.order_id == Order.id)
            .where(Order.status == "PAID", Payment.state == "CAPTURED", Ticket.order_id.is_(None))
            .order_by(Order.created_at, Order.id).limit(20)).scalars().all()
        db.session.rollback()
        for value in ids:
            order = self.order(value, lock=True)
            if order.status == "PAID":
                payment = self.payment(value)
                if payment and payment.state == "CAPTURED":
                    self.seed(order)
            db.session.commit()

    def synchronize(self):
        db.session.rollback()
        ids = db.session.execute(select(Ticket.order_id).where(Ticket.state.in_(("WAITING", "ACCEPTED", "PREPARING")))).scalars().all()
        db.session.rollback()
        for value in ids:
            order = self.order(value, lock=True)
            ticket = self.ticket(value)
            if order.status in {"CANCELLED", "REFUND_PENDING", "REFUNDED"} and ticket.state not in {"REJECTED", "CANCELLED"}:
                ticket.state, ticket.cancelled_at = "CANCELLED", self.clock()
                pending = db.session.execute(select(Command).where(Command.order_id == value, Command.state == "PENDING")).scalars().all()
                for command in pending:
                    command.state, command.applied_at = "CANCELLED", self.clock()
                self.event(ticket, order, "KitchenCancelled", reason="order_cancelled")
            elif not ticket.managed and order.status in {"KITCHEN_ACCEPTED", "PREPARING", "READY", "COURIER_ASSIGNED", "PICKED_UP", "DELIVERED"}:
                # Compatibility actions are observed, never fabricated as worker attempts.
                stages = [("KITCHEN_ACCEPTED", "ACCEPTED", "accepted_at"),
                          ("PREPARING", "PREPARING", "preparation_started_at"), ("READY", "READY", "ready_at")]
                for target, state, field in stages:
                    proof = db.session.execute(select(OrderEvent).where(OrderEvent.order_id == value, OrderEvent.to_state == target)).scalars().first()
                    if proof and getattr(ticket, field) is None:
                        setattr(ticket, field, proof.occurred_at)
                        ticket.state = state
                        self.event(ticket, order, "KitchenStageObserved", reason="order_history")
            db.session.commit()

    def summary(self, ticket, order):
        return {"order_id": order.id, "restaurant_id": ticket.restaurant_id, "state": ticket.state,
                "version": ticket.version, "managed": ticket.managed, "order_status": order.status,
                "order_version": order.version, "estimated_prep_time_mins": ticket.estimated_prep_time_mins,
                "created_at": stamp(ticket.created_at), "updated_at": stamp(ticket.updated_at),
                "paid_at": stamp(ticket.paid_at), "accepted_at": stamp(ticket.accepted_at),
                "preparation_started_at": stamp(ticket.preparation_started_at), "ready_at": stamp(ticket.ready_at),
                "rejected_at": stamp(ticket.rejected_at), "cancelled_at": stamp(ticket.cancelled_at),
                "rejection_reason": ticket.rejection_reason,
                "projection": {"version": ticket.projected_version, "current": ticket.projected_version == ticket.version,
                    "last_confirmed_at": stamp(ticket.projected_at), "last_error_code": ticket.projection_error}}

    def command_data(self, command):
        return {"command_id": command.id, "action": command.action, "state": command.state,
                "reason": command.reason, "task_id": command.task_id, "generation": command.generation,
                "applied_by": command.applied_by, "created_at": stamp(command.created_at), "applied_at": stamp(command.applied_at)}

    def detail(self, value):
        order = self.order(value)
        ticket = self.ticket(value)
        commands = db.session.execute(select(Command).where(Command.order_id == value).order_by(Command.expected_version, Command.created_at, Command.id)).scalars().all()
        return {"ticket": {**self.summary(ticket, order), "items": deepcopy(ticket.items),
                           "commands": [self.command_data(c) for c in commands]}}

    def action(self, value, data, key_digest):
        db.session.rollback()
        order = self.order(value, lock=True)
        saved = db.session.execute(select(RequestReceipt).where(RequestReceipt.order_id == value,
            RequestReceipt.key_digest == key_digest).with_for_update()).scalar_one_or_none()
        digest = request_hash(data)
        if saved is not None:
            if saved.request_hash != digest:
                raise Conflict("This kitchen request key was used with a different action.")
            response = deepcopy(saved.response)
            db.session.rollback()
            return response, True
        if order.version != data["expected_version"]:
            raise Conflict("Order version changed; reload before retrying.")
        ticket = db.session.get(Ticket, value) or self.seed(order)
        action = data["action"]
        if action == "reject":
            allowed = ticket.state in {"WAITING", "ACCEPTED"} and order.status in {"PAID", "KITCHEN_ACCEPTED"}
        else:
            allowed = (ticket.state, order.status) == ALLOWED[action]
        if not allowed:
            raise Conflict("This kitchen action is not allowed at the current order stage.")
        pending = db.session.execute(select(Command.id).where(Command.order_id == value, Command.state == "PENDING")).first()
        prior = db.session.execute(select(Command.id).where(Command.order_id == value, Command.action == action)).first()
        if pending or prior:
            raise Conflict("A kitchen action is pending or already recorded; replay its original key.")
        command = Command(order_id=value, action=action, expected_version=order.version, state="PENDING",
                          reason=data.get("reason"), created_at=self.clock())
        db.session.add(command)
        db.session.flush()
        ticket.managed = True
        if action == "reject":
            # Payment owns this rule. One local SQL transaction records rejection,
            # refund request, Order history/outbox and request receipt.
            PaymentStore(self.coordination, None, clock=self.clock).request_refund_locked(order, self.payment(value))
            ticket.state, ticket.rejected_at, ticket.rejection_reason = "REJECTED", self.clock(), data["reason"]
            command.state, command.applied_at, command.applied_by = "APPLIED", self.clock(), "human"
            self.event(ticket, order, "KitchenRejected", command, data["reason"])
        else:
            self.event(ticket, order, "KitchenActionRequested", command)
        response = {"ticket": self.summary(ticket, order), "command": self.command_data(command)}
        db.session.add(RequestReceipt(order_id=value, key_digest=key_digest, request_hash=digest, response=response))
        db.session.commit()
        return response, False

    def move(self, order, target):
        validate_transition(order.status, target)
        old = order.status
        order.status, order.version, order.updated_at = target, order.version+1, self.clock()
        append_event(order, EVENT_TYPES[target], old)

    def process(self, task_id, proof):
        db.session.rollback()
        task, value = lock_task(db.session, self.coordination, task_id, proof, self.clock,
                               kinds={"kitchen_accept", "preparation"})
        order = self.order(value, lock=True)
        ticket = self.ticket(value)
        if order.status in {"CANCELLED", "REFUND_PENDING", "REFUNDED"} or ticket.state in {"REJECTED", "CANCELLED"}:
            raise Conflict("Order no longer permits kitchen work.")
        kinds = ["accept"] if task["kind"] == "kitchen_accept" else ["start_preparation", "ready"]
        command = db.session.execute(select(Command).where(Command.order_id == value, Command.state == "PENDING",
            Command.action.in_(kinds)).order_by(Command.created_at, Command.id)).scalars().first()
        check_live(task, self.coordination, proof, self.clock())
        if command is None:
            succeeded = (task["kind"] == "kitchen_accept" and ticket.accepted_at is not None) or (
                task["kind"] == "preparation" and ticket.ready_at is not None)
            db.session.rollback()
            return {"task_outcome": "succeeded" if succeeded else "deferred"}
        if order.version != command.expected_version or (ticket.state, order.status) != ALLOWED[command.action]:
            raise Conflict("Kitchen command is no longer valid for this order.")
        target, state, field, event = {
            "accept": ("KITCHEN_ACCEPTED", "ACCEPTED", "accepted_at", "KitchenAccepted"),
            "start_preparation": ("PREPARING", "PREPARING", "preparation_started_at", "KitchenPreparationStarted"),
            "ready": ("READY", "READY", "ready_at", "KitchenReady")}[command.action]
        self.move(order, target)
        ticket.state = state
        setattr(ticket, field, self.clock())
        command.state, command.applied_at, command.applied_by = "APPLIED", self.clock(), "worker"
        command.task_id, command.generation = task_id, proof["generation"]
        self.event(ticket, order, event, command)
        check_live(task, self.coordination, proof, self.clock())
        outcome = "deferred" if command.action == "start_preparation" else "succeeded"
        db.session.commit()
        return {"task_outcome": outcome}

    def projection_payload(self, ticket):
        return {"order_id": ticket.order_id, "restaurant_id": ticket.restaurant_id, "source_event_id": ticket.source_event_id,
                "status": LEGACY_STATUS[ticket.state], "workflow_state": ticket.state, "kitchen_version": ticket.version,
                "managed": ticket.managed, "items": deepcopy(ticket.items),
                "estimated_prep_time_mins": ticket.estimated_prep_time_mins, "received_at": stamp(ticket.created_at),
                **{field: stamp(getattr(ticket, field)) for field in
                   ("paid_at", "accepted_at", "preparation_started_at", "ready_at", "rejected_at", "cancelled_at")},
                "rejection_reason": ticket.rejection_reason}

    def project_one(self, repository):
        db.session.rollback()
        ticket = db.session.execute(select(Ticket).where(Ticket.projection_due_at <= self.clock())
            .order_by(Ticket.projection_due_at, Ticket.order_id).limit(1)).scalar_one_or_none()
        if ticket is None:
            db.session.rollback()
            return False
        value, version = ticket.order_id, ticket.version
        payload = self.projection_payload(ticket)
        db.session.rollback()
        try:
            repository.put(payload)
            error = None
        except Exception:
            error = "firestore_projection_unavailable"
        # Never hold SQL locks across a remote Firestore transaction.
        order = self.order(value, lock=True)
        ticket = self.ticket(value)
        ticket.projection_attempts += 1
        ticket.projection_error = error
        if ticket.version == version:
            if error is None:
                ticket.projected_version, ticket.projected_at = version, self.clock()
            ticket.projection_due_at = self.clock()+timedelta(seconds=30 if error is None else min(2**min(ticket.projection_attempts,5),30))
        db.session.commit()
        return True
