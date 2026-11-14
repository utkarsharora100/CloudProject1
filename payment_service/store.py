"""Durable simulated payments with atomic Order/outbox writes and safe recovery."""
from copy import deepcopy
from sqlalchemy import select, func
from werkzeug.exceptions import Conflict, NotFound, ServiceUnavailable
from order_service.domain import EVENT_TYPES, request_hash, validate_transition
from order_service.events import append_event
from order_service.models import Order, db, timestamp, utc_now
from payment_service.fencing import check_live, lock_task
from payment_service.models import Payment, PaymentAttempt, PaymentEvent, PaymentRequest, Refund

class PaymentStore:
    def __init__(self, coordination, provider, *, clock=None):
        self.coordination, self.provider = coordination, provider
        self.clock = clock or (lambda: db.session.execute(select(func.utc_timestamp(6))).scalar_one()
                               if db.engine.dialect.name == "mysql" else utc_now())

    def order(self, order_id, *, lock=False):
        query = select(Order).where(Order.id == order_id)
        if lock:
            query = query.with_for_update()
        order = db.session.execute(query).scalar_one_or_none()
        if order is None:
            raise NotFound("Order not found.")
        return order

    def payment(self, order_id):
        return db.session.execute(select(Payment).where(Payment.order_id == order_id)).scalar_one_or_none()

    def attempt(self, payment):
        return db.session.execute(select(PaymentAttempt).where(
            PaymentAttempt.payment_id == payment.id, PaymentAttempt.number == payment.attempt_number)).scalar_one_or_none()

    def refund(self, payment):
        return db.session.execute(select(Refund).where(Refund.payment_id == payment.id)).scalar_one_or_none()

    def event(self, payment, kind):
        payment.version += 1
        payment.updated_at = self.clock()
        db.session.add(PaymentEvent(payment_id=payment.id, event_type=kind,
                                   attempt_number=payment.attempt_number, occurred_at=payment.updated_at))

    def move(self, order, target):
        validate_transition(order.status, target)
        old = order.status
        order.status, order.version, order.updated_at = target, order.version + 1, self.clock()
        append_event(order, EVENT_TYPES[target], old)

    def summary(self, order, payment):
        return {"order_id": order.id, "order_status": order.status, "order_version": order.version,
                "payment_id": payment.id, "state": payment.state, "version": payment.version,
                "amount_minor": payment.amount_minor, "currency": payment.currency,
                "attempt_number": payment.attempt_number, "simulation": True,
                "created_at": timestamp(payment.created_at), "updated_at": timestamp(payment.updated_at)}

    def action(self, order_id, action, data, key_digest):
        db.session.rollback()
        # Shared lock order: scheduler Task (processing only) -> Order -> payment.
        order = self.order(order_id, lock=True)
        scope = action + ":" + order_id
        digest = request_hash(data)
        saved = db.session.execute(select(PaymentRequest).where(
            PaymentRequest.scope == scope, PaymentRequest.key_digest == key_digest).with_for_update()).scalar_one_or_none()
        if saved is not None:
            if saved.request_hash != digest:
                raise Conflict("This payment request key was used with a different request.")
            body, code = deepcopy(saved.response), saved.status_code
            db.session.rollback()
            return body, code, True
        if order.version != data["expected_version"]:
            raise Conflict("Order version changed; reload before retrying.")
        payment = self.payment(order_id)
        if action == "create":
            if order.status != "PENDING_PAYMENT" or payment is not None:
                raise Conflict("Payment already exists or the order cannot accept payment.")
            payment = Payment(order_id=order.id, amount_minor=order.total_minor, currency=order.currency,
                scenario=data["scenario"], refund_scenario=data["refund_scenario"], state="PENDING",
                attempt_number=0, version=0, created_at=self.clock(), updated_at=self.clock())
            db.session.add(payment)
            db.session.flush()
            self.new_attempt(payment)
        elif action == "retry":
            if payment is None or payment.state != "DECLINED" or order.status != "PAYMENT_FAILED":
                raise Conflict("Only a confirmed declined payment can start another payment attempt.")
            if payment.attempt_number >= 10:
                raise Conflict("The local demo payment retry limit has been reached.")
            self.move(order, "PENDING_PAYMENT")
            self.new_attempt(payment)
        elif action == "cancel":
            if order.status not in {"PENDING_PAYMENT", "PAYMENT_FAILED", "PAID", "KITCHEN_ACCEPTED"}:
                raise Conflict("This order stage does not permit cancellation.")
            if payment is None:
                if order.status not in {"PENDING_PAYMENT", "PAYMENT_FAILED"}:
                    raise Conflict("A confirmed payment ledger is required to refund this order.")
                payment = Payment(order_id=order.id, amount_minor=order.total_minor, currency=order.currency,
                    scenario="success", refund_scenario="success", state="PENDING", attempt_number=0,
                    version=0, created_at=self.clock(), updated_at=self.clock())
                db.session.add(payment)
                db.session.flush()
            attempt = self.attempt(payment)
            # An independently committed capture can outlive a lost API response.
            if payment.state == "PENDING" and attempt is not None:
                receipt = self.provider.lookup(attempt.id)
                if receipt is not None:
                    self.capture_result(order, payment, attempt, receipt)
            if payment.state == "CAPTURED":
                self.request_refund_locked(order, payment)
            elif payment.state in {"PENDING", "DECLINED"} and order.status in {"PENDING_PAYMENT", "PAYMENT_FAILED"}:
                self.move(order, "CANCELLED")
                payment.state = "CANCELLED"
                if attempt is not None and attempt.state == "PENDING":
                    attempt.state, attempt.completed_at = "CANCELLED", self.clock()
                self.event(payment, "UnpaidOrderCancelled")
            else:
                raise Conflict("Payment cannot be cancelled in its current state.")
        else:
            raise ValueError("Unsupported internal payment action.")
        body = self.summary(order, payment)
        db.session.add(PaymentRequest(scope=scope, key_digest=key_digest, request_hash=digest, response=body, status_code=202))
        db.session.commit()
        return body, 202, False

    def request_refund_locked(self, order, payment):
        """Service-owned full-refund rule; caller holds Order lock and commits."""
        if payment is None or payment.state != "CAPTURED" or order.status not in {"PAID", "KITCHEN_ACCEPTED"}:
            raise Conflict("A captured payment and eligible order stage are required for refund.")
        self.move(order, "REFUND_PENDING")
        payment.state = "REFUND_PENDING"
        db.session.add(Refund(payment_id=payment.id, amount_minor=payment.amount_minor,
            currency=payment.currency, state="PENDING", created_at=self.clock()))
        self.event(payment, "RefundRequested")

    def new_attempt(self, payment):
        payment.attempt_number += 1
        payment.state = "PENDING"
        db.session.add(PaymentAttempt(payment_id=payment.id, number=payment.attempt_number,
                                     state="PENDING", created_at=self.clock()))
        self.event(payment, "PaymentRequested" if payment.attempt_number == 1 else "PaymentRetryRequested")

    def capture_result(self, order, payment, attempt, receipt):
        if (receipt["payment_id"] != payment.id or receipt["kind"] != "CAPTURE"
                or receipt["amount_minor"] != payment.amount_minor or receipt["currency"] != payment.currency):
            raise Conflict("Simulated payment receipt does not match the saved order total.")
        succeeded = receipt["outcome"] == "SUCCEEDED"
        if receipt["outcome"] not in {"SUCCEEDED", "DECLINED"} or order.status != "PENDING_PAYMENT":
            raise Conflict("Payment result requires order reconciliation.")
        self.move(order, "PAID" if succeeded else "PAYMENT_FAILED")
        payment.state = "CAPTURED" if succeeded else "DECLINED"
        attempt.state, attempt.completed_at = "SUCCEEDED" if succeeded else "DECLINED", self.clock()
        self.event(payment, "PaymentCaptured" if succeeded else "PaymentDeclined")

    def process(self, task_id, proof):
        db.session.rollback()
        task, order_id = lock_task(db.session, self.coordination, task_id, proof, self.clock)
        order = self.order(order_id, lock=True)
        payment = self.payment(order_id)
        if payment is None:
            raise Conflict("An explicit payment request is required.")
        if task["kind"] == "payment":
            if payment.state in {"CAPTURED", "REFUND_PENDING", "REFUNDED"}:
                check_live(task, self.coordination, proof, self.clock())
                db.session.rollback()
                return {"task_outcome": "succeeded", "simulation": True}
            if payment.state == "DECLINED":
                check_live(task, self.coordination, proof, self.clock())
                db.session.rollback()
                return {"task_outcome": "deferred", "simulation": True}
            if payment.state != "PENDING" or order.status != "PENDING_PAYMENT":
                raise Conflict("Order no longer permits this payment.")
            attempt = self.attempt(payment)
            decline = payment.scenario == "decline" or (payment.scenario == "decline_then_success" and attempt.number == 1)
            check_live(task, self.coordination, proof, self.clock())
            receipt, new = self.provider.apply(operation_id=attempt.id, payment_id=payment.id, kind="CAPTURE",
                amount_minor=payment.amount_minor, currency=payment.currency, decline=decline)
            if new and payment.scenario == "timeout_once":
                raise ServiceUnavailable("Simulated provider response was interrupted; retry the same logical task.")
            check_live(task, self.coordination, proof, self.clock())
            attempt.task_id, attempt.generation = task_id, proof["generation"]
            self.capture_result(order, payment, attempt, receipt)
            outcome = "succeeded" if payment.state == "CAPTURED" else "deferred"
        else:
            refund = self.refund(payment)
            if refund is None or payment.state not in {"REFUND_PENDING", "REFUNDED"}:
                raise Conflict("A paid cancellation and refund request are required.")
            if refund.state == "SUCCEEDED":
                check_live(task, self.coordination, proof, self.clock())
                db.session.rollback()
                return {"task_outcome": "succeeded", "simulation": True}
            if order.status != "REFUND_PENDING":
                raise Conflict("Order no longer permits this refund.")
            check_live(task, self.coordination, proof, self.clock())
            receipt, new = self.provider.apply(operation_id=refund.id, payment_id=payment.id, kind="REFUND",
                amount_minor=refund.amount_minor, currency=refund.currency)
            if new and payment.refund_scenario == "timeout_once":
                raise ServiceUnavailable("Simulated refund response was interrupted; retry the same logical task.")
            if (receipt["payment_id"] != payment.id or receipt["kind"] != "REFUND"
                    or receipt["amount_minor"] != refund.amount_minor or receipt["currency"] != refund.currency
                    or receipt["outcome"] != "SUCCEEDED"):
                raise Conflict("Simulated refund receipt does not match the captured amount.")
            check_live(task, self.coordination, proof, self.clock())
            self.move(order, "REFUNDED")
            refund.state, refund.completed_at = "SUCCEEDED", self.clock()
            refund.task_id, refund.generation = task_id, proof["generation"]
            payment.state = "REFUNDED"
            self.event(payment, "PaymentRefunded")
            outcome = "succeeded"
        check_live(task, self.coordination, proof, self.clock())
        db.session.commit()
        return {"task_outcome": outcome, "simulation": True}

    def detail(self, order_id):
        order = self.order(order_id)
        payment = self.payment(order_id)
        if payment is None:
            raise NotFound("Payment not found.")
        attempts = db.session.execute(select(PaymentAttempt).where(PaymentAttempt.payment_id == payment.id)
                                      .order_by(PaymentAttempt.number)).scalars().all()
        refund = self.refund(payment)
        return {"payment": {**self.summary(order, payment), "scenario": payment.scenario,
            "refund_scenario": payment.refund_scenario,
            "attempts": [{"attempt_id": a.id, "number": a.number, "state": a.state,
                "task_id": a.task_id, "generation": a.generation, "created_at": timestamp(a.created_at),
                "completed_at": timestamp(a.completed_at) if a.completed_at else None} for a in attempts],
            "refund": {"refund_id": refund.id, "state": refund.state, "amount_minor": refund.amount_minor,
                "created_at": timestamp(refund.created_at), "completed_at": timestamp(refund.completed_at) if refund.completed_at else None} if refund else None}}
