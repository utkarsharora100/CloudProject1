"""Recipient-scoped history, idempotent commands and fenced transport attempts."""
from datetime import timedelta
from sqlalchemy import select, func
from werkzeug.exceptions import BadRequest, Conflict, NotFound, ServiceUnavailable
from order_service.models import Order, db, timestamp, utc_now
from task_scheduler.domain import digest, number
from task_scheduler.fencing import lock_task, check_live
from notification_service.domain import CUSTOMER_TEMPLATES, COURIER_TEMPLATES, recipient, scenario, content_digest
from notification_service.models import Subscription, Message, Attempt, RequestReceipt, TransportReceipt
from notification_service.ingest import materialize
from notification_service.provider import SimulatedTransport

def stamp(value):
    return timestamp(value) if value else None

def summary(message):
    title, body = (CUSTOMER_TEMPLATES if message.recipient_type=="customer" else COURIER_TEMPLATES)[message.stage]
    return {"notification_id": message.id, "order_id": message.order_id, "recipient_type": message.recipient_type,
        "recipient_id": message.recipient_id, "channel": message.channel, "simulation": True,
        "stage": message.stage, "title": title, "body": body, "state": message.state,
        "scenario": message.scenario, "version": message.version, "attempt_count": message.attempts,
        "round_attempts": message.round_attempts, "max_round_attempts": 3, "retry_round": message.retry_round,
        "source": {"topic": message.source_topic, "event_id": message.source_event_id, "sequence": message.sequence,
                   "occurred_at": stamp(message.source_at)},
        "created_at": stamp(message.created_at), "available_at": stamp(message.available_at),
        "sent_at": stamp(message.sent_at), "read_at": stamp(message.read_at),
        "unread": message.state=="SENT" and message.read_at is None, "last_error_code": message.last_error_code,
        "task_id": message.task_id, "workflow_id": message.workflow_id}

def earlier_pending(session, message):
    table = Message.__table__
    return session.execute(select(table.c.id).where(table.c.order_id==message.order_id,
        table.c.recipient_type==message.recipient_type, table.c.recipient_id==message.recipient_id,
        table.c.source_topic==message.source_topic, table.c.sequence<message.sequence,
        table.c.state.in_(["PENDING", "SENDING", "RETRY_WAIT"])).limit(1)).first() is not None

class NotificationStore:
    def __init__(self, coordination, *, transport=None, clock=None):
        self.coordination = coordination
        self.clock = clock or (lambda: db.session.execute(select(func.utc_timestamp(6))).scalar_one()
                               if db.engine.dialect.name == "mysql" else utc_now())
        self.transport = transport

    def receipt(self, scope, key, body):
        row = db.session.get(RequestReceipt, (scope, key))
        if row:
            if row.request_hash!=digest(body):
                raise Conflict("Idempotency-Key was already used for another request.")
            result = row.response
            db.session.rollback()
            return result
        return None

    def save_receipt(self, scope, key, body, response):
        db.session.add(RequestReceipt(scope=scope, key_digest=key, request_hash=digest(body), response=response))

    def subscribe(self, order_id, body, key):
        db.session.rollback()
        order = db.session.execute(select(Order).where(Order.id==order_id).with_for_update()).scalar_one_or_none()
        if not order or order.customer_id!=body["customer_id"]:
            raise NotFound("Order not found for this customer.")
        scope = "subscription:"+order_id
        prior = self.receipt(scope, key, body)
        if prior is not None:
            return prior, True
        sub = db.session.get(Subscription, order_id)
        if sub:
            if (sub.customer_id,sub.scenario,sub.courier_updates)!=(body["customer_id"],body["scenario"],body["courier_updates"]):
                raise Conflict("This order already has a different notification policy.")
        else:
            sub = Subscription(order_id=order_id, **body, created_at=self.clock())
            db.session.add(sub); db.session.flush()
        materialize(sub, self.clock())
        result = {"order_id": order_id, "customer_id": sub.customer_id, "scenario": sub.scenario,
                  "courier_updates": sub.courier_updates, "simulation": True, "channel": "in_app"}
        self.save_receipt(scope, key, body, result)
        db.session.commit()
        return result, False

    def message(self, message_id, who, *, lock=False):
        query = select(Message).where(Message.id==message_id,
            Message.recipient_type==who[0], Message.recipient_id==who[1])
        if lock:
            query = query.with_for_update()
        row = db.session.execute(query).scalar_one_or_none()
        if row is None:
            raise NotFound("Notification not found for this recipient.")
        return row

    def command(self, message_id, action, body, key):
        allowed = {"recipient_type", "recipient_id", "expected_version"} | ({"scenario"} if action=="retry" else set())
        if not isinstance(body, dict) or set(body)-allowed or not {"recipient_type","recipient_id","expected_version"}<=set(body):
            raise BadRequest("Provide recipient_type, recipient_id, expected_version and optional retry scenario only.")
        who = recipient(body["recipient_type"], body["recipient_id"])
        version = number(body["expected_version"], "expected_version", 1, 2147483647)
        next_scenario = scenario(body.get("scenario", "success")) if action=="retry" else None
        db.session.rollback()
        message = self.message(message_id, who, lock=True)
        scope = action+":"+message_id
        previous = self.receipt(scope, key, body)
        if previous is not None:
            return previous, True
        if message.version!=version:
            raise Conflict("Notification version changed; read it before retrying.")
        if action=="read":
            if message.state!="SENT":
                raise Conflict("Only a confirmed notification can be read.")
            if message.read_at is None:
                message.read_at=self.clock(); message.version+=1
        else:
            if message.state!="FAILED" or message.retry_round>=10:
                raise Conflict("Explicit retry requires a failed notification with remaining retry rounds.")
            message.retry_round+=1; message.round_attempts=0; message.version+=1
            message.state="PENDING"; message.scenario=next_scenario
            message.last_error_code=None; message.available_at=self.clock()
        result = summary(message)
        self.save_receipt(scope, key, body, result)
        db.session.commit()
        return result, False

    def fenced_message(self, task_id, proof):
        task, order_id = lock_task(db.session, self.coordination, task_id, proof, self.clock, kinds={"notification"})
        # Serialize recipient ordering with source ingestion, before locking a message.
        db.session.execute(select(Subscription).where(Subscription.order_id==order_id).with_for_update()).first()
        message = db.session.execute(select(Message).where(Message.task_id==task_id).with_for_update()).scalar_one_or_none()
        if message is None or message.order_id!=order_id:
            raise Conflict("Task has no matching canonical notification.")
        return task, message

    def process(self, task_id, proof):
        db.session.rollback()
        task, message = self.fenced_message(task_id, proof)
        if message.state in {"SENT", "FAILED"}:
            result={"task_outcome": "succeeded" if message.state=="SENT" else "deferred", "notification": summary(message)}
            db.session.rollback(); return result
        if message.available_at>self.clock() or earlier_pending(db.session, message):
            db.session.rollback()
            return {"task_outcome":"deferred"}
        if message.state!="SENDING":
            message.attempts+=1; message.round_attempts+=1; message.version+=1; message.state="SENDING"
            db.session.add(Attempt(notification_id=message.id, number=message.attempts,
                retry_round=message.retry_round, task_id=task_id, generation=proof["generation"],
                outcome="STARTED", started_at=self.clock()))
        check_live(task, self.coordination, proof, self.clock())
        db.session.commit()  # durable reservation survives a provider/worker crash
        return self.finish_send(task_id, proof)

    def finish_send(self, task_id, proof):
        db.session.rollback()
        task, message = self.fenced_message(task_id, proof)
        if message.state=="SENT":
            result={"task_outcome":"succeeded","notification":summary(message)}
            db.session.rollback(); return result
        if message.state!="SENDING":
            raise Conflict("There is no reserved notification attempt.")
        attempt=db.session.get(Attempt,(message.id,message.attempts))
        now=self.clock()
        check_live(task, self.coordination, proof, now)
        transport=self.transport
        if transport is None:
            import os
            if os.environ.get("NOTIFICATION_TRANSPORT","sql")=="function":
                from notification_service.cloud_transport import FunctionTransport
                transport=FunctionTransport(db.engine)
            else:transport=SimulatedTransport(db.engine)
        # Stable content excludes retry policy, state and mutable ownership.
        fingerprint=content_digest(message)
        table=TransportReceipt.__table__
        with transport.engine.connect() as connection:
            receipt=connection.execute(select(table).where(table.c.notification_id==message.id)).mappings().one_or_none()
        fail=message.scenario=="always_fail" or (message.scenario=="fail_once" and message.round_attempts==1)
        if receipt:
            if receipt["payload_digest"]!=fingerprint:
                raise Conflict("Transport receipt does not match notification.")
            sent_at,new=receipt["delivered_at"],False
        elif fail:
            sent_at,new=None,False
        else:
            sent_at,new=transport.deliver(message.id,fingerprint,now)
        if new and message.scenario=="response_loss_once" and message.retry_round==1 and message.round_attempts==1:
            db.session.rollback()
            raise ServiceUnavailable("Simulated response lost; retry the same logical notification.")
        check_live(task, self.coordination, proof, self.clock())
        attempt.finished_at=now; message.version+=1
        if sent_at is not None:
            message.state="SENT"; message.sent_at=sent_at; message.last_error_code=None
            attempt.outcome="SENT" if new else "RECEIPT_RECOVERED"
        else:
            message.state="FAILED" if message.round_attempts>=3 else "RETRY_WAIT"
            message.last_error_code="simulated_transport_failure"
            message.available_at=now+timedelta(seconds=min(2**message.round_attempts,30))
            attempt.outcome="TRANSPORT_FAILURE"
        result={"task_outcome":"succeeded" if message.state=="SENT" else "deferred","notification":summary(message)}
        db.session.commit()
        return result
