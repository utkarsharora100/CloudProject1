"""Payment outcomes, idempotency, cancellation, destination fencing and crash gaps."""
from datetime import datetime, timedelta
import json
import tempfile
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from order_service.models import Order, OrderEvent, Outbox, db
from order_service.events import append_event
from payment_service.api import create_app
from payment_service.models import Payment, PaymentAttempt, PaymentEvent, ProviderOperation, Refund
from payment_service.provider import SimulatedProvider
from task_scheduler.models import Task, Attempt, Workflow, db as scheduler_db
from test_task_scheduler import FakeCoordination

class PaymentTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1)
        self.directory = tempfile.TemporaryDirectory()
        self.uri = "sqlite:///" + self.directory.name + "/payments.db"
        self.provider_engine = create_engine("sqlite://")
        ProviderOperation.__table__.create(self.provider_engine)
        self.provider = SimulatedProvider(self.provider_engine)
        self.coordination = FakeCoordination()
        self.worker = next(iter(self.coordination.registrations))
        self.config = {"TESTING": True, "APP_ENV": "local", "AUTO_CREATE_SCHEMA": True,
                       "SQLALCHEMY_DATABASE_URI": self.uri}
        self.app = create_app(self.config, coordination=self.coordination, provider=self.provider, clock=lambda: self.now)
        self.context = self.app.app_context()
        self.context.push()
        scheduler_db.metadata.create_all(bind=db.engine)
        self.client = self.app.test_client()
        self.store = self.app.extensions["payment_store"]
        order = Order(id=str(uuid.uuid4()), customer_id=1, address_id=1, restaurant_id="demo",
            total_minor=12999, currency="INR", status="PENDING_PAYMENT", version=1,
            delivery_snapshot={"line1": "Synthetic private address"},
            restaurant_snapshot={"estimated_prep_time_mins": 2}, items_snapshot=[],
            created_at=self.now, updated_at=self.now)
        db.session.add(order)
        db.session.flush()
        append_event(order, "OrderCreated", None)
        db.session.commit()
        self.order_id, self.path = order.id, "/payments/orders/" + order.id

    def tearDown(self):
        db.session.remove()
        db.engine.dispose()
        self.context.pop()
        self.provider_engine.dispose()
        self.directory.cleanup()

    def post(self, suffix="", body=None, key=None):
        return self.client.post(self.path+suffix, json=body or {}, headers={"Idempotency-Key": key or str(uuid.uuid4())})

    def create(self, scenario="success", refund="success", key=None):
        response = self.post(body={"expected_version": 1, "scenario": scenario, "refund_scenario": refund}, key=key)
        self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
        return response.get_json()

    def task(self, kind="payment", *, task_id=None, generation=1):
        token = str(uuid.uuid4())
        workflow_id = str(uuid.uuid4())
        if task_id is None:
            task_id = str(uuid.uuid4())
            db.session.execute(Workflow.__table__.insert().values(id=workflow_id, request_digest=uuid.uuid4().hex*2,
                request_hash="a"*64, order_id=self.order_id, created_at=self.now))
            db.session.execute(Task.__table__.insert().values(id=task_id, workflow_id=workflow_id, key=kind,
                kind=kind, label=kind, condition="always", params={}, state="RUNNING", reason="lease_started",
                generation=generation, revision=1, owner_id=self.worker, owner_name="payment-test",
                owner_session=self.coordination.registrations[self.worker]["session"],
                lease_until=self.now+timedelta(seconds=8), available_at=self.now, attempts=1, failures=0,
                max_failures=3, created_at=self.now, updated_at=self.now))
        else:
            db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task_id).values(
                generation=generation, state="RUNNING", lease_until=self.now+timedelta(seconds=8)))
        db.session.execute(Attempt.__table__.insert().values(task_id=task_id, generation=generation,
            lease_token=token, worker_id=self.worker, worker_name="payment-test",
            worker_session=self.coordination.registrations[self.worker]["session"], outcome="RUNNING", started_at=self.now))
        db.session.commit()
        self.coordination.leases[(task_id,generation)] = (self.worker,self.coordination.registrations[self.worker]["session"],token)
        return task_id, {"worker_id": self.worker, "generation": generation, "lease_token": token}

    def process(self, task):
        return self.client.post("/internal/tasks/"+task[0]+"/process",json=task[1])

    def state(self):
        return self.client.get(self.path).get_json()["payment"]

    def operations(self, kind=None):
        with self.provider_engine.connect() as c:
            rows=c.execute(select(ProviderOperation.__table__)).mappings().all()
        return [r for r in rows if kind is None or r["kind"]==kind]

    def version(self):
        db.session.expire_all()
        return db.session.get(Order,self.order_id).version

    def test_create_uses_saved_integer_amount_and_rejects_sensitive_or_price_inputs(self):
        for extra in ({"amount_minor":1},{"card_number":"dummy-not-a-card"},{"api_key":"dummy-private"}):
            response=self.post(body={"expected_version":1,"scenario":"success",**extra})
            self.assertEqual(response.status_code,400)
        value=self.create()
        self.assertEqual((value["amount_minor"],value["currency"],value["simulation"]),(12999,"INR",True))
        self.assertEqual(len(self.operations()),0)
        self.assertNotIn("delivery_snapshot",json.dumps(value))

    def test_create_replay_works_after_order_advances_and_changed_request_conflicts(self):
        key="MixedCase-payment-key"
        first=self.create(key=key)
        self.assertEqual(self.process(self.task()).status_code,200)
        replay=self.create(key=key)
        self.assertEqual(first,replay)
        self.assertEqual(self.post(body={"expected_version":1,"scenario":"decline"},key=key).status_code,409)
        self.assertEqual(Payment.query.count(),1)
        self.assertEqual(PaymentAttempt.query.count(),1)

    def test_case_distinct_keys_do_not_replay_same_request(self):
        self.create(key="MixedCase-payment-key")
        response=self.post(body={"expected_version":1,"scenario":"success"},key="mixedcase-payment-key")
        self.assertEqual(response.status_code,409)
        self.assertNotEqual(response.headers.get("Idempotency-Replayed"),"true")

    def test_success_and_repeat_processing_do_not_duplicate_capture_or_order_event(self):
        self.create()
        task=self.task()
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(self.state()["state"],"CAPTURED")
        self.assertEqual(len(self.operations()),1)
        self.assertEqual(OrderEvent.query.count(),2)
        self.assertEqual(Outbox.query.count(),2)

    def test_decline_requires_explicit_retry_then_single_successful_capture(self):
        self.create("decline_then_success")
        self.assertEqual(self.process(self.task()).get_json()["task_outcome"],"deferred")
        value=self.state()
        self.assertEqual((value["state"],value["order_status"]),("DECLINED","PAYMENT_FAILED"))
        key=str(uuid.uuid4());body={"expected_version":value["order_version"]}
        first=self.post("/retry",body,key)
        self.assertEqual(first.status_code,202)
        self.assertEqual(self.post("/retry",body,key).get_json(),first.get_json())
        self.assertEqual(self.process(self.task()).status_code,200)
        self.assertEqual(self.state()["state"],"CAPTURED")
        self.assertEqual([r["outcome"] for r in self.operations()].count("SUCCEEDED"),1)
        self.assertEqual(PaymentAttempt.query.count(),2)

    def test_always_decline_never_advances_kitchen_or_creates_a_capture(self):
        self.create("decline")
        self.process(self.task())
        self.assertEqual(self.state()["order_status"],"PAYMENT_FAILED")
        self.assertTrue(all(r["outcome"]=="DECLINED" for r in self.operations()))
        self.assertEqual(self.post("/retry",{"expected_version":1}).status_code,409)

    def test_timeout_receipt_survives_rollback_and_same_attempt_recovers(self):
        self.create("timeout_once")
        task=self.task()
        self.assertEqual(self.process(task).status_code,503)
        self.assertEqual((self.state()["state"],self.state()["order_status"]),("PENDING","PENDING_PAYMENT"))
        self.assertEqual(len(self.operations()),1)
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual((len(self.operations()),PaymentAttempt.query.count(),OrderEvent.query.count()),(1,1,2))

    def test_unpaid_cancel_records_no_charge_and_rejects_late_worker(self):
        self.create()
        task=self.task()
        response=self.post("/cancel",{"expected_version":1})
        self.assertEqual(response.status_code,202)
        self.assertEqual(self.state()["state"],"CANCELLED")
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(len(self.operations()),0)
        self.assertEqual(Refund.query.count(),0)

    def test_cancel_without_payment_request_has_a_persistent_no_charge_record(self):
        response=self.post("/cancel",{"expected_version":1})
        self.assertEqual(response.status_code,202)
        self.assertEqual((self.state()["state"],PaymentAttempt.query.count()),("CANCELLED",0))

    def test_cancel_after_uncertain_capture_queues_refund_instead_of_unpaid_cancel(self):
        self.create("timeout_once","timeout_once")
        self.assertEqual(self.process(self.task()).status_code,503)
        self.assertEqual(self.post("/cancel",{"expected_version":1}).status_code,202)
        self.assertEqual(self.state()["order_status"],"REFUND_PENDING")
        refund_task=self.task("refund")
        self.assertEqual(self.process(refund_task).status_code,503)
        self.assertEqual(self.state()["state"],"REFUND_PENDING")
        self.assertEqual(self.process(refund_task).status_code,200)
        self.assertEqual(self.process(refund_task).status_code,200)
        self.assertEqual((self.state()["state"],len(self.operations("CAPTURE")),len(self.operations("REFUND"))),("REFUNDED",1,1))

    def test_paid_cancel_and_duplicate_request_create_one_refund(self):
        self.create()
        self.process(self.task())
        body={"expected_version":self.version()};key=str(uuid.uuid4())
        first=self.post("/cancel",body,key)
        self.assertEqual(first.status_code,202)
        self.assertEqual(self.post("/cancel",body,key).get_json(),first.get_json())
        self.assertEqual(Refund.query.count(),1)
        self.assertEqual(self.process(self.task("refund")).status_code,200)
        self.assertEqual(self.post("/cancel",{"expected_version":self.version()}).status_code,409)

    def test_declined_cancel_has_no_refund(self):
        self.create("decline");self.process(self.task())
        self.assertEqual(self.post("/cancel",{"expected_version":self.version()}).status_code,202)
        self.assertEqual((self.state()["state"],Refund.query.count()),("CANCELLED",0))

    def test_late_cancellation_is_rejected_without_changing_payment(self):
        self.create();self.process(self.task())
        order=db.session.get(Order,self.order_id)
        self.store.move(order,"KITCHEN_ACCEPTED");self.store.move(order,"PREPARING");db.session.commit()
        self.assertEqual(self.post("/cancel",{"expected_version":self.version()}).status_code,409)
        self.assertEqual((self.state()["state"],Refund.query.count()),("CAPTURED",0))

    def test_missing_invalid_stale_and_expired_proofs_make_no_provider_operation(self):
        self.create();task=self.task()
        self.assertEqual(self.client.post("/internal/tasks/"+task[0]+"/process",json={}).status_code,400)
        for change in ({"generation":2},{"lease_token":str(uuid.uuid4())},{"worker_id":str(uuid.uuid4())}):
            self.assertEqual(self.process((task[0],{**task[1],**change})).status_code,409)
        self.now+=timedelta(seconds=9)
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(len(self.operations()),0)

    def test_quorum_loss_rejects_processing_but_preserves_readable_payment(self):
        self.create();task=self.task();self.coordination.live=False
        self.assertEqual(self.process(task).status_code,503)
        self.assertEqual(self.client.get(self.path).status_code,200)
        self.assertEqual(len(self.operations()),0)

    def test_lease_loss_after_provider_effect_blocks_confirmation_and_reassignment_recovers(self):
        self.create();task=self.task()
        original=self.provider.apply
        def lose_lease(**kwargs):
            result=original(**kwargs);self.now+=timedelta(seconds=9);return result
        with patch.object(self.provider,"apply",side_effect=lose_lease):
            self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(self.state()["order_status"],"PENDING_PAYMENT")
        replacement=self.task(task_id=task[0],generation=2)
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(self.process(replacement).status_code,200)
        self.assertEqual(len(self.operations()),1)

    def test_waiting_for_order_lock_cannot_start_provider_work_after_lease_expiry(self):
        self.create();task=self.task()
        original=self.store.order
        def delayed_order(*args,**kwargs):
            result=original(*args,**kwargs);self.now+=timedelta(seconds=9);return result
        with patch.object(self.store,"order",side_effect=delayed_order):
            self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(len(self.operations()),0)

    def test_order_confirmation_rollback_preserves_receipt_for_retry(self):
        self.create();task=self.task()
        with patch.object(db.session,"commit",side_effect=SQLAlchemyError("dummy-private-detail")):
            response=self.process(task)
        self.assertEqual(response.status_code,503)
        self.assertNotIn("dummy-private-detail",response.get_data(as_text=True))
        self.assertEqual(self.state()["order_status"],"PENDING_PAYMENT")
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(len(self.operations()),1)

    def test_public_history_is_paginated_and_contains_no_proof_or_customer_fields(self):
        self.create();task=self.task();self.process(task)
        response=self.client.get(self.path+"/events?limit=1")
        self.assertEqual(response.status_code,200)
        self.assertIsNotNone(response.get_json()["next_after"])
        data=json.dumps(self.state())+response.get_data(as_text=True)
        self.assertNotIn(task[1]["lease_token"],data)
        self.assertNotIn("Synthetic private address",data)
        self.assertEqual(response.headers["Cache-Control"],"no-store")
        self.assertEqual(self.client.get(self.path+"/events?limit=0").status_code,400)

    def test_provider_same_key_with_changed_amount_is_rejected(self):
        args={"operation_id":str(uuid.uuid4()),"payment_id":str(uuid.uuid4()),"kind":"CAPTURE","amount_minor":12999,"currency":"INR"}
        self.provider.apply(**args)
        from werkzeug.exceptions import Conflict
        with self.assertRaises(Conflict):
            self.provider.apply(**{**args,"amount_minor":1})
        self.assertEqual(len(self.operations()),1)

    def test_managed_order_payment_transition_cannot_bypass_ledger(self):
        self.create()
        from order_service.event_producer import create_app as order_app
        from order_fixtures import DemoDependencies
        app=order_app({**self.config,"AUTO_CREATE_SCHEMA":False},dependencies=DemoDependencies())
        response=app.test_client().post("/orders/"+self.order_id+"/transitions",
            json={"expected_version":1,"to_state":"PAID"},headers={"Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(response.status_code,409)
        self.assertEqual(len(self.operations()),0)
        with app.app_context():
            db.session.remove();db.engine.dispose()

    def test_invalid_scenarios_versions_and_public_mode_fail_closed(self):
        for body in ({"expected_version":True,"scenario":"success"},{"expected_version":1,"scenario":[]},
                     {"expected_version":1,"scenario":"random"},{"expected_version":1,"scenario":"success","refund_scenario":"decline"}):
            self.assertEqual(self.post(body=body).status_code,400)
        self.assertEqual(self.post(body={"expected_version":2,"scenario":"success"}).status_code,409)
        with self.assertRaises(RuntimeError):
            create_app({"APP_ENV":"production"},coordination=self.coordination,provider=self.provider)
