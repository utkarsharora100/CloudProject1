"""Kitchen stage invariants, rejection/refund atomicity, fencing and projection recovery."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import tempfile
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from order_service.models import Order, OrderEvent, Outbox, db
from order_service.events import append_event
from payment_service.models import Payment, Refund
from payment_service.store import PaymentStore
from kitchen_workflow.api import create_app
from kitchen_workflow.models import Ticket, Command, RequestReceipt, Event
from kitchen_workflow.projection import should_apply
from task_scheduler.models import db as scheduler_db
from test_task_scheduler import FakeCoordination
import test_payment_service as payment_helpers

class KitchenTests(unittest.TestCase):
    task = payment_helpers.PaymentTests.task

    def setUp(self):
        self.now = datetime(2026,1,1)
        self.directory = tempfile.TemporaryDirectory()
        self.config = {"TESTING":True,"APP_ENV":"local","AUTO_CREATE_SCHEMA":True,
                       "SQLALCHEMY_DATABASE_URI":"sqlite:///"+self.directory.name+"/kitchen.db"}
        self.coordination = FakeCoordination()
        self.worker = next(iter(self.coordination.registrations))
        self.app = create_app(self.config,coordination=self.coordination,clock=lambda:self.now)
        self.context = self.app.app_context();self.context.push()
        scheduler_db.metadata.create_all(bind=db.engine)
        self.client = self.app.test_client()
        self.store = self.app.extensions["kitchen_store"]
        order = Order(id=str(uuid.uuid4()),customer_id=1,address_id=1,restaurant_id="demo",
            total_minor=12999,currency="INR",status="PENDING_PAYMENT",version=1,
            delivery_snapshot={"line1":"Private synthetic address","phone":"dummy-private-phone"},
            restaurant_snapshot={"estimated_prep_time_mins":18},
            items_snapshot=[{"item_id":"meal","name":"Saved meal","quantity":1,"unit_price_minor":12999,"line_total_minor":12999}],
            created_at=self.now,updated_at=self.now)
        db.session.add(order);db.session.flush();append_event(order,"OrderCreated",None)
        order.status,order.version="PAID",2
        append_event(order,"OrderPaymentSucceeded","PENDING_PAYMENT")
        payment = Payment(order_id=order.id,amount_minor=12999,currency="INR",scenario="success",
            refund_scenario="success",state="CAPTURED",attempt_number=1,version=2,
            created_at=self.now,updated_at=self.now)
        db.session.add(payment);db.session.commit()
        self.order_id = order.id
        self.path = "/kitchen/orders/"+order.id
        self.store.materialize()

    def tearDown(self):
        db.session.remove();db.engine.dispose();self.context.pop();self.directory.cleanup()

    def version(self):
        db.session.expire_all()
        return db.session.get(Order,self.order_id).version

    def action(self,action,*,key=None,version=None,**extra):
        return self.client.post(self.path+"/actions",
            json={"action":action,"expected_version":self.version() if version is None else version,**extra},
            headers={"Idempotency-Key":key or str(uuid.uuid4())})

    def process(self,task):
        return self.client.post("/internal/tasks/"+task[0]+"/process",json=task[1])

    def detail(self):
        response=self.client.get(self.path)
        self.assertEqual(response.status_code,200)
        return response.get_json()["ticket"]

    def accept(self):
        self.assertEqual(self.action("accept").status_code,202)
        self.assertEqual(self.process(self.task("kitchen_accept")).status_code,200)

    def start(self):
        self.accept()
        self.assertEqual(self.action("start_preparation").status_code,202)
        task=self.task("preparation")
        self.assertEqual(self.process(task).get_json()["task_outcome"],"deferred")
        return task

    def test_queue_materializes_once_from_captured_payment_and_saved_snapshot(self):
        self.store.materialize()
        self.assertEqual((Ticket.query.count(),Event.query.count()),(1,1))
        value=self.detail()
        self.assertEqual((value["state"],value["estimated_prep_time_mins"]),("WAITING",18))
        self.assertEqual(value["items"],[{"item_id":"meal","name":"Saved meal","quantity":1}])
        self.assertFalse(value["managed"])
        self.assertEqual(self.version(),2)

    def test_uncaptured_or_unpaid_orders_cannot_be_accepted(self):
        Event.query.delete();Ticket.query.delete()
        payment=Payment.query.one();payment.state="DECLINED";db.session.commit()
        self.store.materialize()
        self.assertEqual(Ticket.query.count(),0)
        self.assertEqual(self.action("accept").status_code,409)
        self.assertEqual(Command.query.count(),0)

    def test_accept_request_is_explicit_and_replay_survives_worker_progress(self):
        key=str(uuid.uuid4())
        response=self.action("accept",key=key,version=2)
        self.assertEqual(response.status_code,202)
        saved=response.get_json()
        self.assertEqual((self.detail()["state"],self.version()),("WAITING",2))
        task=self.task("kitchen_accept")
        self.assertEqual(self.process(task).get_json()["task_outcome"],"succeeded")
        self.assertEqual(self.process(task).status_code,200)
        replay=self.action("accept",key=key,version=2)
        self.assertEqual(replay.get_json(),saved)
        self.assertEqual(replay.headers["Idempotency-Replayed"],"true")
        self.assertEqual((self.detail()["state"],Command.query.count(),OrderEvent.query.count()),("ACCEPTED",1,3))

    def test_ready_and_preparation_are_blocked_before_acceptance(self):
        self.assertEqual(self.action("ready").status_code,409)
        self.assertEqual(self.action("start_preparation").status_code,409)
        self.assertEqual(Command.query.count(),0)

    def test_preparation_needs_separate_ready_action_and_records_actual_timestamps(self):
        task=self.start()
        started=self.detail()["preparation_started_at"]
        self.now+=timedelta(minutes=7)
        self.store.synchronize()
        self.assertEqual(self.detail()["state"],"PREPARING")
        self.assertIsNone(self.detail()["ready_at"])
        self.assertEqual(self.action("ready").status_code,202)
        replacement=self.task("preparation",task_id=task[0],generation=2)
        self.assertEqual(self.process(replacement).get_json()["task_outcome"],"succeeded")
        value=self.detail()
        self.assertEqual((value["state"],value["order_status"]),("READY","READY"))
        self.assertLess(started,value["ready_at"])
        self.assertEqual([c["action"] for c in value["commands"]],["accept","start_preparation","ready"])

    def test_rejection_records_human_decision_and_payment_owned_refund_atomically(self):
        response=self.action("reject",reason="capacity")
        self.assertEqual(response.status_code,202)
        value=self.detail()
        self.assertEqual((value["state"],value["order_status"],value["rejection_reason"]),("REJECTED","REFUND_PENDING","capacity"))
        self.assertEqual((Refund.query.count(),Payment.query.one().state),(1,"REFUND_PENDING"))
        command=Command.query.one()
        self.assertEqual((command.state,command.applied_by,command.task_id),("APPLIED","human",None))
        self.store.synchronize()
        self.assertEqual(self.detail()["state"],"REJECTED")

    def test_rejection_after_acceptance_is_allowed_before_preparation(self):
        self.accept()
        self.assertEqual(self.action("reject",reason="closing").status_code,202)
        self.assertEqual((self.detail()["state"],Refund.query.count()),("REJECTED",1))

    def test_rejection_after_preparation_is_rejected_without_refund(self):
        self.start()
        self.assertEqual(self.action("reject",reason="closing").status_code,409)
        self.assertEqual((self.detail()["state"],Refund.query.count()),("PREPARING",0))

    def test_rejection_replay_does_not_duplicate_refund_or_history(self):
        key=str(uuid.uuid4())
        first=self.action("reject",key=key,version=2)
        again=self.action("reject",key=key,version=2)
        self.assertEqual(first.get_json(),again.get_json())
        self.assertEqual((Refund.query.count(),OrderEvent.query.count(),Command.query.count()),(1,3,1))

    def test_pending_accept_prevents_contradictory_or_duplicate_new_actions(self):
        key=str(uuid.uuid4())
        self.action("accept",key=key)
        self.assertEqual(self.action("reject").status_code,409)
        self.assertEqual(self.action("accept").status_code,409)
        self.assertEqual(self.action("reject",key=key).status_code,409)
        self.assertEqual((Command.query.count(),Refund.query.count()),(1,0))

    def test_request_keys_are_case_sensitive_and_stale_versions_conflict(self):
        self.assertEqual(self.action("accept",version=1).status_code,409)
        first=self.action("accept",key="CaseSensitive-key")
        other=self.action("accept",key="casesensitive-key")
        self.assertEqual(first.status_code,202)
        self.assertEqual(other.status_code,409)
        self.assertNotEqual(other.headers.get("Idempotency-Replayed"),"true")
        self.assertEqual(RequestReceipt.query.count(),1)

    def test_customer_cancellation_cancels_pending_kitchen_action(self):
        self.action("accept")
        order=db.session.get(Order,self.order_id)
        PaymentStore(self.coordination,None,clock=lambda:self.now).request_refund_locked(order,Payment.query.one())
        db.session.commit()
        self.store.synchronize()
        self.assertEqual((self.detail()["state"],Command.query.one().state),("CANCELLED","CANCELLED"))
        self.assertEqual(self.action("accept").status_code,409)
        self.assertEqual(self.version(),3)

    def test_stale_tokens_generations_sessions_and_expiry_cannot_advance_stage(self):
        self.action("accept");task=self.task("kitchen_accept")
        for change in ({"lease_token":str(uuid.uuid4())},{"generation":2},{"worker_id":str(uuid.uuid4())}):
            self.assertEqual(self.process((task[0],{**task[1],**change})).status_code,409)
        self.coordination.registrations[self.worker]["session"]="different-session"
        self.assertEqual(self.process(task).status_code,409)
        self.coordination.registrations[self.worker]["session"]="100"
        self.now+=timedelta(seconds=9)
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual((self.version(),self.detail()["state"]),(2,"WAITING"))

    def test_quorum_loss_rejects_worker_stage_but_human_rejection_can_request_refund(self):
        self.action("accept");task=self.task("kitchen_accept");self.coordination.live=False
        self.assertEqual(self.process(task).status_code,503)
        self.assertEqual(self.client.get(self.path).status_code,200)
        # The pending accept remains protected; a fresh order's rejection needs
        # SQL business validation rather than a fabricated worker lease.
        db.session.get(Command,Command.query.one().id).state="CANCELLED";db.session.commit()
        self.assertEqual(self.action("reject").status_code,202)

    def test_lock_wait_expiry_is_checked_before_any_stage_write(self):
        self.action("accept");task=self.task("kitchen_accept")
        original=self.store.order
        def delay(*args,**kwargs):
            value=original(*args,**kwargs);self.now+=timedelta(seconds=9);return value
        with patch.object(self.store,"order",side_effect=delay):
            self.assertEqual(self.process(task).status_code,409)
        self.assertEqual((self.version(),OrderEvent.query.count()),(2,2))

    def test_stage_commit_failure_rolls_back_order_ticket_command_and_outbox(self):
        self.action("accept");task=self.task("kitchen_accept")
        with patch.object(db.session,"commit",side_effect=SQLAlchemyError("dummy-private-sql-detail")):
            response=self.process(task)
        self.assertEqual(response.status_code,503)
        self.assertNotIn("dummy-private-sql-detail",response.get_data(as_text=True))
        self.assertEqual((self.version(),self.detail()["state"],Outbox.query.count()),(2,"WAITING",2))
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual((self.version(),Outbox.query.count()),(3,3))

    def test_rejection_commit_failure_rolls_back_refund_and_kitchen_decision(self):
        with patch.object(db.session,"commit",side_effect=SQLAlchemyError("dummy-private-sql-detail")):
            response=self.action("reject")
        self.assertEqual(response.status_code,503)
        self.assertEqual((Refund.query.count(),Command.query.count(),self.version()),(0,0,2))
        self.assertEqual(self.detail()["state"],"WAITING")

    def test_queue_history_and_responses_omit_private_customer_and_lease_data(self):
        self.action("accept");task=self.task("kitchen_accept");self.process(task)
        queue=self.client.get("/kitchen/restaurants/demo/orders?state=ACCEPTED&limit=1")
        self.assertEqual(queue.status_code,200)
        self.assertEqual(len(queue.get_json()["tickets"]),1)
        history=self.client.get(self.path+"/events?limit=1")
        self.assertIsNotNone(history.get_json()["next_after"])
        data=json.dumps(self.detail())+queue.get_data(as_text=True)+history.get_data(as_text=True)
        for value in (task[1]["lease_token"],"Private synthetic address","dummy-private-phone","delivery_snapshot"):
            self.assertNotIn(value,data)
        self.assertEqual(history.headers["Cache-Control"],"no-store")
        self.assertEqual(self.client.get("/kitchen/restaurants/demo/orders?state=invalid").status_code,400)
        self.assertEqual(self.client.get(self.path+"/events?limit=0").status_code,400)

    def test_invalid_actions_and_unknown_fields_leave_no_commands(self):
        for body in ({"action":[],"expected_version":2},{"action":"accept","expected_version":True},
                     {"action":"ready","expected_version":2,"reason":"capacity"},
                     {"action":"reject","expected_version":2,"reason":"arbitrary-secret-text"},
                     {"action":"accept","expected_version":2,"phone":"private"}):
            response=self.client.post(self.path+"/actions",json=body,headers={"Idempotency-Key":str(uuid.uuid4())})
            self.assertEqual(response.status_code,400)
        self.assertEqual(Command.query.count(),0)

    def test_managed_order_cannot_bypass_kitchen_stage_api(self):
        self.action("accept")
        from order_service.event_producer import create_app as order_app
        from order_fixtures import DemoDependencies
        app=order_app({**self.config,"AUTO_CREATE_SCHEMA":False},dependencies=DemoDependencies())
        response=app.test_client().post("/orders/"+self.order_id+"/transitions",
            json={"expected_version":2,"to_state":"KITCHEN_ACCEPTED"},headers={"Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(response.status_code,409)
        with app.app_context():db.session.remove();db.engine.dispose()

    def test_projection_monotonic_version_and_source_identity(self):
        payload=self.store.projection_payload(Ticket.query.one())
        self.assertTrue(should_apply(None,payload))
        self.assertFalse(should_apply(deepcopy(payload),payload))
        self.assertFalse(should_apply({**payload,"kitchen_version":10},payload))
        self.assertTrue(should_apply({**payload,"kitchen_version":0},payload))
        with self.assertRaises(ValueError):
            should_apply({**payload,"source_event_id":str(uuid.uuid4())},payload)

    def test_projection_failure_preserves_business_state_and_retries(self):
        class Broken:
            def put(self,payload):raise RuntimeError("dummy-private-error")
        self.store.project_one(Broken())
        ticket=Ticket.query.one()
        self.assertEqual((ticket.state,ticket.projected_version,ticket.projection_error),("WAITING",0,"firestore_projection_unavailable"))
        self.now+=timedelta(seconds=3)
        class Good:
            def put(self,payload):return True
        self.store.project_one(Good())
        ticket=Ticket.query.one()
        self.assertEqual((ticket.projected_version,ticket.version,ticket.projection_error),(1,1,None))

    def test_projection_race_cannot_acknowledge_a_newer_ticket_version(self):
        outer=self
        class Concurrent:
            def put(self,payload):
                self.payload=payload
                outer.action("accept")
        repository=Concurrent()
        self.store.project_one(repository)
        ticket=Ticket.query.one()
        self.assertGreater(ticket.version,repository.payload["kitchen_version"])
        self.assertLess(ticket.projected_version,ticket.version)

    def test_nonlocal_mode_is_blocked(self):
        with self.assertRaises(RuntimeError):
            create_app({"APP_ENV":"production"},coordination=self.coordination)
