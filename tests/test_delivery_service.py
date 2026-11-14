"""Courier exclusivity, honest presence, handoff fencing, capacity release and event recovery."""
from datetime import datetime,timedelta
import json
import tempfile
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from order_service.models import Order,OrderEvent,Outbox,db
from order_service.events import append_event
from kitchen_workflow.models import Ticket
from payment_service.models import Payment
from delivery_assignment_service.api import create_app
from delivery_assignment_service.models import Zone,Courier,Job,Assignment,ActiveSlot,Command,Receipt,Event
from delivery_assignment_service import policy
from delivery_assignment_service import outbox
from task_scheduler.models import db as scheduler_db
from test_task_scheduler import FakeCoordination
import test_payment_service as payment_helpers

class DeliveryTests(unittest.TestCase):
    task=payment_helpers.PaymentTests.task

    def setUp(self):
        self.now=datetime(2026,1,1)
        self.directory=tempfile.TemporaryDirectory()
        self.config={"TESTING":True,"APP_ENV":"local","AUTO_CREATE_SCHEMA":True,
                     "SQLALCHEMY_DATABASE_URI":"sqlite:///"+self.directory.name+"/delivery.db"}
        self.coordination=FakeCoordination();self.worker=next(iter(self.coordination.registrations))
        self.app=create_app(self.config,coordination=self.coordination,clock=lambda:self.now)
        self.context=self.app.app_context();self.context.push();scheduler_db.metadata.create_all(bind=db.engine)
        self.client=self.app.test_client();self.store=self.app.extensions["delivery_store"]
        self.order_id=self.seed_order()
        self.post("/delivery/zones",{"zone":"demo","scenario":"manual"},expected=201)

    def tearDown(self):
        db.session.remove();db.engine.dispose();self.context.pop();self.directory.cleanup()

    def seed_order(self):
        order=Order(id=str(uuid.uuid4()),customer_id=1,address_id=1,restaurant_id="demo",total_minor=12999,currency="INR",
            status="PENDING_PAYMENT",version=1,delivery_snapshot={"line1":"Private synthetic street","phone":"dummy-private-phone"},
            restaurant_snapshot={"estimated_prep_time_mins":2},items_snapshot=[{"item_id":"meal","name":"Demo meal","quantity":1}],
            created_at=self.now,updated_at=self.now)
        db.session.add(order);db.session.flush();append_event(order,"OrderCreated",None)
        for state in ("PAID","KITCHEN_ACCEPTED","PREPARING","READY"):
            old=order.status;order.status=state;order.version+=1;append_event(order,"FixtureStage",old)
        paid=db.session.execute(select(OrderEvent).where(OrderEvent.order_id==order.id,OrderEvent.to_state=="PAID")).scalar_one()
        db.session.add(Payment(order_id=order.id,amount_minor=12999,currency="INR",scenario="success",refund_scenario="success",
            state="CAPTURED",attempt_number=1,version=2,created_at=self.now,updated_at=self.now))
        db.session.add(Ticket(order_id=order.id,restaurant_id="demo",source_event_id=paid.event_id,state="READY",version=7,managed=True,
            items=[{"item_id":"meal","name":"Demo meal","quantity":1}],estimated_prep_time_mins=2,paid_at=self.now,
            created_at=self.now,updated_at=self.now,ready_at=self.now,projected_version=7,projection_due_at=self.now,projection_attempts=0))
        db.session.commit();return order.id

    def post(self,path,body,key=None,expected=None):
        response=self.client.post(path,json=body,headers={"Idempotency-Key":key or str(uuid.uuid4())})
        if expected is not None:self.assertEqual(response.status_code,expected,response.get_data(as_text=True))
        return response

    def courier(self,name="Courier A",zone="demo",online=True):
        data=self.post("/couriers",{"name":name,"zone":zone},expected=201).get_json()["courier"]
        return self.presence(data["courier_id"]) if online else data

    def read_courier(self,value):
        return self.client.get("/couriers/"+value).get_json()["courier"]

    def presence(self,value,online=True,accepting=True,key=None,version=None):
        current=self.read_courier(value)
        return self.post("/couriers/"+value+"/presence",{"expected_version":current["version"] if version is None else version,
            "online":online,"accepting_work":accepting},key,expected=200).get_json()["courier"]

    def dispatch(self,value=None,zone="demo",key=None):
        value=value or self.order_id
        order=db.session.get(Order,value)
        return self.post("/delivery/orders/"+value,{"expected_version":order.version,"zone":zone},key,expected=202).get_json()["delivery"]

    def process(self,task):
        return self.client.post("/internal/tasks/"+task[0]+"/process",json=task[1])

    def assigned(self):
        courier=self.courier();self.dispatch()
        task=self.task("courier_assignment")
        self.assertEqual(self.process(task).status_code,200)
        return courier,self.detail()["assignment"],task

    def detail(self,value=None):
        return self.client.get("/delivery/orders/"+(value or self.order_id)).get_json()["delivery"]

    def command(self,action,*,key=None,courier=None,assignment=None,version=None,expected=202):
        data=self.detail()
        return self.post("/delivery/orders/"+self.order_id+"/actions",{"action":action,
            "expected_version":data["order_version"] if version is None else version,
            "courier_id":courier or data["assignment"]["courier_id"],
            "assignment_id":assignment or data["assignment"]["assignment_id"]},key,expected)

    def picked(self):
        courier,assignment,task=self.assigned()
        self.command("pickup");self.assertEqual(self.process(self.task("pickup")).status_code,200)
        return courier,assignment

    def test_courier_registration_is_offline_and_idempotent(self):
        key=str(uuid.uuid4());body={"name":"Demo A","zone":"demo"}
        first=self.post("/couriers",body,key,201)
        repeat=self.post("/couriers",body,key,201)
        self.assertEqual(first.get_json(),repeat.get_json())
        self.assertFalse(first.get_json()["courier"]["eligible"])
        self.assertEqual(Courier.query.count(),1)

    def test_presence_expires_without_claiming_courier_is_available(self):
        courier=self.courier();self.assertTrue(courier["eligible"])
        self.now+=timedelta(seconds=31)
        value=self.read_courier(courier["courier_id"])
        self.assertEqual((value["state"],value["presence_fresh"]),("OFFLINE",False))
        self.assertEqual(self.store.capacity("demo")["counts"]["AVAILABLE"],0)

    def test_heartbeat_replay_does_not_extend_presence_but_new_key_does(self):
        courier=self.courier();path="/couriers/"+courier["courier_id"]+"/heartbeat";key=str(uuid.uuid4())
        original=self.post(path,{},key,200).get_json()
        self.now+=timedelta(seconds=31)
        self.assertEqual(self.post(path,{},key,200).get_json(),original)
        self.assertFalse(self.read_courier(courier["courier_id"])["eligible"])
        self.post(path,{},expected=200)
        self.assertTrue(self.read_courier(courier["courier_id"])["eligible"])

    def test_offline_heartbeat_cannot_silently_bring_courier_online(self):
        courier=self.courier(online=False)
        self.assertEqual(self.post("/couriers/"+courier["courier_id"]+"/heartbeat",{}).status_code,409)

    def test_unready_unpaid_or_unmanaged_food_cannot_dispatch(self):
        ticket=db.session.get(Ticket,self.order_id);ticket.managed=False;db.session.commit()
        self.assertEqual(self.post("/delivery/orders/"+self.order_id,{"expected_version":5,"zone":"demo"}).status_code,409)
        ticket.managed=True;ticket.state="PREPARING";db.session.commit()
        self.assertEqual(self.post("/delivery/orders/"+self.order_id,{"expected_version":5,"zone":"demo"}).status_code,409)
        self.assertEqual(Job.query.count(),0)

    def test_no_courier_wait_is_a_resource_condition_without_assignment(self):
        self.dispatch()
        self.assertEqual(policy.gate(db.session,self.order_id,"courier_assignment",self.now),"courier_unavailable")
        self.assertEqual(self.process(self.task("courier_assignment")).get_json()["task_outcome"],"deferred")
        self.assertEqual((Assignment.query.count(),ActiveSlot.query.count(),self.detail()["order_status"]),(0,0,"READY"))

    def test_oldest_ready_order_is_selected_before_later_request(self):
        courier=self.courier();self.dispatch()
        self.now+=timedelta(seconds=1);second=self.seed_order();self.dispatch(second)
        original=self.order_id;self.order_id=second
        self.assertEqual(self.process(self.task("courier_assignment")).get_json()["task_outcome"],"deferred")
        self.assertEqual(self.detail()["wait_reason"],"older_order_waiting")
        self.order_id=original
        self.assertEqual(self.process(self.task("courier_assignment")).status_code,200)
        self.assertEqual(Assignment.query.one().order_id,original)

    def test_one_courier_cannot_be_allocated_to_competing_orders(self):
        courier,assignment,task=self.assigned()
        self.order_id=self.seed_order();self.dispatch()
        self.assertEqual(self.process(self.task("courier_assignment")).get_json()["task_outcome"],"deferred")
        self.assertEqual((Assignment.query.count(),ActiveSlot.query.count()),(1,1))

    def test_assignment_retry_reuses_identity_and_does_not_duplicate_order_event(self):
        courier,assignment,task=self.assigned()
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(self.detail()["assignment"]["assignment_id"],assignment["assignment_id"])
        self.assertEqual((Assignment.query.count(),ActiveSlot.query.count(),OrderEvent.query.count()),(1,1,6))

    def test_database_unique_slot_constraints_protect_both_sides(self):
        courier,assignment,task=self.assigned()
        other=self.courier("Courier B")
        with self.assertRaises(IntegrityError):
            db.session.execute(ActiveSlot.__table__.insert().values(courier_id=other["courier_id"],order_id=self.order_id,assignment_id=str(uuid.uuid4())))
            db.session.commit()
        db.session.rollback()
        self.assertEqual(ActiveSlot.query.count(),1)

    def test_delivery_requires_pickup_and_correct_assignment_actor(self):
        courier,assignment,task=self.assigned()
        self.command("deliver",expected=409)
        self.command("pickup",courier=str(uuid.uuid4()),expected=409)
        self.command("pickup",assignment=str(uuid.uuid4()),expected=409)
        self.assertEqual(Command.query.count(),0)

    def test_pickup_is_explicit_and_keeps_capacity_occupied(self):
        courier,assignment,task=self.assigned()
        self.command("pickup")
        self.assertEqual(self.detail()["state"],"ASSIGNED")
        self.process(self.task("pickup"))
        self.assertEqual((self.detail()["state"],ActiveSlot.query.count()),("PICKED_UP",1))
        self.assertEqual(self.read_courier(courier["courier_id"])["state"],"DELIVERING")

    def test_confirmed_delivery_releases_capacity_once_and_replay_is_safe(self):
        courier,assignment=self.picked()
        self.command("deliver");task=self.task("delivery");self.process(task)
        self.assertEqual((self.detail()["state"],ActiveSlot.query.count()),("DELIVERED",0))
        self.assertTrue(self.read_courier(courier["courier_id"])["eligible"])
        self.process(task)
        self.assertEqual((OrderEvent.query.count(),ActiveSlot.query.count()),(8,0))

    def test_busy_offline_courier_keeps_assignment_until_delivery(self):
        courier,assignment,task=self.assigned()
        value=self.presence(courier["courier_id"],False,False)
        self.assertEqual((value["state"],value["occupied"]),("RECONCILIATION_REQUIRED",True))
        self.command("pickup",expected=409)
        restored=self.presence(courier["courier_id"],True,False)
        self.assertFalse(restored["eligible"])
        self.command("pickup");self.process(self.task("pickup"))
        self.command("deliver");self.process(self.task("delivery"))
        self.assertFalse(self.read_courier(courier["courier_id"])["eligible"])

    def test_stale_busy_presence_does_not_free_physical_capacity(self):
        courier,assignment,task=self.assigned()
        self.now+=timedelta(seconds=31)
        value=self.read_courier(courier["courier_id"])
        self.assertEqual((value["state"],value["active_assignment_id"]),("RECONCILIATION_REQUIRED",assignment["assignment_id"]))
        self.assertEqual(ActiveSlot.query.count(),1)

    def test_queued_handoff_waits_for_refreshed_presence(self):
        courier,assignment,task=self.assigned();self.command("pickup")
        self.now+=timedelta(seconds=31)
        self.assertEqual(policy.gate(db.session,self.order_id,"pickup",self.now),"courier_presence_required")
        self.assertEqual(self.process(self.task("pickup")).get_json()["task_outcome"],"deferred")
        self.post("/couriers/"+courier["courier_id"]+"/heartbeat",{},expected=200)
        self.assertEqual(self.process(self.task("pickup")).status_code,200)
        self.assertEqual(self.detail()["state"],"PICKED_UP")

    def test_presence_version_and_case_sensitive_keys_prevent_stale_control_changes(self):
        courier=self.courier();path="/couriers/"+courier["courier_id"]+"/presence"
        body={"expected_version":courier["version"],"online":False,"accepting_work":False}
        first=self.post(path,body,"MixedCase-presence",200)
        self.assertEqual(self.post(path,body,"MixedCase-presence",200).get_json(),first.get_json())
        self.assertEqual(self.post(path,body,"mixedcase-presence").status_code,409)

    def test_duplicate_handoff_requests_replay_even_after_worker_progress(self):
        self.assigned();key=str(uuid.uuid4());version=self.detail()["order_version"]
        first=self.command("pickup",key=key,version=version)
        self.process(self.task("pickup"))
        self.assertEqual(self.command("pickup",key=key,version=version).get_json(),first.get_json())
        self.command("pickup",expected=409)
        self.assertEqual(Command.query.count(),1)

    def test_wrong_generation_token_quorum_and_expiry_cannot_reserve_capacity(self):
        self.courier();self.dispatch();task=self.task("courier_assignment")
        for change in ({"generation":2},{"lease_token":str(uuid.uuid4())}):
            self.assertEqual(self.process((task[0],{**task[1],**change})).status_code,409)
        self.coordination.live=False;self.assertEqual(self.process(task).status_code,503)
        self.coordination.live=True;self.now+=timedelta(seconds=9)
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual((Assignment.query.count(),ActiveSlot.query.count()),(0,0))

    def test_lock_wait_expiry_prevents_assignment_effect(self):
        self.courier();self.dispatch();task=self.task("courier_assignment");original=self.store.fleet
        def delayed():
            value=original();self.now+=timedelta(seconds=9);return value
        with patch.object(self.store,"fleet",side_effect=delayed):
            self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(Assignment.query.count(),0)

    def test_assignment_commit_failure_rolls_back_order_slot_and_events(self):
        self.courier();self.dispatch();task=self.task("courier_assignment")
        with patch.object(db.session,"commit",side_effect=SQLAlchemyError("dummy-private-sql")):
            response=self.process(task)
        self.assertEqual(response.status_code,503);self.assertNotIn("dummy-private-sql",response.get_data(as_text=True))
        self.assertEqual((Assignment.query.count(),ActiveSlot.query.count(),self.detail()["state"]),(0,0,"WAITING"))
        self.assertEqual(self.process(task).status_code,200)

    def test_delivery_commit_failure_preserves_occupied_slot_until_retry(self):
        self.picked();self.command("deliver");task=self.task("delivery")
        with patch.object(db.session,"commit",side_effect=SQLAlchemyError("dummy-private-sql")):
            self.assertEqual(self.process(task).status_code,503)
        self.assertEqual((self.detail()["state"],ActiveSlot.query.count()),("PICKED_UP",1))
        self.process(task)
        self.assertEqual((self.detail()["state"],ActiveSlot.query.count()),("DELIVERED",0))

    def test_zone_isolation_prevents_borrowing_another_fleet(self):
        self.post("/delivery/zones",{"zone":"other","scenario":"manual"},expected=201)
        self.courier(zone="other");self.dispatch()
        self.assertEqual(self.detail()["wait_reason"],"courier_unavailable")

    def test_least_recently_assigned_courier_is_preferred(self):
        a=self.courier("A");self.now+=timedelta(seconds=1);b=self.courier("B");self.dispatch()
        self.process(self.task("courier_assignment"));self.assertEqual(self.detail()["assignment"]["courier_id"],a["courier_id"])
        self.command("pickup");self.process(self.task("pickup"));self.command("deliver");self.process(self.task("delivery"))
        self.order_id=self.seed_order();self.dispatch();self.process(self.task("courier_assignment"))
        self.assertEqual(self.detail()["assignment"]["courier_id"],b["courier_id"])

    def test_capacity_counts_are_exclusive_and_public_history_omits_private_fields(self):
        courier,assignment,task=self.assigned()
        self.courier("B");self.courier("C",online=False)
        capacity=self.client.get("/delivery/capacity?zone=demo").get_json()
        self.assertEqual((capacity["total"],sum(capacity["counts"].values()),capacity["counts"]["ASSIGNED"]),(3,3,1))
        response=self.client.get("/delivery/events?zone=demo&limit=1")
        self.assertIsNotNone(response.get_json()["next_after"])
        text=json.dumps(self.detail())+json.dumps(capacity)+response.get_data(as_text=True)
        for value in ("Private synthetic street","dummy-private-phone",task[1]["lease_token"],"claimed_by"):
            self.assertNotIn(value,text)
        self.assertEqual(response.headers["Cache-Control"],"no-store")

    def test_managed_ready_order_cannot_bypass_assignment_ledger(self):
        from order_service.event_producer import create_app as order_app
        from order_fixtures import DemoDependencies
        app=order_app({**self.config,"AUTO_CREATE_SCHEMA":False},dependencies=DemoDependencies())
        response=app.test_client().post("/orders/"+self.order_id+"/transitions",
            json={"expected_version":5,"to_state":"COURIER_ASSIGNED"},headers={"Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(response.status_code,409)
        with app.app_context():db.session.remove();db.engine.dispose()

    def test_invalid_inputs_public_mode_and_old_assignment_endpoint_fail_closed(self):
        self.assertEqual(self.post("/assign",{"order_id":self.order_id,"driver_id":1}).status_code,410)
        self.assertEqual(self.post("/couriers",{"name":"A","zone":"demo","phone":"private"}).status_code,400)
        self.assertEqual(self.client.get("/delivery/capacity").status_code,400)
        self.assertEqual(self.client.get("/delivery/events?zone=demo&limit=0").status_code,400)
        with self.assertRaises(RuntimeError):create_app({"APP_ENV":"production"},coordination=self.coordination)

    def test_relay_failure_keeps_stable_event_for_retry(self):
        class Producer:
            def __init__(self,fail):self.fail=fail;self.payloads=[]
            def produce(self,**kwargs):
                self.payloads.append(json.loads(kwargs["value"]));kwargs["on_delivery"]("dummy" if self.fail else None,None)
            def flush(self,timeout):return 0
        with patch.object(outbox,"now",side_effect=lambda:self.now):
            bad=Producer(True);self.assertEqual(outbox.dispatch_once(bad),"retry")
            event=Event.query.order_by(Event.id).first();event_id=event.event_id
            self.assertIsNone(event.published_at);self.now+=timedelta(seconds=3)
            good=Producer(False);self.assertEqual(outbox.dispatch_once(good),"published")
            self.assertEqual(good.payloads[0]["event_id"],event_id)

    def test_relay_expired_owner_cannot_ack_reclaimed_event(self):
        with patch.object(outbox,"now",side_effect=lambda:self.now):
            old=outbox.claim_next();self.now+=timedelta(seconds=31);new=outbox.claim_next()
            self.assertEqual(old["event_id"],new["event_id"])
            self.assertFalse(outbox.finish(old,True));self.assertTrue(outbox.finish(new,True))

    def test_relay_does_not_publish_past_an_unfinished_earlier_event(self):
        self.courier()
        with patch.object(outbox,"now",side_effect=lambda:self.now):
            first=outbox.claim_next();self.assertIsNone(outbox.claim_next())
            outbox.finish(first,True)
            second=outbox.claim_next();self.assertNotEqual(first["event_id"],second["event_id"])
