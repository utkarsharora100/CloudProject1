"""Notification identity, scoping, canonical source, transport failures and lease recovery."""
from datetime import datetime, timedelta
import json
import tempfile
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import create_engine, select
from werkzeug.exceptions import Conflict
import test_payment_service as payment_helpers
from test_task_scheduler import FakeCoordination
from order_service.models import Order, OrderEvent, Outbox, db
from order_service.events import append_event
from task_scheduler.models import Task, db as scheduler_db
from notification_service.api import create_app
from notification_service.ingest import consume_record, reconcile_order
from notification_service.models import Message, Attempt, TransportReceipt, Subscription, StreamReceipt
from notification_service.provider import SimulatedTransport
from notification_service import policy

class NotificationTests(unittest.TestCase):
    task=payment_helpers.PaymentTests.task
    def setUp(self):
        self.now=datetime(2026,1,1)
        self.directory=tempfile.TemporaryDirectory()
        self.provider_engine=create_engine("sqlite://")
        TransportReceipt.__table__.create(self.provider_engine)
        self.transport=SimulatedTransport(self.provider_engine)
        self.coordination=FakeCoordination()
        self.worker=next(iter(self.coordination.registrations))
        self.app=create_app({"TESTING":True,"APP_ENV":"local","AUTO_CREATE_SCHEMA":True,
            "SQLALCHEMY_DATABASE_URI":"sqlite:///"+self.directory.name+"/notifications.db"},
            coordination=self.coordination,transport=self.transport,clock=lambda:self.now)
        self.context=self.app.app_context();self.context.push()
        scheduler_db.metadata.create_all(bind=db.engine)
        self.client=self.app.test_client();self.store=self.app.extensions["notification_store"]
        order=Order(id=str(uuid.uuid4()),customer_id=1,address_id=1,restaurant_id="demo",
            total_minor=12999,currency="INR",status="PENDING_PAYMENT",version=1,
            delivery_snapshot={"line1":"Synthetic private address","phone":"private-phone"},
            restaurant_snapshot={"estimated_prep_time_mins":2},items_snapshot=[],created_at=self.now,updated_at=self.now)
        db.session.add(order);db.session.flush();append_event(order,"OrderCreated",None);db.session.commit()
        self.order_id=order.id;self.path="/notifications/orders/"+order.id+"/subscription"

    def tearDown(self):
        db.session.remove();db.engine.dispose();self.context.pop();self.provider_engine.dispose();self.directory.cleanup()

    def post(self,path,body,key=None):
        return self.client.post(path,json=body,headers={"Idempotency-Key":key or str(uuid.uuid4())})

    def subscribe(self,scenario="success",**extra):
        return self.post(self.path,{"customer_id":1,"scenario":scenario,**extra})

    def message(self):
        return db.session.execute(select(Message).where(Message.order_id==self.order_id).order_by(Message.sequence)).scalars().first()

    def bind(self,message=None):
        row=message or self.message()
        task=self.task("notification")
        row.task_id=task[0];db.session.commit()
        return task

    def process(self,task):
        return self.client.post("/internal/tasks/"+task[0]+"/process",json=task[1])

    def provider_count(self):
        with self.provider_engine.connect() as c:
            return len(c.execute(select(TransportReceipt.__table__)).all())

    def event(self,state,event_type):
        order=db.session.get(Order,self.order_id);previous=order.status
        order.status=state;order.version+=1;order.updated_at=self.now
        append_event(order,event_type,previous);db.session.commit()

    def payload(self):
        row=db.session.execute(select(Outbox).where(Outbox.order_id==self.order_id).order_by(Outbox.sequence.desc())).scalars().first()
        return json.dumps(row.payload).encode()

    def test_subscription_requires_matching_customer_and_discards_no_private_fields(self):
        self.assertEqual(self.post(self.path,{"customer_id":2}).status_code,404)
        for field in ("email","phone","address","api_key"):
            self.assertEqual(self.subscribe(**{field:"synthetic-private"}).status_code,400)
        self.assertEqual(self.subscribe().status_code,201)

    def test_subscription_idempotency_and_immutable_policy(self):
        key=str(uuid.uuid4());body={"customer_id":1,"scenario":"success"}
        first=self.post(self.path,body,key);second=self.post(self.path,body,key)
        self.assertEqual(first.get_json(),second.get_json())
        self.assertEqual(second.headers["Idempotency-Replayed"],"true")
        self.assertEqual(self.post(self.path,{**body,"scenario":"always_fail"},key).status_code,409)
        self.assertEqual(self.subscribe("always_fail").status_code,409)

    def test_no_implicit_subscription_for_old_orders(self):
        consume_record("orders",0,1,self.payload(),now=self.now)
        self.assertIsNone(self.message())

    def test_subscribe_backfills_before_consumer_and_repeated_reconcile_deduplicates(self):
        self.event("PAID","OrderPaymentSucceeded")
        self.assertEqual(self.subscribe().status_code,201)
        for _ in range(3):reconcile_order(self.order_id,now=self.now)
        self.assertEqual(db.session.query(Message).count(),2)

    def test_duplicate_offsets_and_event_republication_create_one_message(self):
        self.subscribe()
        for offset in (1,1,2,3):consume_record("orders",0,offset,self.payload(),now=self.now)
        self.assertEqual(db.session.query(Message).count(),1)
        self.assertEqual(db.session.query(StreamReceipt).count(),3)

    def test_out_of_order_event_backfills_canonical_prior_stages(self):
        self.subscribe();self.event("PAID","OrderPaymentSucceeded");self.event("KITCHEN_ACCEPTED","OrderKitchenAccepted")
        consume_record("orders",0,8,self.payload(),now=self.now)
        self.assertEqual([x.stage for x in db.session.execute(select(Message).order_by(Message.sequence)).scalars()],
            ["PENDING_PAYMENT","PAID","KITCHEN_ACCEPTED"])

    def test_poison_is_metadata_only_and_next_record_progresses(self):
        raw=b'{"password":"synthetic-sensitive",'
        self.assertEqual(consume_record("orders",1,1,raw,now=self.now),"QUARANTINED")
        self.subscribe()
        self.assertEqual(consume_record("orders",1,2,self.payload(),now=self.now),"MATERIALIZED")
        row=db.session.get(StreamReceipt,("orders",1,1))
        self.assertEqual(row.reason,"invalid_json")
        self.assertNotIn("synthetic-sensitive",str(row.__dict__))

    def test_forged_source_fields_cannot_make_notifications(self):
        self.subscribe()
        payload=json.loads(self.payload());payload["customer_id"]=2
        self.assertEqual(consume_record("orders",0,1,json.dumps(payload).encode(),now=self.now),"QUARANTINED")
        self.assertEqual(db.session.query(Message).count(),1)

    def test_extra_kafka_payload_fields_never_reach_messages(self):
        self.subscribe()
        payload=json.loads(self.payload());payload["password"]="synthetic-sensitive"
        consume_record("orders",0,1,json.dumps(payload).encode(),now=self.now)
        result=self.client.get("/notifications?customer_id=1").get_data(as_text=True)
        for value in ("Synthetic private address","private-phone","synthetic-sensitive"):
            self.assertNotIn(value,result)

    def test_invalid_json_schema_and_oversize_are_quarantined(self):
        for offset,raw in enumerate((b"null",b"[]",b"bad",b"a"*65537,b'{"schema_version":99}')):
            self.assertEqual(consume_record("orders",0,offset,raw,now=self.now),"QUARANTINED")

    def test_recipient_scope_and_cursor_do_not_leak_messages(self):
        self.subscribe();value=self.message().id
        self.assertEqual(self.client.get("/notifications/"+value+"?customer_id=2").status_code,404)
        self.assertEqual(self.client.get("/notifications/"+value+"/attempts?customer_id=2").status_code,404)
        self.assertEqual(self.client.get("/notifications?customer_id=2").get_json()["notifications"],[])
        self.assertEqual(self.client.get("/notifications?customer_id=2&after="+value).status_code,404)
        self.assertEqual(self.client.get(self.path+"?customer_id=2").status_code,404)

    def test_unscoped_or_malformed_queries_rejected(self):
        for query in ("","?customer_id=1&courier_id="+str(uuid.uuid4()),"?customer_id=0","?customer_id=1&limit=51",
                      "?customer_id=1&token=value","?customer_id=1&state=FANTASY"):
            self.assertEqual(self.client.get("/notifications"+query).status_code,400)

    def test_success_has_one_receipt_and_safe_attempt_history(self):
        self.subscribe();task=self.bind()
        first=self.process(task);self.assertEqual(first.status_code,200)
        self.assertEqual(first.get_json()["notification"]["state"],"SENT")
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(self.provider_count(),1)
        message=self.message();self.assertEqual(message.attempts,1)
        history=self.client.get("/notifications/"+message.id+"/attempts?customer_id=1").get_json()
        self.assertEqual(history["attempts"][0]["outcome"],"SENT")
        self.assertNotIn("lease_token",json.dumps(history))

    def test_transient_failure_retries_same_message_after_backoff(self):
        self.subscribe("fail_once");task=self.bind()
        first=self.process(task).get_json()["notification"]
        self.assertEqual((first["state"],first["round_attempts"]),("RETRY_WAIT",1))
        self.process(task);self.assertEqual(self.message().attempts,1)
        self.now+=timedelta(seconds=3)
        self.assertEqual(self.process(task).get_json()["notification"]["state"],"SENT")
        self.assertEqual((self.message().attempts,self.provider_count()),(2,1))

    def test_response_loss_recovers_without_duplicate_or_extra_attempt(self):
        self.subscribe("response_loss_once");task=self.bind()
        self.assertEqual(self.process(task).status_code,503)
        self.assertEqual(self.message().state,"SENDING")
        self.assertEqual(self.provider_count(),1)
        result=self.process(task)
        self.assertEqual(result.get_json()["notification"]["state"],"SENT")
        self.assertEqual((self.message().attempts,self.provider_count()),(1,1))
        self.assertEqual(db.session.get(Attempt,(self.message().id,1)).outcome,"RECEIPT_RECOVERED")

    def test_stale_generation_has_no_transport_effect(self):
        self.subscribe();task=self.bind();body={**task[1],"generation":2}
        response=self.client.post("/internal/tasks/"+task[0]+"/process",json=body)
        self.assertEqual(response.status_code,409);self.assertEqual(self.provider_count(),0)

    def test_wrong_token_and_disconnected_session_have_no_effect(self):
        self.subscribe();task=self.bind()
        response=self.client.post("/internal/tasks/"+task[0]+"/process",json={**task[1],"lease_token":str(uuid.uuid4())})
        self.assertEqual(response.status_code,409)
        self.coordination.live=False
        self.assertIn(self.process(task).status_code,(409,503))
        self.assertEqual(self.provider_count(),0)

    def test_durable_reservation_resumes_after_worker_replacement(self):
        self.subscribe();task=self.bind()
        with patch.object(self.store,"finish_send",side_effect=RuntimeError("simulated worker crash")):
            self.assertEqual(self.process(task).status_code,500)
        self.assertEqual(self.message().state,"SENDING")
        replacement=self.task("notification",task_id=task[0],generation=2)
        self.assertEqual(self.process(task).status_code,409)
        self.assertEqual(self.message().attempts,1)
        self.assertEqual(self.process(replacement).status_code,200)
        self.assertEqual(self.provider_count(),1)

    def test_unconfirmed_cannot_be_marked_read(self):
        self.subscribe();row=self.message()
        self.assertEqual(self.post("/notifications/"+row.id+"/read",
            {"recipient_type":"customer","recipient_id":1,"expected_version":row.version}).status_code,409)

    def test_read_is_versioned_idempotent_and_does_not_resend(self):
        self.subscribe();self.process(self.bind());row=self.message()
        path="/notifications/"+row.id+"/read";key=str(uuid.uuid4())
        body={"recipient_type":"customer","recipient_id":1,"expected_version":row.version}
        first=self.post(path,body,key);second=self.post(path,body,key)
        self.assertEqual(first.get_json(),second.get_json())
        self.assertFalse(first.get_json()["unread"]);self.assertEqual(self.provider_count(),1)
        self.assertEqual(self.post(path,body).status_code,409)

    def fail_message(self):
        self.subscribe("always_fail");task=self.bind()
        for _ in range(3):
            self.process(task);self.now+=timedelta(seconds=7)
            # A live lease is renewed as time moves, preserving ownership proof.
            db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0])
                .values(lease_until=self.now+timedelta(seconds=8)));db.session.commit()
        return self.message(),task

    def test_permanent_failure_is_bounded_and_explicit_retry_keeps_identity(self):
        row,task=self.fail_message()
        self.assertEqual((row.state,row.attempts,self.provider_count()),("FAILED",3,0))
        value=row.id
        response=self.post("/notifications/"+value+"/retry",{"recipient_type":"customer",
            "recipient_id":1,"expected_version":row.version,"scenario":"success"})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.process(task).get_json()["notification"]["state"],"SENT")
        self.assertEqual((self.message().id,self.message().retry_round,self.message().attempts),(value,2,4))

    def test_retry_wrong_recipient_or_success_rejected(self):
        self.subscribe();self.process(self.bind());row=self.message()
        body={"recipient_type":"customer","recipient_id":2,"expected_version":row.version}
        self.assertEqual(self.post("/notifications/"+row.id+"/retry",body).status_code,404)
        body["recipient_id"]=1
        self.assertEqual(self.post("/notifications/"+row.id+"/retry",body).status_code,409)

    def test_pending_earlier_stage_blocks_later_send(self):
        self.subscribe();self.event("PAID","OrderPaymentSucceeded");reconcile_order(self.order_id,now=self.now)
        rows=db.session.execute(select(Message).order_by(Message.sequence)).scalars().all()
        later_task=self.bind(rows[1])
        self.assertEqual(self.process(later_task).get_json()["task_outcome"],"deferred")
        self.assertEqual(self.provider_count(),0)
        self.process(self.bind(rows[0]))
        self.assertEqual(self.process(later_task).get_json()["notification"]["state"],"SENT")

    def test_failed_earlier_message_does_not_block_later_progress(self):
        self.fail_message()
        self.event("PAID","OrderPaymentSucceeded");reconcile_order(self.order_id,now=self.now)
        later=db.session.execute(select(Message).where(Message.stage=="PAID")).scalar_one()
        later.scenario="success";db.session.commit()
        self.assertEqual(self.process(self.bind(later)).get_json()["notification"]["state"],"SENT")

    def test_destination_rechecks_ownership_before_committing_confirmation(self):
        self.subscribe();task=self.bind()
        real=self.transport.deliver
        def lose(*args):
            result=real(*args);self.coordination.live=False;return result
        with patch.object(self.transport,"deliver",side_effect=lose):
            self.assertIn(self.process(task).status_code,(409,503))
        self.assertEqual(self.message().state,"SENDING")
        self.assertEqual(self.provider_count(),1)
        self.coordination.live=True
        self.assertEqual(self.process(task).status_code,200)
        self.assertEqual(self.provider_count(),1)

    def test_transport_rejects_changed_payload_for_same_logical_id(self):
        value=str(uuid.uuid4())
        self.transport.deliver(value,"a"*64,self.now)
        with self.assertRaises(Conflict):self.transport.deliver(value,"b"*64,self.now)
        self.assertEqual(self.provider_count(),1)

    def test_default_clock_uses_notification_database_session(self):
        from notification_service.store import NotificationStore
        self.assertIsInstance(NotificationStore(self.coordination).clock(),datetime)

    def test_policy_requires_bound_message_and_honest_receipt(self):
        self.subscribe();task=self.bind()
        self.assertIsNone(policy.gate(db.session,task[0],self.now))
        self.assertEqual(policy.gate(db.session,str(uuid.uuid4()),self.now),"notification_event_required")
        row=policy.task_message(db.session,task[0])
        self.assertIsNone(policy.confirmed(db.session,row))

    def test_notifications_never_change_business_order_or_outbox(self):
        self.subscribe("response_loss_once");before=(db.session.get(Order,self.order_id).version,db.session.query(OrderEvent).count(),db.session.query(Outbox).count())
        task=self.bind();self.process(task);self.process(task)
        after=(db.session.get(Order,self.order_id).version,db.session.query(OrderEvent).count(),db.session.query(Outbox).count())
        self.assertEqual(before,after)

    def courier_event(self):
        from delivery_assignment_service.models import Zone,Courier,Job,Assignment,Event
        zone=Zone(id="notification-test-zone",scenario="manual",created_at=self.now)
        courier=Courier(id=str(uuid.uuid4()),zone=zone.id,name="Synthetic courier")
        job=Job(order_id=self.order_id,zone=zone.id,state="ASSIGNED",source_event_id=str(uuid.uuid4()),ready_at=self.now)
        db.session.add_all([zone,courier,job]);db.session.flush()
        assignment=Assignment(id=str(uuid.uuid4()),order_id=self.order_id,courier_id=courier.id,state="ASSIGNED",
            task_id=str(uuid.uuid4()),generation=1)
        db.session.add(assignment);db.session.flush()
        event=Event(zone=zone.id,order_id=self.order_id,courier_id=courier.id,assignment_id=assignment.id,
            event_type="CourierAssigned",data={},occurred_at=self.now)
        db.session.add(event);db.session.commit()
        return event

    def test_delivery_topic_creates_only_courier_messages_and_deduplicates(self):
        self.subscribe();event=self.courier_event()
        payload=json.dumps({"schema_version":2,"event_id":event.event_id,"event_type":event.event_type,
            "order_id":event.order_id,"courier_id":event.courier_id,"assignment_id":event.assignment_id}).encode()
        for offset in (1,2):consume_record("delivery_assignments",0,offset,payload,now=self.now)
        rows=db.session.execute(select(Message)).scalars().all()
        self.assertEqual(len(rows),2)
        self.assertEqual(sum(row.recipient_type=="customer" for row in rows),1)
        target=next(row for row in rows if row.recipient_type=="courier")
        self.assertEqual(target.recipient_id,event.courier_id)
        self.assertEqual(target.source_topic,"delivery_assignments")

    def test_courier_opt_out_does_not_materialize_courier_messages(self):
        self.subscribe(courier_updates=False);self.courier_event();reconcile_order(self.order_id,now=self.now)
        self.assertEqual(db.session.query(Message).count(),1)

    def test_mismatched_delivery_assignment_never_routes_a_courier_message(self):
        self.subscribe();event=self.courier_event()
        event.courier_id=str(uuid.uuid4());db.session.commit()
        reconcile_order(self.order_id,now=self.now)
        self.assertEqual(db.session.query(Message).count(),1)

    def scheduler_app(self):
        from task_scheduler.storage import create_storage_app as scheduler_app
        return scheduler_app({"TESTING":True,"APP_ENV":"local","AUTO_CREATE_SCHEMA":True,
            "SQLALCHEMY_DATABASE_URI":"sqlite:///"+self.directory.name+"/notifications.db"})

    def test_scheduler_rejects_worker_success_without_notification_receipt(self):
        from task_scheduler.store import TaskStore
        self.subscribe();task=self.bind()
        with self.scheduler_app().app_context():
            with self.assertRaises(Conflict):
                TaskStore(self.coordination,clock=lambda:self.now).complete(task[0],{
                    **task[1],"outcome":"succeeded","retry_after":2})
            scheduler_db.session.rollback()

    def test_scheduler_success_cites_the_committed_notification_receipt(self):
        from task_scheduler.store import TaskStore
        self.subscribe();task=self.bind();self.process(task)
        row=self.message()
        db.session.add(TransportReceipt(notification_id=row.id,payload_digest="a"*64,delivered_at=self.now))
        db.session.commit()
        source_event_id=row.source_event_id
        with self.scheduler_app().app_context():
            result,_=TaskStore(self.coordination,clock=lambda:self.now).complete(task[0],{
                **task[1],"outcome":"succeeded","retry_after":2})
            self.assertEqual(result["state"],"SUCCEEDED")
            actual=scheduler_db.session.get(Task,task[0])
            self.assertEqual(actual.completion_source,"notification_receipt")
            self.assertEqual(actual.proof_event_id,source_event_id)
            self.assertEqual(actual.confirmed_at,self.now)
            scheduler_db.session.remove()

    def test_pagination_retains_stable_source_order_without_leaking_scope(self):
        self.subscribe();self.event("PAID","OrderPaymentSucceeded");reconcile_order(self.order_id,now=self.now)
        first=self.client.get("/notifications?customer_id=1&limit=1").get_json()
        second=self.client.get("/notifications?customer_id=1&limit=1&after="+first["next_after"]).get_json()
        self.assertEqual(first["notifications"][0]["stage"],"PENDING_PAYMENT")
        self.assertEqual(second["notifications"][0]["stage"],"PAID")
        self.assertIsNone(second["next_after"])

    def test_worker_budget_exhaustion_becomes_explicitly_retryable(self):
        from notification_service.ingest import recover_exhausted_tasks
        self.subscribe();task=self.bind()
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(state="FAILED"))
        db.session.commit()
        recover_exhausted_tasks(now=self.now)
        row=self.message()
        self.assertEqual((row.state,row.last_error_code),("FAILED","worker_budget_exhausted"))
        response=self.post("/notifications/"+row.id+"/retry",{"recipient_type":"customer",
            "recipient_id":1,"expected_version":row.version})
        self.assertEqual(response.status_code,200)

    def test_exhausted_worker_recovers_committed_receipt_without_redelivery(self):
        from notification_service.ingest import recover_exhausted_tasks
        from notification_service.domain import content_digest
        self.subscribe("response_loss_once");task=self.bind();self.process(task)
        row=self.message()
        db.session.add(TransportReceipt(notification_id=row.id,payload_digest=content_digest(row),delivered_at=self.now))
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(state="FAILED"))
        db.session.commit()
        recover_exhausted_tasks(now=self.now)
        self.assertEqual((self.message().state,self.message().attempts,self.provider_count()),("SENT",1,1))
        self.assertEqual(db.session.get(Attempt,(row.id,1)).outcome,"RECEIPT_RECOVERED")

    def test_mismatched_receipt_never_confirms_exhausted_message(self):
        from notification_service.ingest import recover_exhausted_tasks
        self.subscribe();task=self.bind();row=self.message()
        db.session.add(TransportReceipt(notification_id=row.id,payload_digest="z"*64,delivered_at=self.now))
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(state="FAILED"))
        db.session.commit();recover_exhausted_tasks(now=self.now)
        self.assertEqual(self.message().state,"FAILED")
        self.assertIsNone(self.message().sent_at)

    def test_kitchen_bridge_quarantines_malformed_shared_topic_records(self):
        from unittest.mock import MagicMock
        from order_service.event_consumer import KitchenBridge
        database=MagicMock();bridge=KitchenBridge(database,MagicMock())
        self.assertFalse(bridge.process_record("orders",0,123,b'{"secret":"synthetic-private",'))
        record=database.collection.return_value.document.return_value.set.call_args.args[0]
        self.assertEqual(record["reason"],"invalid_json")
        self.assertNotIn("synthetic-private",json.dumps(record))
        self.assertEqual(record["offset"],123)

    def test_kitchen_quarantine_failure_does_not_acknowledge_bad_record(self):
        from unittest.mock import MagicMock
        from order_service.event_consumer import KitchenBridge
        database=MagicMock()
        database.collection.return_value.document.return_value.set.side_effect=RuntimeError("simulated storage outage")
        with self.assertRaises(RuntimeError):
            KitchenBridge(database,MagicMock()).process_record("orders",0,123,b"bad")

    def test_kitchen_record_wrapper_still_processes_valid_paid_source(self):
        from unittest.mock import MagicMock
        from order_service.event_consumer import KitchenBridge
        bridge=KitchenBridge(MagicMock(),MagicMock())
        event={"schema_version":2,"event_type":"OrderPaymentSucceeded","status":"PAID"}
        with patch.object(bridge,"process",return_value=True) as process:
            self.assertTrue(bridge.process_record("orders",0,124,json.dumps(event).encode()))
            process.assert_called_once_with(event)

    def test_late_subscription_reactivates_a_previously_skipped_notification(self):
        from task_scheduler.store import TaskStore
        self.subscribe();task=self.bind()
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(
            state="SKIPPED",reason="prerequisite_failed",owner_id=None,owner_name=None,owner_session=None,
            lease_until=None,completed_at=self.now))
        db.session.commit()
        with self.scheduler_app().app_context():
            store=TaskStore(self.coordination,clock=lambda:self.now)
            leader_id=str(uuid.uuid4());epoch=store.begin_term(leader_id,"notification-test")
            self.coordination.current_leader={"epoch":epoch,"leader_id":leader_id}
            workflow_id=scheduler_db.session.get(Task,task[0]).workflow_id
            store.reconcile(workflow_id,epoch,leader_id,[])
            actual=scheduler_db.session.get(Task,task[0])
            self.assertEqual(actual.state,"READY")
            self.assertIsNone(actual.completed_at)
            self.assertIsNone(actual.completion_source)
            scheduler_db.session.remove()

    def test_background_recovery_does_not_fail_a_new_unacknowledged_retry_round(self):
        from notification_service.ingest import recover_exhausted_tasks
        row,task=self.fail_message()
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(
            state="FAILED",reason="notification_failure",result={"retry_round":1}))
        db.session.commit()
        response=self.post("/notifications/"+row.id+"/retry",{"recipient_type":"customer",
            "recipient_id":1,"expected_version":row.version})
        self.assertEqual(response.status_code,200)
        recover_exhausted_tasks(now=self.now)
        self.assertEqual((self.message().state,self.message().retry_round),("PENDING",2))

    def test_scheduler_acknowledges_manual_retry_once_and_keeps_worker_budget_bounded(self):
        from task_scheduler.store import TaskStore
        from notification_service.ingest import recover_exhausted_tasks
        row,task=self.fail_message()
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(
            state="FAILED",reason="notification_failure",result={"retry_round":1},
            owner_id=None,owner_name=None,owner_session=None,lease_until=None))
        db.session.commit()
        response=self.post("/notifications/"+row.id+"/retry",{"recipient_type":"customer",
            "recipient_id":1,"expected_version":row.version})
        self.assertEqual(response.status_code,200)
        app=self.scheduler_app()
        with app.app_context():
            store=TaskStore(self.coordination,clock=lambda:self.now)
            leader_id=str(uuid.uuid4());epoch=store.begin_term(leader_id,"notification-test")
            self.coordination.current_leader={"epoch":epoch,"leader_id":leader_id}
            workflow=scheduler_db.session.get(Task,task[0]).workflow_id
            store.reconcile(workflow,epoch,leader_id,[])
            actual=scheduler_db.session.get(Task,task[0])
            self.assertEqual(actual.state,"READY")
            self.assertEqual(actual.result["retry_round"],2)
            actual.state="FAILED";actual.reason="worker_lost";scheduler_db.session.commit()
            store.reconcile(workflow,epoch,leader_id,[])
            self.assertEqual(scheduler_db.session.get(Task,task[0]).state,"FAILED")
            scheduler_db.session.remove()
        recover_exhausted_tasks(now=self.now)
        self.assertEqual(self.message().state,"FAILED")
        self.assertEqual(self.message().last_error_code,"worker_budget_exhausted")

    def test_retry_acknowledges_new_round_before_old_worker_completion_becomes_terminal(self):
        from task_scheduler.store import TaskStore
        row,task=self.fail_message()
        response=self.post("/notifications/"+row.id+"/retry",{"recipient_type":"customer",
            "recipient_id":1,"expected_version":row.version})
        self.assertEqual(response.status_code,200)
        db.session.execute(Task.__table__.update().where(Task.__table__.c.id==task[0]).values(
            state="RETRY_WAIT",reason="resource_wait",result={"retry_round":1},
            owner_id=None,owner_name=None,owner_session=None,lease_until=None))
        db.session.commit()
        with self.scheduler_app().app_context():
            store=TaskStore(self.coordination,clock=lambda:self.now)
            leader_id=str(uuid.uuid4());epoch=store.begin_term(leader_id,"notification-test")
            self.coordination.current_leader={"epoch":epoch,"leader_id":leader_id}
            workflow=scheduler_db.session.get(Task,task[0]).workflow_id
            store.reconcile(workflow,epoch,leader_id,[])
            actual=scheduler_db.session.get(Task,task[0])
            self.assertEqual(actual.result["retry_round"],2)
            actual.state="FAILED";actual.reason="worker_lost";scheduler_db.session.commit()
            store.reconcile(workflow,epoch,leader_id,[])
            self.assertEqual(scheduler_db.session.get(Task,task[0]).state,"FAILED")
            scheduler_db.session.remove()

if __name__=="__main__":
    unittest.main()
