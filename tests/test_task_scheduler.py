from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import Conflict, ServiceUnavailable
from order_service.models import Order, OrderEvent
from task_scheduler.api import create_app
from task_scheduler.domain import default_graph
from task_scheduler.models import Attempt, Dependency, LeaderState, Task, TaskEvent, Workflow, db


class FakeCoordination:
    def __init__(self):
        self.live = True
        self.current_leader = None
        self.registrations = {str(uuid.uuid4()): {"name": name, "capabilities": ["demo"], "session": str(index + 100)}
                              for index, name in enumerate(("demo-a", "demo-b"))}
        self.leases = {}

    def guard(self):
        if not self.live:
            raise ServiceUnavailable("Coordination unavailable.")

    def leader(self):
        self.guard()
        return self.current_leader

    def verify_leader(self, epoch, leader_id, *, own=False):
        self.guard()
        if self.current_leader != {"epoch": epoch, "leader_id": leader_id}:
            raise Conflict("Stale leader.")

    def workers(self):
        self.guard()
        return [{"worker_id": key, **value} for key, value in self.registrations.items()]

    def worker(self, worker_id, session=None):
        self.guard()
        worker = self.registrations.get(worker_id)
        if worker is None or (session is not None and worker["session"] != session):
            raise Conflict("Worker ownership changed.")
        return {"worker_id": worker_id, **worker}

    def verify_lease(self, task_id, generation, worker_id, session, token):
        self.worker(worker_id, session)
        if self.leases.get((task_id, generation)) != (worker_id, session, token):
            raise Conflict("Worker lease changed.")


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1)
        self.coordination = FakeCoordination()
        self.app = create_app({"TESTING": True, "APP_ENV": "local", "SQLALCHEMY_DATABASE_URI": "sqlite://",
                               "AUTO_CREATE_SCHEMA": True}, coordination=self.coordination, clock=lambda: self.now)
        self.context = self.app.app_context()
        self.context.push()
        self.client = self.app.test_client()
        self.store = self.app.extensions["task_store"]
        self.leader_id = str(uuid.uuid4())
        self.epoch = self.store.begin_term(self.leader_id, "scheduler-a")
        self.coordination.current_leader = {"epoch": self.epoch, "leader_id": self.leader_id}

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.context.pop()

    def create(self, tasks=None, *, order_id=None, key=None):
        body = {"tasks": tasks if tasks is not None else [{"key": "job", "kind": "demo"}]}
        if order_id:
            body["order_id"] = order_id
        response = self.client.post("/workflows", json=body, headers={"Idempotency-Key": key or str(uuid.uuid4())})
        self.assertEqual(response.status_code, 201, response.get_data(as_text=True))
        return response.get_json()

    def tick(self, workflow):
        self.store.reconcile(workflow["workflow_id"], self.epoch, self.leader_id, self.coordination.workers())

    def start(self, task_id):
        task = db.session.get(Task, task_id)
        proof = {"worker_id": task.owner_id, "generation": task.generation, "lease_token": str(uuid.uuid4())}
        self.coordination.leases[(task_id, task.generation)] = (task.owner_id, task.owner_session, proof["lease_token"])
        response = self.client.post(f"/tasks/{task_id}/lease/start", json=proof)
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        return proof

    def complete(self, task_id, proof, outcome="succeeded", **extra):
        return self.client.post(f"/tasks/{task_id}/complete", json={**proof, "outcome": outcome, **extra})

    def graph(self, workflow):
        return self.client.get(f"/workflows/{workflow['workflow_id']}/graph").get_json()

    def seed_order(self, states=None):
        states = states or ["PENDING_PAYMENT"]
        order_id = str(uuid.uuid4())
        db.session.execute(Order.__table__.insert().values(id=order_id, customer_id=1, address_id=1,
            restaurant_id="demo", total_minor=12999, currency="INR", status=states[-1], version=len(states),
            delivery_snapshot={"line1": "Synthetic test street"}, restaurant_snapshot={}, items_snapshot=[],
            created_at=self.now, updated_at=self.now))
        for sequence, state in enumerate(states, 1):
            db.session.execute(OrderEvent.__table__.insert().values(event_id=str(uuid.uuid4()), order_id=order_id,
                sequence=sequence, event_type="TestFixture", from_state=states[sequence - 2] if sequence > 1 else None,
                to_state=state, occurred_at=self.now - timedelta(seconds=20 - sequence)))
        db.session.commit()
        return order_id

    def test_graph_replay_and_case_distinct_keys_are_safe(self):
        first = self.create(key="CaseSensitive-Key")
        repeated = self.create(key="CaseSensitive-Key")
        self.assertEqual(first, repeated)
        other = self.create(key="casesensitive-key")
        self.assertNotEqual(first["workflow_id"], other["workflow_id"])
        response = self.client.post("/workflows", json={"tasks": [{"key": "different", "kind": "demo"}]},
                                    headers={"Idempotency-Key": "CaseSensitive-Key"})
        self.assertEqual(response.status_code, 409)

    def test_invalid_dags_and_untrusted_payloads_leave_no_partial_records(self):
        invalid = [
            {"tasks": [{"key": "a", "kind": "demo", "depends_on": ["a"]}]},
            {"tasks": [{"key": "a", "kind": "demo", "depends_on": ["missing"]}]},
            {"tasks": [{"key": "a", "kind": "demo", "depends_on": ["b"]}, {"key": "b", "kind": "demo", "depends_on": ["a"]}]},
            {"tasks": [{"key": "a", "kind": []}]}, {"tasks": [{"key": "a", "kind": "demo", "depends_on": [{}]}]},
            {"tasks": [{"key": "a", "kind": "demo", "params": {"command": "not-allowed"}}]},
            {"tasks": [{"key": "a", "kind": "payment"}]}, {"tasks": [{"key": "a", "kind": "demo", "params": {"duration_ms": True}}]},
        ]
        for body in invalid:
            response = self.client.post("/workflows", json=body, headers={"Idempotency-Key": str(uuid.uuid4())})
            self.assertEqual(response.status_code, 400)
        self.assertEqual([Workflow.query.count(), Task.query.count(), Dependency.query.count()], [0, 0, 0])

    def test_graph_exposes_real_and_dependencies_and_waiting_reasons(self):
        workflow = self.create([{"key": "a", "kind": "demo"}, {"key": "b", "kind": "demo"},
                                {"key": "join", "kind": "demo", "depends_on": ["a", "b"]}])
        self.tick(workflow)
        nodes = {node["key"]: node for node in self.graph(workflow)["nodes"]}
        self.assertEqual(nodes["join"]["state"], "BLOCKED")
        self.assertEqual(nodes["join"]["reason_code"], "dependencies")
        self.assertEqual(len(nodes["join"]["prerequisites"]), 2)
        self.assertEqual(nodes["join"]["prerequisites_complete"], 0)

    def test_and_join_runs_only_after_both_prerequisites_succeed(self):
        workflow = self.create([{"key": "a", "kind": "demo"}, {"key": "b", "kind": "demo"},
                                {"key": "join", "kind": "demo", "depends_on": ["a", "b"]}])
        self.tick(workflow)
        a, b, join = (workflow["tasks"][name] for name in ("a", "b", "join"))
        pa, pb = self.start(a), self.start(b)
        self.assertEqual(self.complete(a, pa).status_code, 200)
        self.tick(workflow)
        self.assertEqual(db.session.get(Task, join).state, "BLOCKED")
        self.assertEqual(self.complete(b, pb).status_code, 200)
        self.tick(workflow)
        self.assertEqual(db.session.get(Task, join).state, "ASSIGNED")

    def test_worker_capacity_is_enforced_across_workflows(self):
        graphs = [self.create() for _ in range(3)]
        for graph in graphs:
            self.tick(graph)
        states = [db.session.get(Task, graph["tasks"]["job"]).state for graph in graphs]
        self.assertEqual(states.count("ASSIGNED"), 2)
        self.assertEqual(states.count("READY"), 1)

    def seed_payment(self, order_id):
        from payment_service.models import Payment
        db.session.execute(Payment.__table__.insert().values(id=str(uuid.uuid4()), order_id=order_id,
            amount_minor=12999, currency="INR", scenario="success", refund_scenario="success",
            state="PENDING", attempt_number=1, version=1, created_at=self.now, updated_at=self.now))
        db.session.commit()

    def test_worker_without_required_capability_is_not_assigned(self):
        order_id = self.seed_order()
        self.seed_payment(order_id)
        workflow = self.create([{"key": "pay", "kind": "payment"}], order_id=order_id)
        self.tick(workflow)
        task = db.session.get(Task, workflow["tasks"]["pay"])
        self.assertEqual((task.state, task.reason, task.attempts), ("READY", "worker_unavailable", 0))

    def test_start_replay_does_not_create_another_attempt(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        response = self.client.post(f"/tasks/{task_id}/lease/start", json=proof)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(Attempt.query.count(), 1)
        self.assertEqual(db.session.get(Task, task_id).attempts, 1)

    def test_completion_replay_works_after_ephemeral_lease_cleanup(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        first = self.complete(task_id, proof)
        del self.coordination.leases[(task_id, proof["generation"])]
        repeated = self.complete(task_id, proof)
        self.assertEqual(repeated.status_code, 200)
        self.assertEqual(repeated.get_json(), first.get_json())
        self.assertEqual(repeated.headers["Idempotency-Replayed"], "true")
        self.assertEqual(self.complete(task_id, proof, "failed").status_code, 409)

    def test_expired_lease_cannot_renew_or_complete(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        self.now += timedelta(seconds=9)
        self.assertEqual(self.client.post(f"/tasks/{task_id}/lease/renew", json=proof).status_code, 409)
        self.assertEqual(self.complete(task_id, proof).status_code, 409)
        self.assertIsNone(db.session.get(Task, task_id).result)

    def test_reassignment_fences_a_still_connected_old_worker(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        old = self.start(task_id)
        self.now += timedelta(seconds=9)
        self.tick(workflow)
        self.now += timedelta(seconds=2)
        self.tick(workflow)
        task = db.session.get(Task, task_id)
        self.assertNotEqual(task.owner_id, old["worker_id"])
        self.assertGreater(task.generation, old["generation"])
        self.assertEqual(self.complete(task_id, old).status_code, 409)
        new = self.start(task_id)
        self.assertEqual(self.complete(task_id, new).status_code, 200)
        self.assertEqual(db.session.get(Task, task_id).result["generation"], new["generation"])

    def test_missing_worker_registration_reclaims_unexpired_work(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        del self.coordination.registrations[proof["worker_id"]]
        self.tick(workflow)
        task = db.session.get(Task, task_id)
        self.assertEqual((task.state, task.reason), ("RETRY_WAIT", "worker_lost"))
        self.assertEqual(db.session.get(Attempt, (task_id, proof["generation"])).outcome, "ABANDONED")

    def test_wrong_token_and_changed_worker_session_are_rejected(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        self.assertEqual(self.complete(task_id, {**proof, "lease_token": str(uuid.uuid4())}).status_code, 409)
        self.coordination.registrations[proof["worker_id"]]["session"] = "new-session"
        self.assertEqual(self.complete(task_id, proof).status_code, 409)

    def test_renewal_extends_only_a_live_owned_lease(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        first = db.session.get(Task, task_id).lease_until
        self.now += timedelta(seconds=2)
        self.assertEqual(self.client.post(f"/tasks/{task_id}/lease/renew", json=proof).status_code, 200)
        self.assertGreater(db.session.get(Task, task_id).lease_until, first)

    def test_retry_backoff_and_budget_preserve_attempt_history(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        first = self.start(task_id)
        self.assertEqual(self.complete(task_id, first, "failed").status_code, 200)
        self.tick(workflow)
        self.assertEqual(db.session.get(Task, task_id).state, "RETRY_WAIT")
        self.now += timedelta(seconds=3)
        self.tick(workflow)
        second = self.start(task_id)
        self.assertEqual(self.complete(task_id, second).status_code, 200)
        task = db.session.get(Task, task_id)
        self.assertEqual((task.state, task.attempts, task.failures), ("SUCCEEDED", 2, 1))

    def test_resource_deferral_does_not_consume_failure_budget(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        self.assertEqual(self.complete(task_id, proof, "deferred", retry_after=5).status_code, 200)
        task = db.session.get(Task, task_id)
        self.assertEqual((task.state, task.reason, task.failures), ("RETRY_WAIT", "resource_wait", 0))

    def test_permanent_failure_skips_descendants_but_not_parallel_work(self):
        workflow = self.create([{"key": "a", "kind": "demo", "max_failures": 1},
            {"key": "child", "kind": "demo", "depends_on": ["a"]}, {"key": "parallel", "kind": "demo"}])
        self.tick(workflow)
        task_id = workflow["tasks"]["a"]
        proof = self.start(task_id)
        self.complete(task_id, proof, "failed")
        self.tick(workflow)
        self.assertEqual(db.session.get(Task, task_id).state, "FAILED")
        self.assertEqual(db.session.get(Task, workflow["tasks"]["child"]).state, "SKIPPED")
        self.assertEqual(db.session.get(Task, workflow["tasks"]["parallel"]).state, "ASSIGNED")

    def test_assignment_timeout_without_start_does_not_count_as_worker_failure(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        self.now += timedelta(seconds=9)
        self.tick(workflow)
        task = db.session.get(Task, task_id)
        self.assertEqual((task.state, task.attempts, task.failures), ("RETRY_WAIT", 0, 0))

    def test_old_leader_epoch_cannot_reconcile_or_start_old_assignments(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        old_epoch, old_leader = self.epoch, self.leader_id
        self.leader_id = str(uuid.uuid4())
        self.epoch = self.store.begin_term(self.leader_id, "scheduler-b")
        self.coordination.current_leader = {"epoch": self.epoch, "leader_id": self.leader_id}
        with self.assertRaises(Conflict):
            self.store.reconcile(workflow["workflow_id"], old_epoch, old_leader, self.coordination.workers())
        db.session.rollback()
        task = db.session.get(Task, task_id)
        proof = {"worker_id": task.owner_id, "generation": task.generation, "lease_token": str(uuid.uuid4())}
        self.assertEqual(self.client.post(f"/tasks/{task_id}/lease/start", json=proof).status_code, 409)

    def test_running_worker_survives_scheduler_leader_change(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        new_leader = str(uuid.uuid4())
        new_epoch = self.store.begin_term(new_leader, "scheduler-b")
        self.coordination.current_leader = {"epoch": new_epoch, "leader_id": new_leader}
        self.assertEqual(self.client.post(f"/tasks/{task_id}/lease/renew", json=proof).status_code, 200)
        self.assertEqual(self.complete(task_id, proof).status_code, 200)

    def test_quorum_loss_blocks_lease_changes_but_allows_reading_graph(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        self.coordination.live = False
        self.assertEqual(self.client.post(f"/tasks/{task_id}/lease/renew", json=proof).status_code, 503)
        self.assertEqual(self.complete(task_id, proof).status_code, 503)
        self.assertEqual(self.client.get(f"/workflows/{workflow['workflow_id']}/graph").status_code, 200)
        self.assertEqual(self.client.get("/health").status_code, 503)
        self.assertEqual(db.session.get(Task, task_id).state, "RUNNING")

    def test_order_graph_is_materialized_once_with_payment_gate_and_pickup_join(self):
        order_id = self.seed_order()
        self.store.materialize_orders(self.epoch, self.leader_id)
        self.store.materialize_orders(self.epoch, self.leader_id)
        row = Workflow.query.filter_by(managed_order_id=order_id).one()
        workflow = self.store.creation_response(row)
        self.tick(workflow)
        nodes = {node["key"]: node for node in self.graph(workflow)["nodes"]}
        self.assertEqual((Workflow.query.count(), len(nodes)), (1, 10))
        self.assertEqual(nodes["payment"]["state"], "READY")
        self.assertEqual(nodes["kitchen_accept"]["state"], "BLOCKED")
        self.assertEqual({parent["key"] for parent in nodes["pickup"]["prerequisites"]}, {"prepare", "assign"})
        self.assertEqual(nodes["refund"]["reason_code"], "dependencies")

    def test_authoritative_order_history_confirms_business_milestones_without_fake_attempts(self):
        states = ["PENDING_PAYMENT", "PAID", "KITCHEN_ACCEPTED", "PREPARING", "READY", "COURIER_ASSIGNED", "PICKED_UP", "DELIVERED"]
        order_id = self.seed_order(states)
        definition = default_graph(order_id)
        response, _ = self.store.create_graph(definition, "f" * 64, managed=True)
        self.tick(response)
        nodes = {node["key"]: node for node in self.graph(response)["nodes"]}
        for name in ("payment", "kitchen_accept", "prepare", "assign", "pickup", "deliver"):
            self.assertEqual(nodes[name]["state"], "SUCCEEDED")
            self.assertEqual(nodes[name]["completion_source"], "order_history")
            self.assertEqual(nodes[name]["attempt_count"], 0)
            self.assertIsNotNone(nodes[name]["confirmed_event_id"])
        self.assertEqual(nodes["refund"]["state"], "SKIPPED")
        self.assertEqual(nodes["notify_paid"]["state"], "READY")
        self.assertEqual(db.session.execute(select(Order.__table__.c.status).where(Order.__table__.c.id == order_id)).scalar_one(), "DELIVERED")

    def test_paid_cancellation_enables_refund_and_cancels_future_kitchen_work(self):
        order_id = self.seed_order(["PENDING_PAYMENT", "PAID", "REFUND_PENDING"])
        response, _ = self.store.create_graph(default_graph(order_id), "a" * 64, managed=True)
        self.tick(response)
        nodes = {node["key"]: node for node in self.graph(response)["nodes"]}
        self.assertEqual(nodes["payment"]["state"], "SUCCEEDED")
        self.assertEqual(nodes["refund"]["state"], "READY")
        self.assertEqual(nodes["kitchen_accept"]["state"], "CANCELLED")
        self.assertEqual(nodes["deliver"]["state"], "CANCELLED")

    def test_a_worker_cannot_fake_a_payment_outcome(self):
        order_id = self.seed_order()
        self.seed_payment(order_id)
        for value in self.coordination.registrations.values():
            value["capabilities"] = ["payment"]
        workflow = self.create([{"key": "pay", "kind": "payment"}], order_id=order_id)
        self.tick(workflow)
        task_id = workflow["tasks"]["pay"]
        proof = self.start(task_id)
        self.assertEqual(self.complete(task_id, proof).status_code, 409)
        status = db.session.execute(select(Order.__table__.c.status).where(Order.__table__.c.id == order_id)).scalar_one()
        self.assertEqual(status, "PENDING_PAYMENT")
        self.assertIsNone(db.session.get(Task, task_id).result)

    def test_graph_and_logs_never_expose_lease_proofs(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        text = json.dumps(self.graph(workflow))
        self.assertNotIn(proof["lease_token"], text)
        self.assertNotIn("lease_token", text)
        self.assertNotIn("owner_session", text)
        response = self.client.post(f"/tasks/{task_id}/complete", json={**proof, "outcome": []})
        self.assertEqual(response.status_code, 400)
        self.assertNotIn(proof["lease_token"], response.get_data(as_text=True))

    def test_history_is_persisted_and_paginated(self):
        workflow = self.create()
        self.tick(workflow)
        task_id = workflow["tasks"]["job"]
        proof = self.start(task_id)
        self.complete(task_id, proof)
        first = self.client.get(f"/tasks/{task_id}/events?limit=1").get_json()
        self.assertEqual(len(first["events"]), 1)
        self.assertIsNotNone(first["next_after"])
        following = self.client.get(f"/tasks/{task_id}/events?after={first['next_after']}").get_json()
        self.assertEqual(following["events"][-1]["to_state"], "SUCCEEDED")

    def test_database_errors_are_redacted(self):
        private = "dummy-private-scheduler-detail"
        with patch.object(db.session, "execute", side_effect=SQLAlchemyError(private)):
            with self.assertLogs(self.app.logger, level="ERROR") as logs:
                response = self.client.get("/health")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(private, response.get_data(as_text=True))
        self.assertNotIn(private, " ".join(logs.output))

    def test_public_mode_and_nonlocal_coordination_are_rejected(self):
        from task_scheduler.coordination import Coordination
        from task_scheduler.storage import create_storage_app
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            create_storage_app({"SQLALCHEMY_DATABASE_URI": "sqlite://"})
        with self.assertRaises(RuntimeError):
            Coordination(environ={"APP_ENV": "local", "ZK_HOSTS": "remote.invalid:2181"})


if __name__ == "__main__":
    unittest.main()
