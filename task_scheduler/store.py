"""Transactional scheduler state, dependency eligibility and fenced lease mutations."""
from copy import deepcopy
from datetime import timedelta
from graphlib import TopologicalSorter
import hmac
import uuid
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import Conflict, NotFound, ServiceUnavailable
from order_service.models import Order, OrderEvent, timestamp
from task_scheduler.domain import ACTIVE, BUSINESS_PROOFS, TERMINAL, default_graph, digest
from task_scheduler.models import Attempt, Dependency, LeaderState, Task, TaskEvent, Workflow, db
from task_scheduler.storage import database_now
from payment_service.models import Payment, Refund
from kitchen_workflow.models import Command as KitchenCommand
from delivery_assignment_service.policy import gate as delivery_gate
from notification_service import policy as notification_policy


ORDER = Order.__table__
ORDER_EVENT = OrderEvent.__table__
LEASE_SECONDS = 8
ASSIGNMENT_SECONDS = 8
TERM_SECONDS = 6


def stamp(value):
    return timestamp(value) if value is not None else None


def public_task(task):
    return {"task_id": task.id, "workflow_id": task.workflow_id, "key": task.key,
            "label": task.label, "kind": task.kind, "condition": task.condition,
            "state": task.state, "reason_code": task.reason, "generation": task.generation,
            "revision": task.revision, "attempt_count": task.attempts, "failure_count": task.failures,
            "max_failures": task.max_failures, "params": task.params,
            "owner": {"worker_id": task.owner_id, "name": task.owner_name} if task.owner_id else None,
            "lease_until": stamp(task.lease_until), "available_at": stamp(task.available_at),
            "completion_source": task.completion_source, "confirmed_event_id": task.proof_event_id,
            "confirmed_at": stamp(task.confirmed_at), "created_at": stamp(task.created_at),
            "updated_at": stamp(task.updated_at), "ready_at": stamp(task.ready_at),
            "started_at": stamp(task.started_at), "completed_at": stamp(task.completed_at),
            "result": task.result}


def record_state(task, state, reason, now):
    old = task.state
    task.state, task.reason, task.updated_at = state, reason, now
    task.revision += 1
    db.session.add(TaskEvent(workflow_id=task.workflow_id, task_id=task.id,
        from_state=old, to_state=state, reason=reason, generation=task.generation, occurred_at=now))


def clear_owner(task):
    task.last_worker_id = task.owner_id or task.last_worker_id
    task.owner_id = task.owner_name = task.owner_session = None
    task.assignment_until = task.lease_until = None


class TaskStore:
    def __init__(self, coordination, *, clock=None):
        self.coordination = coordination
        self.clock = clock or database_now

    def task(self, task_id, *, lock=False):
        query = select(Task).where(Task.id == task_id)
        if lock:
            query = query.with_for_update()
        result = db.session.execute(query).scalar_one_or_none()
        if result is None:
            raise NotFound("Task not found.")
        return result

    def begin_term(self, leader_id, name):
        db.session.rollback()
        self.coordination.guard()
        row = db.session.execute(select(LeaderState).where(LeaderState.id == 1).with_for_update()).scalar_one()
        now = self.clock()
        row.epoch += 1
        row.leader_id, row.leader_name = leader_id, name
        row.heartbeat_at, row.expires_at = now, now + timedelta(seconds=TERM_SECONDS)
        epoch = row.epoch
        self.coordination.guard()
        db.session.commit()
        return epoch

    def term(self, epoch, leader_id, *, own=False):
        row = db.session.execute(select(LeaderState).where(LeaderState.id == 1).with_for_update()).scalar_one()
        if row.epoch != epoch or row.leader_id != leader_id:
            raise Conflict("Scheduler epoch is stale.")
        self.coordination.verify_leader(epoch, leader_id, own=own)
        return row

    def pulse(self, epoch, leader_id):
        db.session.rollback()
        row = self.term(epoch, leader_id, own=True)
        now = self.clock()
        row.heartbeat_at, row.expires_at = now, now + timedelta(seconds=TERM_SECONDS)
        self.coordination.guard()
        db.session.commit()

    def create_graph(self, definition, request_digest, *, managed=False, term=None):
        db.session.rollback()
        if term:
            self.term(*term, own=True)
        previous = db.session.execute(select(Workflow).where(Workflow.request_digest == request_digest)).scalar_one_or_none()
        request_hash = digest(definition)
        if previous is not None:
            if previous.request_hash != request_hash:
                raise Conflict("This workflow key was used with a different definition.")
            response = self.creation_response(previous)
            db.session.rollback()
            return response, True
        if definition["order_id"] is not None:
            exists = db.session.execute(select(ORDER.c.id).where(ORDER.c.id == definition["order_id"])).first()
            if exists is None:
                raise NotFound("Order not found.")
        now = self.clock()
        workflow = Workflow(id=str(uuid.uuid5(uuid.NAMESPACE_OID, "scheduler:" + request_digest)),
            request_digest=request_digest, request_hash=request_hash, order_id=definition["order_id"],
            managed_order_id=definition["order_id"] if managed else None, created_at=now)
        try:
            db.session.add(workflow)
            db.session.flush()
            tasks = {}
            for node in definition["tasks"]:
                task = Task(id=str(uuid.uuid4()), workflow_id=workflow.id, key=node["key"], label=node["label"],
                    kind=node["kind"], condition=node["when"], params=deepcopy(node["params"]),
                    state="BLOCKED", reason="awaiting_scheduler", generation=0, revision=1,
                    attempts=0, failures=0, max_failures=node["max_failures"], available_at=now,
                    created_at=now, updated_at=now)
                db.session.add(task)
                tasks[node["key"]] = task
            db.session.flush()
            for node in definition["tasks"]:
                task = tasks[node["key"]]
                for parent in node["depends_on"]:
                    db.session.add(Dependency(task_id=task.id, prerequisite_id=tasks[parent].id))
                db.session.add(TaskEvent(workflow_id=workflow.id, task_id=task.id, from_state=None,
                    to_state="BLOCKED", reason="created", generation=0, occurred_at=now))
            response = {"workflow_id": workflow.id, "order_id": workflow.order_id,
                        "tasks": {key: task.id for key, task in tasks.items()}}
            if term:
                self.coordination.guard()
            db.session.commit()
            return response, False
        except IntegrityError:
            db.session.rollback()
            previous = db.session.execute(select(Workflow).where(Workflow.request_digest == request_digest)).scalar_one_or_none()
            if previous is None or previous.request_hash != request_hash:
                raise Conflict("Workflow creation conflicts with an existing definition.") from None
            return self.creation_response(previous), True

    def creation_response(self, workflow):
        tasks = db.session.execute(select(Task).where(Task.workflow_id == workflow.id)).scalars().all()
        return {"workflow_id": workflow.id, "order_id": workflow.order_id,
                "tasks": {task.key: task.id for task in tasks}}

    def materialize_orders(self, epoch, leader_id):
        db.session.rollback()
        ids = db.session.execute(select(ORDER.c.id).outerjoin(Workflow, Workflow.managed_order_id == ORDER.c.id)
            .where(Workflow.id.is_(None)).order_by(ORDER.c.created_at, ORDER.c.id).limit(20)).scalars().all()
        db.session.rollback()
        for order_id in ids:
            self.create_graph(default_graph(order_id), digest({"managed_order": order_id}),
                              managed=True, term=(epoch, leader_id))

    def order_context(self, order_id, *, lock=False):
        if order_id is None:
            return None, {}
        query = select(ORDER.c.id, ORDER.c.status).where(ORDER.c.id == order_id)
        if lock:
            query = query.with_for_update()
        order = db.session.execute(query).mappings().one_or_none()
        if order is None:
            raise Conflict("Workflow order is unavailable.")
        records = db.session.execute(select(ORDER_EVENT.c.event_id, ORDER_EVENT.c.to_state, ORDER_EVENT.c.occurred_at)
            .where(ORDER_EVENT.c.order_id == order_id).order_by(ORDER_EVENT.c.sequence)).mappings().all()
        return order, {record["to_state"]: record for record in records}

    def _finish_attempt(self, task, outcome, now):
        attempt = db.session.get(Attempt, (task.id, task.generation))
        if attempt is not None and attempt.outcome == "RUNNING":
            attempt.outcome, attempt.finished_at = outcome, now

    def _abandon(self, task, reason, now):
        running = task.state == "RUNNING"
        self._finish_attempt(task, "ABANDONED", now)
        if running:
            task.failures += 1
        clear_owner(task)
        task.available_at = now + timedelta(seconds=1)
        state = "FAILED" if task.failures >= task.max_failures else "RETRY_WAIT"
        record_state(task, state, reason, now)
        if state == "FAILED":
            task.completed_at = now

    def reconcile(self, workflow_id, epoch, leader_id, workers):
        db.session.rollback()
        leader = self.term(epoch, leader_id, own=True)
        now = self.clock()
        workflow = db.session.get(Workflow, workflow_id)
        # Lock only one workflow per transaction. Task -> Order lock ordering is
        # shared by completion, preventing cross-workflow/order lock cycles.
        tasks = db.session.execute(select(Task).where(Task.workflow_id == workflow_id)
            .order_by(Task.id).with_for_update()).scalars().all()
        if workflow is None:
            db.session.rollback()
            return
        indexed = {task.id: task for task in tasks}
        parents = {task.id: [] for task in tasks}
        if tasks:
            edges = db.session.execute(select(Dependency).where(Dependency.task_id.in_(indexed))).scalars().all()
            for edge in edges:
                parents[edge.task_id].append(edge.prerequisite_id)
        order, proofs = self.order_context(workflow.order_id, lock=True)
        available_workers = {worker["worker_id"]: worker for worker in workers}
        loads = {worker_id: 0 for worker_id in available_workers}
        active_rows = db.session.execute(select(Task.owner_id).where(Task.state.in_(ACTIVE))).scalars().all()
        for owner in active_rows:
            if owner in loads:
                loads[owner] += 1
        for task_id in TopologicalSorter(parents).static_order():
            task = indexed[task_id]
            prerequisites = [indexed[parent] for parent in parents[task_id]]
            satisfied = all(parent.state == "SUCCEEDED" for parent in prerequisites)
            condition_active = task.condition == "always" or "REFUND_PENDING" in proofs
            proof = proofs.get(BUSINESS_PROOFS.get(task.kind))
            if proof is not None and satisfied and condition_active and task.state != "SUCCEEDED":
                self._finish_attempt(task, "CONFIRMED_BY_ORDER", now)
                if task.owner_id in loads:
                    loads[task.owner_id] -= 1
                if task.state in ACTIVE:
                    task.generation += 1
                clear_owner(task)
                task.completion_source, task.proof_event_id = "order_history", proof["event_id"]
                task.confirmed_at, task.completed_at = proof["occurred_at"], now
                record_state(task, "SUCCEEDED", "order_confirmed", now)
                continue
            if task.kind == "notification" and notification_policy.reconcile(self, task, now, loads):
                continue
            if task.state in TERMINAL:
                continue
            if (task.condition == "refund_requested" and not condition_active
                    and order and order["status"] in {"CANCELLED", "DELIVERED", "REFUNDED"}):
                clear_owner(task)
                record_state(task, "SKIPPED", "branch_not_taken", now)
                task.completed_at = now
                continue
            cancelled = order and order["status"] in {"CANCELLED", "REFUND_PENDING", "REFUNDED"}
            if cancelled and task.kind in BUSINESS_PROOFS and task.kind != "refund":
                self._finish_attempt(task, "CANCELLED", now)
                if task.state in ACTIVE:
                    task.generation += 1
                if task.owner_id in loads:
                    loads[task.owner_id] -= 1
                clear_owner(task)
                record_state(task, "CANCELLED", "order_cancelled", now)
                task.completed_at = now
                continue
            if task.state in ACTIVE:
                worker = available_workers.get(task.owner_id)
                expired = (task.state == "ASSIGNED" and (task.assignment_until <= now or task.assignment_epoch != epoch)) or (task.state == "RUNNING" and task.lease_until <= now)
                gone = worker is None or worker["session"] != task.owner_session
                if expired or gone:
                    if task.owner_id in loads:
                        loads[task.owner_id] -= 1
                    self._abandon(task, "worker_lost" if gone else "lease_expired", now)
                continue
            if any(parent.state in {"FAILED", "CANCELLED", "SKIPPED"} for parent in prerequisites):
                record_state(task, "SKIPPED", "prerequisite_failed", now)
                task.completed_at = now
                continue
            if not satisfied or not condition_active:
                reason = "dependencies" if not satisfied else "branch_condition"
                if task.state != "BLOCKED" or task.reason != reason:
                    record_state(task, "BLOCKED", reason, now)
                continue
            if task.available_at > now:
                if task.state != "RETRY_WAIT":
                    record_state(task, "RETRY_WAIT", "backoff", now)
                continue
            if task.state != "READY":
                task.ready_at = task.ready_at or now
                record_state(task, "READY", "worker_unavailable", now)
            gate = None
            if task.kind in {"payment", "refund"}:
                table = Payment.__table__
                payment = db.session.execute(select(table.c.id, table.c.state).where(table.c.order_id == workflow.order_id)).mappings().one_or_none()
                if task.kind == "payment" and (payment is None or payment["state"] != "PENDING"):
                    gate = "payment_retry_required" if payment and payment["state"] == "DECLINED" else "payment_request_required"
                elif task.kind == "refund":
                    refund_table = Refund.__table__
                    refund = db.session.execute(select(refund_table.c.id).where(refund_table.c.payment_id == payment["id"],
                        refund_table.c.state == "PENDING")).first() if payment else None
                    if refund is None:
                        gate = "refund_request_required"
            if task.kind in {"kitchen_accept", "preparation"}:
                actions = ["accept"] if task.kind == "kitchen_accept" else ["start_preparation", "ready"]
                table = KitchenCommand.__table__
                pending = db.session.execute(select(table.c.id).where(table.c.order_id == workflow.order_id,
                    table.c.state == "PENDING", table.c.action.in_(actions))).first()
                if pending is None:
                    gate = "kitchen_action_required"
            if task.kind in {'courier_assignment', 'pickup', 'delivery'}:
                gate = delivery_gate(db.session, workflow.order_id, task.kind, now)
            if task.kind == "notification":
                gate = notification_policy.gate(db.session, task.id, now)
            if gate:
                if task.reason != gate:
                    record_state(task, "READY", gate, now)
                continue
            candidates = [worker for worker in workers if task.kind in worker["capabilities"] and loads[worker["worker_id"]] < 1]
            alternatives = [worker for worker in candidates if worker["worker_id"] != task.last_worker_id]
            if alternatives:
                candidates = alternatives
            if candidates:
                worker = sorted(candidates, key=lambda item: (loads[item["worker_id"]], item["name"], item["worker_id"]))[0]
                task.generation += 1
                task.assignment_epoch = epoch
                task.owner_id, task.owner_name, task.owner_session = worker["worker_id"], worker["name"], worker["session"]
                task.assignment_until = now + timedelta(seconds=ASSIGNMENT_SECONDS)
                record_state(task, "ASSIGNED", "worker_selected", now)
                loads[worker["worker_id"]] += 1
        leader.heartbeat_at, leader.expires_at = now, now + timedelta(seconds=TERM_SECONDS)
        self.coordination.verify_leader(epoch, leader_id, own=True)
        self.coordination.guard()
        db.session.commit()

    def assignments(self, worker_id):
        worker = self.coordination.worker(worker_id)
        tasks = db.session.execute(select(Task).where(Task.owner_id == worker_id,
            Task.owner_session == worker["session"], Task.state == "ASSIGNED").order_by(Task.created_at)).scalars().all()
        return [{"task_id": task.id, "generation": task.generation, "kind": task.kind,
                 "params": task.params, "order_id": db.session.get(Workflow, task.workflow_id).order_id}
                for task in tasks]

    def _proof(self, task, proof, now):
        if (task.state != "RUNNING" or task.generation != proof["generation"]
                or task.owner_id != proof["worker_id"] or task.lease_until is None or task.lease_until <= now):
            raise Conflict("Task lease is expired or ownership changed.")
        attempt = db.session.get(Attempt, (task.id, task.generation))
        if attempt is None or not hmac.compare_digest(attempt.lease_token, proof["lease_token"]):
            raise Conflict("Task lease proof is invalid.")
        self.coordination.verify_lease(task.id, task.generation, task.owner_id, task.owner_session, proof["lease_token"])
        return attempt

    def start(self, task_id, proof):
        db.session.rollback()
        leader = db.session.execute(select(LeaderState).where(LeaderState.id == 1).with_for_update()).scalar_one()
        task = self.task(task_id, lock=True)
        now = self.clock()
        if task.state == "RUNNING":
            self._proof(task, proof, now)
            result = self.lease_response(task)
            db.session.rollback()
            return result
        if (task.state != "ASSIGNED" or task.generation != proof["generation"] or task.owner_id != proof["worker_id"]
                or task.assignment_until is None or task.assignment_until <= now):
            raise Conflict("Task assignment is no longer valid.")
        if task.assignment_epoch != leader.epoch or leader.expires_at is None or leader.expires_at <= now:
            raise Conflict("Scheduler assignment epoch is no longer active.")
        self.coordination.verify_leader(leader.epoch, leader.leader_id)
        self.coordination.verify_lease(task.id, task.generation, task.owner_id, task.owner_session, proof["lease_token"])
        workflow = db.session.get(Workflow, task.workflow_id)
        order, proofs = self.order_context(workflow.order_id, lock=True)
        if order and order["status"] in {"CANCELLED", "REFUND_PENDING", "REFUNDED"} and task.kind in BUSINESS_PROOFS and task.kind != "refund":
            raise Conflict("Order no longer permits this task.")
        task.attempts += 1
        task.started_at = task.started_at or now
        task.lease_until = now + timedelta(seconds=LEASE_SECONDS)
        db.session.add(Attempt(task_id=task.id, generation=task.generation, lease_token=proof["lease_token"],
            worker_id=task.owner_id, worker_name=task.owner_name, worker_session=task.owner_session,
            outcome="RUNNING", started_at=now))
        record_state(task, "RUNNING", "lease_started", now)
        self.coordination.guard()
        result = self.lease_response(task)
        db.session.commit()
        return result

    def lease_response(self, task):
        return {"task_id": task.id, "generation": task.generation, "state": task.state,
                "lease_until": stamp(task.lease_until), "attempt_number": task.attempts}

    def renew(self, task_id, proof):
        db.session.rollback()
        task = self.task(task_id, lock=True)
        now = self.clock()
        self._proof(task, proof, now)
        task.lease_until, task.updated_at = now + timedelta(seconds=LEASE_SECONDS), now
        self.coordination.guard()
        result = self.lease_response(task)
        db.session.commit()
        return result

    def complete(self, task_id, proof):
        db.session.rollback()
        task = self.task(task_id, lock=True)
        attempt = db.session.get(Attempt, (task.id, proof["generation"]))
        completion_hash = digest({"outcome": proof["outcome"], "retry_after": proof["retry_after"]})
        if attempt and attempt.response is not None and attempt.worker_id == proof["worker_id"] and hmac.compare_digest(attempt.lease_token, proof["lease_token"]):
            if attempt.response.get("completion_hash") != completion_hash:
                raise Conflict("Completion retry differs from the original result.")
            response = {key: value for key, value in attempt.response.items() if key != "completion_hash"}
            db.session.rollback()
            return response, True
        now = self.clock()
        attempt = self._proof(task, proof, now)
        if proof["outcome"] == "succeeded":
            workflow = db.session.get(Workflow, task.workflow_id)
            order, proofs = self.order_context(workflow.order_id, lock=True)
            required = BUSINESS_PROOFS.get(task.kind)
            confirmation = proofs.get(required) if required else None
            if required and confirmation is None:
                raise Conflict("The business outcome is not confirmed by Order history.")
            if task.kind == "notification":
                message = notification_policy.task_message(db.session, task.id, lock=True)
                receipt = notification_policy.confirmed(db.session, message)
                if receipt is None:
                    raise Conflict("Notification delivery has no confirmed transport receipt.")
                task.proof_event_id, task.confirmed_at = message["source_event_id"], receipt
            task.completion_source = "notification_receipt" if task.kind == "notification" else "worker"
            if confirmation:
                task.proof_event_id, task.confirmed_at = confirmation["event_id"], confirmation["occurred_at"]
            task.result = ({"notification_id": message["id"], "simulation": True, "retry_round": message["retry_round"]} if task.kind == "notification"
                           else {"processed": True, "generation": task.generation})
            task.completed_at = now
            record_state(task, "SUCCEEDED", "worker_completed", now)
        elif proof["outcome"] == "failed":
            task.failures += 1
            task.available_at = now + timedelta(seconds=min(2 ** task.failures, 30))
            state = "FAILED" if task.failures >= task.max_failures else "RETRY_WAIT"
            record_state(task, state, "worker_failure", now)
            if state == "FAILED":
                task.completed_at = now
        else:
            task.available_at = now + timedelta(seconds=proof["retry_after"])
            record_state(task, "RETRY_WAIT", "resource_wait", now)
        attempt.outcome, attempt.finished_at = proof["outcome"].upper(), now
        clear_owner(task)
        response = {"task_id": task.id, "generation": task.generation, "state": task.state,
                    "attempt_outcome": attempt.outcome}
        attempt.response = {**response, "completion_hash": completion_hash}
        self.coordination.guard()
        db.session.commit()
        return response, False

    def graph(self, workflow_id):
        workflow = db.session.get(Workflow, workflow_id)
        if workflow is None:
            raise NotFound("Workflow not found.")
        tasks = db.session.execute(select(Task).where(Task.workflow_id == workflow_id).order_by(Task.key)).scalars().all()
        indexed = {task.id: task for task in tasks}
        edges = db.session.execute(select(Dependency).where(Dependency.task_id.in_(indexed))).scalars().all() if tasks else []
        parents = {task.id: [] for task in tasks}
        for edge in edges:
            parent = indexed[edge.prerequisite_id]
            parents[edge.task_id].append({"task_id": parent.id, "key": parent.key,
                "state": parent.state, "satisfied": parent.state == "SUCCEEDED"})
        nodes = []
        now = self.clock()
        for task in tasks:
            attempts = db.session.execute(select(Attempt).where(Attempt.task_id == task.id).order_by(Attempt.generation)).scalars().all()
            nodes.append({**public_task(task), "lease_deadline_expired": bool(task.lease_until and task.lease_until <= now),
                "ready_for_assignment": task.state == "READY" and task.available_at <= now and task.reason not in {"payment_request_required", "payment_retry_required", "refund_request_required", "kitchen_action_required", "delivery_request_required", "courier_unavailable", "older_order_waiting", "assignment_reconciliation_required", "courier_action_required", "courier_presence_required", "notification_event_required", "notification_backoff", "notification_ordering", "notification_failed"}, "prerequisites": parents[task.id],
                "prerequisites_complete": sum(parent["satisfied"] for parent in parents[task.id]),
                "attempts": [{"generation": attempt.generation, "worker_id": attempt.worker_id,
                    "worker_name": attempt.worker_name, "outcome": attempt.outcome,
                    "started_at": stamp(attempt.started_at), "finished_at": stamp(attempt.finished_at)} for attempt in attempts]})
        order, _ = self.order_context(workflow.order_id)
        return {"workflow_id": workflow.id, "order_id": workflow.order_id,
                "managed": workflow.managed_order_id is not None, "coordination_available": self.coordination.live, "order_state": order["status"] if order else None,
                "created_at": stamp(workflow.created_at), "nodes": nodes,
                "edges": [{"from": edge.prerequisite_id, "to": edge.task_id, "rule": "all_succeeded"} for edge in edges]}
