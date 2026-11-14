"""Fence destination writes with a locked scheduler generation and ZK proof."""
import hmac
from sqlalchemy import select
from werkzeug.exceptions import Conflict
from task_scheduler.models import Task, Attempt, Workflow

def lock_task(session, coordination, task_id, proof, clock, *, kinds=frozenset({'payment', 'refund'})):
    table = Task.__table__
    task = session.execute(select(table).where(table.c.id == task_id).with_for_update()).mappings().one_or_none()
    now = clock()
    if (task is None or task["kind"] not in kinds or task["state"] != "RUNNING"
            or task["generation"] != proof["generation"] or task["owner_id"] != proof["worker_id"]
            or task["lease_until"] is None or task["lease_until"] <= now):
        raise Conflict("Task ownership is expired or changed.")
    attempt = Attempt.__table__
    token = session.execute(select(attempt.c.lease_token).where(
        attempt.c.task_id == task_id, attempt.c.generation == proof["generation"])).scalar_one_or_none()
    if token is None or not hmac.compare_digest(token, proof["lease_token"]):
        raise Conflict("Task proof is invalid.")
    coordination.verify_lease(task_id, task["generation"], task["owner_id"], task["owner_session"], proof["lease_token"])
    workflow = Workflow.__table__
    order_id = session.execute(select(workflow.c.order_id).where(workflow.c.id == task["workflow_id"])).scalar_one()
    if order_id is None:
        raise Conflict("Tasks require an order.")
    return dict(task), order_id

def check_live(task, coordination, proof, now):
    if task["lease_until"] <= now:
        raise Conflict("Task lease expired before confirmation.")
    coordination.verify_lease(task["id"], task["generation"], task["owner_id"], task["owner_session"], proof["lease_token"])
    coordination.guard()
