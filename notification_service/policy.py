"""Scheduler integration reads notification proofs and never changes business state."""
from sqlalchemy import select
from notification_service.domain import DEFAULT_KEYS
from notification_service.models import Message, TransportReceipt
from notification_service.store import earlier_pending
from task_scheduler.domain import digest, graph_request, ACTIVE
from task_scheduler.models import Task, Workflow, db

def materialize_tasks(store, epoch, leader_id):
    table=Message.__table__
    db.session.rollback()
    rows=db.session.execute(select(table).where(table.c.task_id.is_(None))
        .order_by(table.c.created_at,table.c.sequence,table.c.id).limit(30)).mappings().all()
    db.session.rollback()
    for message in rows:
        default_key=DEFAULT_KEYS.get(message["stage"]) if message["recipient_type"]=="customer" else None
        task_id=workflow_id=None
        if default_key:
            row=db.session.execute(select(Task.id,Task.workflow_id).join(Workflow,Workflow.id==Task.workflow_id)
                .where(Workflow.managed_order_id==message["order_id"],Task.key==default_key)).first()
            db.session.rollback()
            if row:task_id,workflow_id=row
            else:continue
        if task_id is None:
            definition=graph_request({"order_id":message["order_id"],"tasks":[
                {"key":"notify","kind":"notification","label":"Send "+message["stage"].lower().replace("_"," ")}]})
            result,_=store.create_graph(definition,digest({"notification":message["id"]}),term=(epoch,leader_id))
            task_id,workflow_id=result["tasks"]["notify"],result["workflow_id"]
        store.term(epoch,leader_id,own=True)
        db.session.execute(table.update().where(table.c.id==message["id"],table.c.task_id.is_(None))
            .values(task_id=task_id,workflow_id=workflow_id))
        store.coordination.guard();db.session.commit()

def task_message(session, task_id, *, lock=False):
    table=Message.__table__
    query=select(table).where(table.c.task_id==task_id)
    if lock:query=query.with_for_update()
    return session.execute(query).mappings().one_or_none()

def confirmed(session, message):
    if not message or message["state"]!="SENT":return None
    table=TransportReceipt.__table__
    return session.execute(select(table.c.delivered_at).where(
        table.c.notification_id==message["id"])).scalar_one_or_none()

def gate(session, task_id, now):
    message=task_message(session,task_id)
    if not message:return "notification_event_required"
    if message["state"]=="FAILED":return "notification_failed"
    if message["available_at"]>now:return "notification_backoff"
    # Core row is wrapped for the common ordering predicate.
    from types import SimpleNamespace
    if earlier_pending(session,SimpleNamespace(**message)):return "notification_ordering"
    return None

def reconcile(store, task, now, loads):
    from task_scheduler.store import clear_owner, record_state
    message=task_message(db.session,task.id,lock=True)
    if not message:return False
    receipt=confirmed(db.session,message)
    if receipt is not None or message["state"]=="FAILED":
        state="SUCCEEDED" if receipt is not None else "FAILED"
        if task.state!=state:
            store._finish_attempt(task,"CONFIRMED_BY_NOTIFICATION" if receipt else "NOTIFICATION_FAILED",now)
            if task.owner_id in loads:loads[task.owner_id]-=1
            if task.state in ACTIVE:task.generation+=1
            clear_owner(task)
            task.completion_source="notification_receipt" if receipt else None
            task.proof_event_id=message["source_event_id"] if receipt else None
            task.confirmed_at=receipt;task.completed_at=now
            task.result={"notification_id":message["id"],"simulation":True,"retry_round":message["retry_round"]}
            record_state(task,state,"notification_confirmed" if receipt else "notification_failure",now)
        task.result={"notification_id":message["id"],"simulation":True,"retry_round":message["retry_round"]}
        return True
    if task.state=="SKIPPED" and task.reason=="prerequisite_failed" and message["state"]=="PENDING":
        task.completed_at=None;task.available_at=now
        record_state(task,"RETRY_WAIT","notification_subscription_enabled",now)
    acknowledged_round=(task.result or {}).get("retry_round",1)
    if message["retry_round"]>acknowledged_round:
        task.failures=0
        task.result={"notification_id":message["id"],"simulation":True,"retry_round":message["retry_round"]}
        if task.state=="FAILED":
            task.completed_at=None;task.available_at=now
            record_state(task,"RETRY_WAIT","notification_retry_requested",now)
    return False
