"""Read-only resource eligibility reused by the scheduler and delivery destination."""
from datetime import timedelta
from sqlalchemy import select
from order_service.models import Order
from delivery_assignment_service.models import Courier, Job, ActiveSlot, Assignment, Command
from delivery_assignment_service.domain import PRESENCE_SECONDS

def available(session,zone,now):
    c,s=Courier.__table__,ActiveSlot.__table__
    return select(c).outerjoin(s,s.c.courier_id==c.c.id).where(c.c.zone==zone,c.c.online.is_(True),
        c.c.accepting_work.is_(True),c.c.last_seen_at>now-timedelta(seconds=PRESENCE_SECONDS),
        s.c.courier_id.is_(None)).order_by(c.c.last_assigned_at,c.c.created_at,c.c.id)

def head(session,zone,*,current=False):
    j,o=Job.__table__,Order.__table__
    query=select(j.c.order_id).join(o,o.c.id==j.c.order_id).where(
        j.c.zone==zone,j.c.state=="WAITING",o.c.status=="READY").order_by(
        j.c.ready_at,j.c.requested_at,j.c.order_id).limit(1)
    if current:
        query=query.with_for_update()
    return session.execute(query).scalar_one_or_none()

def fresh(courier,now):
    return bool(courier.online and courier.last_seen_at and courier.last_seen_at>now-timedelta(seconds=PRESENCE_SECONDS))

def gate(session,order_id,kind,now):
    j=Job.__table__
    job=session.execute(select(j).where(j.c.order_id==order_id)).mappings().one_or_none()
    if job is None:
        return "delivery_request_required"
    if kind=="courier_assignment":
        if job["state"]!="WAITING":
            return "assignment_reconciliation_required"
        if head(session,job["zone"])!=order_id:
            return "older_order_waiting"
        if session.execute(available(session,job["zone"],now).limit(1)).first() is None:
            return "courier_unavailable"
        return None
    action="pickup" if kind=="pickup" else "deliver"
    cmd,c,a,s=Command.__table__,Courier.__table__,Assignment.__table__,ActiveSlot.__table__
    row=session.execute(select(c.c.online,c.c.last_seen_at).select_from(cmd).join(a,a.c.id==cmd.c.assignment_id)
        .join(s,(s.c.assignment_id==a.c.id)&(s.c.order_id==cmd.c.order_id)&(s.c.courier_id==cmd.c.courier_id))
        .join(c,c.c.id==cmd.c.courier_id).where(cmd.c.order_id==order_id,cmd.c.action==action,
            cmd.c.state=="PENDING",a.c.state==("ASSIGNED" if action=="pickup" else "PICKED_UP"))).mappings().one_or_none()
    if row is None:
        return "courier_action_required"
    if not row["online"] or row["last_seen_at"] is None or row["last_seen_at"]<=now-timedelta(seconds=PRESENCE_SECONDS):
        return "courier_presence_required"
    return None
