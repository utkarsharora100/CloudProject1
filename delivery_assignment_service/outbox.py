"""Ordered at-least-once publication of durable delivery/fleet events."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import time
import uuid
from sqlalchemy import select,func
from order_service.models import db,utc_now,timestamp
from order_service.storage import create_storage_app
from order_service.outbox import confirmed_publish,create_producer
from delivery_assignment_service.models import Event

HEARTBEAT=Path("/tmp/delivery-outbox-heartbeat")
LEASE_SECONDS=30

def now():
    return db.session.execute(select(func.utc_timestamp(6))).scalar_one() if db.engine.dialect.name=="mysql" else utc_now()

def claim_next():
    db.session.rollback()
    # One globally ordered local stream keeps all order/fleet changes in sequence.
    row=db.session.execute(select(Event).where(Event.published_at.is_(None)).order_by(Event.id).limit(1).with_for_update()).scalar_one_or_none()
    current=now()
    if row is None or row.available_at>current or (row.lease_until and row.lease_until>current):
        db.session.rollback();return None
    row.claimed_by=str(uuid.uuid4());row.lease_until=current+timedelta(seconds=LEASE_SECONDS);row.attempts+=1
    result={"event_id":row.event_id,"token":row.claimed_by,"payload":{"schema_version":2,"event_id":row.event_id,
        "event_type":row.event_type,"zone":row.zone,"order_id":row.order_id,"courier_id":row.courier_id,
        "assignment_id":row.assignment_id,"timestamp":timestamp(row.occurred_at),"data":deepcopy(row.data)}}
    db.session.commit();return result

def finish(claim,published):
    db.session.rollback()
    row=db.session.execute(select(Event).where(Event.event_id==claim["event_id"]).with_for_update()).scalar_one_or_none()
    current=now()
    if row is None or row.claimed_by!=claim["token"] or row.lease_until is None or row.lease_until<=current:
        db.session.rollback();return False
    if published:row.published_at,row.last_error_code=current,None
    else:
        row.last_error_code="delivery_unconfirmed"
        row.available_at=current+timedelta(seconds=min(2**min(row.attempts,5),30))
    row.claimed_by=row.lease_until=None
    db.session.commit();return True

def dispatch_once(producer):
    claim=claim_next()
    if claim is None:return "idle"
    try:
        confirmed_publish(producer,"delivery_assignments",claim["payload"]["zone"],claim["payload"])
        published=True
    except Exception:
        published=False
    finish(claim,published)
    return "published" if published else "retry"

def main():
    app=create_storage_app();producer=create_producer("delivery-outbox")
    with app.app_context():
        while True:
            try:
                HEARTBEAT.touch()
                if dispatch_once(producer)=="idle":time.sleep(0.2)
            except KeyboardInterrupt:return
            except Exception:
                db.session.rollback()
                print("Delivery event publication deferred safely.",flush=True);time.sleep(1)

if __name__=="__main__":
    main()
