"""Recover subscribed SQL facts even if a Kafka event arrived before opt-in."""
import time
from pathlib import Path
from sqlalchemy import select
from order_service.storage import create_storage_app
from order_service.models import db
from task_scheduler.runtime import heartbeat
from notification_service.models import Subscription
from notification_service.ingest import reconcile_order, recover_exhausted_tasks

HEARTBEAT=Path("/tmp/notification-reconcile-heartbeat.json")

def main():
    app=create_storage_app()
    with app.app_context():
        cursor=""
        while True:
            try:
                ids=db.session.execute(select(Subscription.order_id).where(Subscription.order_id>cursor)
                    .order_by(Subscription.order_id).limit(30)).scalars().all()
                db.session.rollback()
                for value in ids:
                    reconcile_order(value);cursor=value
                if not ids:cursor=""
                recover_exhausted_tasks()
                heartbeat(HEARTBEAT,{"running":True})
                time.sleep(0.5)
            except KeyboardInterrupt:return
            except Exception:
                db.session.rollback()
                print("Notification reconciliation deferred safely.",flush=True);time.sleep(1)

if __name__=="__main__":
    main()
