from common.deployment import firestore_client
"""Durable queue/cancellation reconciliation and retryable Firestore projection."""
from pathlib import Path
import time
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from order_service.models import db
from order_service.storage import create_storage_app
from local.environment import require_local_emulator
from task_scheduler.runtime import heartbeat
from kitchen_workflow.projection import ProjectionRepository
from kitchen_workflow.store import KitchenStore

HEARTBEAT = Path("/tmp/kitchen-sync-heartbeat.json")

def main():
    database_client = firestore_client()
    app = create_storage_app()
    repository = ProjectionRepository(database_client)
    with app.app_context():
        store = KitchenStore()
        while True:
            try:
                store.materialize()
                store.synchronize()
                heartbeat(HEARTBEAT, {"running": True})
                for _ in range(20):
                    if not store.project_one(repository):
                        break
                    heartbeat(HEARTBEAT, {"running": True})
                time.sleep(0.25)
            except KeyboardInterrupt:
                return
            except Exception:
                db.session.rollback()
                print("Kitchen reconciliation deferred; retrying safely.", flush=True)
                time.sleep(1)

if __name__ == "__main__":
    main()
