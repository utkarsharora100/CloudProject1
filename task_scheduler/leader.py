"""Two replicas contend through Kazoo Election; SQL epochs fence old leaders."""
import json
import os
import threading
import time
import uuid
from sqlalchemy import select
from task_scheduler.coordination import Coordination, ROOT
from task_scheduler.models import Workflow, db
from task_scheduler.runtime import LEADER_HEARTBEAT, heartbeat
from task_scheduler.storage import create_storage_app
from task_scheduler.store import TaskStore
from notification_service.policy import materialize_tasks


def main():
    app = create_storage_app()
    name = os.environ.get("SCHEDULER_NAME", "scheduler")
    while True:
        coordination = None
        stopped = threading.Event()
        state = {"role": "standby", "instance_id": str(uuid.uuid4()), "name": name, "epoch": None}
        try:
            coordination = Coordination(strict_session=True).start()
            election = coordination.client.Election(ROOT + "/election", identifier=state["instance_id"])
            def report():
                while not stopped.wait(0.5):
                    heartbeat(LEADER_HEARTBEAT, {**state, "connected": coordination.live})
            thread = threading.Thread(target=report, daemon=True)
            thread.start()
            def lead():
                with app.app_context():
                    store = TaskStore(coordination)
                    epoch = store.begin_term(state["instance_id"], name)
                    coordination.client.create(ROOT + "/leader", json.dumps({"leader_id": state["instance_id"],
                        "name": name, "epoch": epoch}).encode(), ephemeral=True)
                    state.update(role="leader", epoch=epoch)
                    try:
                        while coordination.live:
                            store.pulse(epoch, state["instance_id"])
                            store.materialize_orders(epoch, state["instance_id"])
                            materialize_tasks(store, epoch, state["instance_id"])
                            workers = coordination.workers()
                            ids = db.session.execute(select(Workflow.id).order_by(Workflow.created_at)).scalars().all()
                            db.session.rollback()
                            for workflow_id in ids:
                                store.reconcile(workflow_id, epoch, state["instance_id"], workers)
                            time.sleep(0.25)
                    finally:
                        state["role"] = "standby"
                        db.session.rollback()
            election.run(lead)
        except KeyboardInterrupt:
            return
        except Exception:
            print("Scheduler coordination cycle interrupted; rejoining election safely.", flush=True)
            time.sleep(0.5)
        finally:
            stopped.set()
            if coordination is not None:
                coordination.close()


if __name__ == "__main__":
    main()
