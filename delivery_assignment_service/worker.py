from common.deployment import service_endpoint
"""Delivery worker owns ZK leases and calls a fenced simulated destination."""
import os
import time
import uuid
from pathlib import Path
from task_scheduler.coordination import Coordination
from task_scheduler.runtime import WorkerHTTPError, api_call, heartbeat

HEARTBEAT = Path("/tmp/delivery-worker-heartbeat.json")

def main():
    task_api = os.environ.get("TASK_API_URL", "")
    delivery_api = os.environ.get("DELIVERY_API_URL", "")
    service_endpoint(task_api,{"http://task-api:8085"})
    service_endpoint(delivery_api,{"http://delivery-service:8082"})
    name = os.environ.get("WORKER_NAME", "delivery-worker")
    while True:
        coordination = None
        worker_id = str(uuid.uuid4())
        try:
            coordination = Coordination(strict_session=True).start()
            coordination.register_worker(worker_id, name, ["courier_assignment", "pickup", "delivery"])
            while coordination.live:
                heartbeat(HEARTBEAT, {"connected": True, "instance_id": worker_id, "name": name})
                jobs = api_call(task_api, "GET", f"/workers/{worker_id}/assignments")["assignments"]
                for job in jobs:
                    task_id, generation = job["task_id"], job["generation"]
                    if job["kind"] not in {"courier_assignment", "pickup", "delivery"}:
                        raise RuntimeError("Unsupported delivery handler.")
                    proof = {"worker_id": worker_id, "generation": generation, "lease_token": str(uuid.uuid4())}
                    coordination.create_lease(task_id, generation, worker_id, proof["lease_token"])
                    try:
                        api_call(task_api, "POST", f"/tasks/{task_id}/lease/start", proof)
                        try:
                            result = api_call(delivery_api, "POST", f"/internal/tasks/{task_id}/process", proof)
                            outcome = result["task_outcome"]
                        except WorkerHTTPError as error:
                            if error.code != 503:
                                raise
                            outcome = "deferred"
                        coordination.guard()
                        api_call(task_api, "POST", f"/tasks/{task_id}/complete",
                                 {**proof, "outcome": outcome, "retry_after": 2})
                    finally:
                        coordination.remove_lease(task_id, generation)
                time.sleep(0.25)
        except KeyboardInterrupt:
            return
        except Exception:
            print("Delivery worker ownership interrupted; rejoining safely.", flush=True)
            time.sleep(0.5)
        finally:
            if coordination is not None:
                coordination.close()

if __name__ == "__main__":
    main()
