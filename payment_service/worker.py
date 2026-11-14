from common.deployment import service_endpoint
"""Payment/refund worker owns ZK leases and calls a fenced simulated destination."""
import os
import time
import uuid
from pathlib import Path
from task_scheduler.coordination import Coordination
from task_scheduler.runtime import WorkerHTTPError, api_call, heartbeat

HEARTBEAT = Path("/tmp/payment-worker-heartbeat.json")

def main():
    task_api = os.environ.get("TASK_API_URL", "")
    payment_api = os.environ.get("PAYMENT_API_URL", "")
    service_endpoint(task_api,{"http://task-api:8085"})
    service_endpoint(payment_api,{"http://payment-service:8086"})
    name = os.environ.get("WORKER_NAME", "payment-worker")
    while True:
        coordination = None
        worker_id = str(uuid.uuid4())
        try:
            coordination = Coordination(strict_session=True).start()
            coordination.register_worker(worker_id, name, ["payment", "refund"])
            while coordination.live:
                heartbeat(HEARTBEAT, {"connected": True, "instance_id": worker_id, "name": name})
                jobs = api_call(task_api, "GET", f"/workers/{worker_id}/assignments")["assignments"]
                for job in jobs:
                    task_id, generation = job["task_id"], job["generation"]
                    if job["kind"] not in {"payment", "refund"}:
                        raise RuntimeError("Unsupported payment handler.")
                    proof = {"worker_id": worker_id, "generation": generation, "lease_token": str(uuid.uuid4())}
                    coordination.create_lease(task_id, generation, worker_id, proof["lease_token"])
                    try:
                        api_call(task_api, "POST", f"/tasks/{task_id}/lease/start", proof)
                        try:
                            result = api_call(payment_api, "POST", f"/internal/tasks/{task_id}/process", proof)
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
            print("Payment worker ownership interrupted; rejoining safely.", flush=True)
            time.sleep(0.5)
        finally:
            if coordination is not None:
                coordination.close()

if __name__ == "__main__":
    main()
