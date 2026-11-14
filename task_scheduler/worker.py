from common.deployment import service_endpoint
"""Demo-only worker; future business handlers must use the same lease protocol."""
import os
import time
import uuid
from task_scheduler.coordination import Coordination
from task_scheduler.runtime import WORKER_HEARTBEAT, WorkerHTTPError, api_call, heartbeat


def main():
    base = os.environ.get("TASK_API_URL", "http://task-api:8085")
    service_endpoint(base,{"http://task-api:8085","http://127.0.0.1:8085","http://localhost:8085"})
    name = os.environ.get("WORKER_NAME", "demo-worker")
    while True:
        coordination = None
        worker_id = str(uuid.uuid4())
        try:
            coordination = Coordination(strict_session=True).start()
            coordination.register_worker(worker_id, name, ["demo"])
            def report():
                heartbeat(WORKER_HEARTBEAT, {"role": "worker", "instance_id": worker_id,
                                           "name": name, "connected": coordination.live})
            while coordination.live:
                report()
                assignments = api_call(base, "GET", f"/workers/{worker_id}/assignments")["assignments"]
                if not assignments:
                    time.sleep(0.25)
                    continue
                for assignment in assignments:
                    if assignment["kind"] != "demo":
                        raise RuntimeError("Demo worker received an unsupported handler.")
                    task_id, generation = assignment["task_id"], assignment["generation"]
                    token = str(uuid.uuid4())
                    proof = {"worker_id": worker_id, "generation": generation, "lease_token": token}
                    coordination.create_lease(task_id, generation, worker_id, token)
                    try:
                        started = api_call(base, "POST", f"/tasks/{task_id}/lease/start", proof)
                        deadline = time.monotonic() + assignment["params"]["duration_ms"] / 1000
                        next_renewal = time.monotonic() + 1
                        while time.monotonic() < deadline:
                            coordination.guard()
                            report()
                            if time.monotonic() >= next_renewal:
                                api_call(base, "POST", f"/tasks/{task_id}/lease/renew", proof)
                                next_renewal = time.monotonic() + 1
                            time.sleep(0.1)
                        coordination.guard()
                        outcome = "failed" if started["attempt_number"] <= assignment["params"]["fail_attempts"] else "succeeded"
                        api_call(base, "POST", f"/tasks/{task_id}/complete", {**proof, "outcome": outcome})
                    finally:
                        coordination.remove_lease(task_id, generation)
                    report()
        except KeyboardInterrupt:
            return
        except Exception:
            print("Demo worker ownership cycle interrupted; dropping the session and retrying.", flush=True)
            time.sleep(0.5)
        finally:
            if coordination is not None:
                coordination.close()


if __name__ == "__main__":
    main()
