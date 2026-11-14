"""Opt-in local worker/leader/quorum failures with scoped service restoration."""
import argparse
import json
from pathlib import Path
from local.environment import require_approved_recovery_root
import subprocess
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:8085"


def http(method, path, payload=None, *, key=None, base=BASE):
    request = Request(base + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **({"Idempotency-Key": key} if key else {})})
    try:
        response = urlopen(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        content = response.read()
        return response.status, json.loads(content) if content else None


def compose(arguments):
    result = subprocess.run(["docker", "compose", "--env-file", ".env.local.example", *arguments],
                            cwd=ROOT, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("A scoped scheduler recovery operation failed; inspect local service status.")
    return result.stdout


def wait_for(predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise RuntimeError("Timed out waiting for verified scheduler recovery.")


def task(task_id):
    code, data = http("GET", "/tasks/" + task_id)
    if code != 200:
        raise RuntimeError("Recovery task could not be read.")
    return data["task"]


def job(duration=100):
    code, workflow = http("POST", "/workflows", {"tasks": [{"key": "recovery", "kind": "demo",
        "max_failures": 5, "params": {"duration_ms": duration}}]}, key=str(uuid.uuid4()))
    if code != 201:
        raise RuntimeError("Recovery workflow could not be created.")
    return workflow, workflow["tasks"]["recovery"]


def helper(mode, task_id=None, generation=None, *, epoch=None, leader_id=None):
    arguments = ["run", "--rm", "--no-deps", "-T", "scheduler-smoke-test", "python", "-m", "local.scheduler_recovery_test", mode]
    if task_id:
        arguments.extend(["--task-id", task_id, "--generation", str(generation)])
    if epoch is not None:
        arguments.extend(["--epoch", str(epoch), "--leader-id", leader_id])
    output = compose(arguments)
    if "PASS" not in output:
        raise RuntimeError("Private scheduler proof check did not confirm rejection.")


def private_probe(args):
    # This helper runs only in the isolated application image. Tokens are read in
    # memory, sent in HTTP bodies and never put in arguments, stdout or documents.
    from local.environment import require_local_emulator
    from task_scheduler.models import Attempt, Task, db
    from task_scheduler.storage import create_storage_app
    require_local_emulator()
    app = create_storage_app()
    with app.app_context():
        row = db.session.get(Attempt, (args.task_id, args.generation))
        if row is None:
            raise RuntimeError("Recorded worker attempt is unavailable.")
        proof = {"worker_id": row.worker_id, "generation": row.generation, "lease_token": row.lease_token}
        old_outcome = row.outcome
        db.session.rollback()
        base = "http://task-api:8085"
        renew_code, _ = http("POST", f"/tasks/{args.task_id}/lease/renew", proof, base=base)
        complete_code, _ = http("POST", f"/tasks/{args.task_id}/complete", {**proof, "outcome": "succeeded"}, base=base)
        db.session.expire_all()
        current = db.session.get(Task, args.task_id)
        if args.probe_stale:
            assert renew_code == 409 and complete_code == 409
            assert old_outcome == "ABANDONED" and current.generation > args.generation
            assert current.state == "SUCCEEDED" and current.result["generation"] == current.generation
            print("Old worker generation cannot renew, complete or replace the recovered result: PASS", flush=True)
        else:
            assert renew_code in {409, 503} and complete_code in {409, 503}
            assert current.result is None and current.state != "SUCCEEDED"
            print("New ownership writes are rejected during quorum loss: PASS", flush=True)


def leader_probe(args):
    from local.environment import require_local_emulator
    from task_scheduler.coordination import Coordination
    from task_scheduler.models import db
    from task_scheduler.storage import create_storage_app
    from task_scheduler.store import TaskStore
    from werkzeug.exceptions import Conflict
    require_local_emulator()
    coordination = Coordination().start()
    try:
        app = create_storage_app()
        with app.app_context():
            try:
                TaskStore(coordination).term(args.epoch, args.leader_id)
                raise AssertionError("Stale leader epoch was accepted.")
            except Conflict:
                db.session.rollback()
        print("Stale scheduler epoch is rejected by the real SQL fence: PASS", flush=True)
    finally:
        coordination.close()


def run_recovery():
    require_approved_recovery_root(ROOT)
    def healthy(min_workers=1):
        code, status = http("GET", "/scheduler")
        code2, workers = http("GET", "/workers")
        return status if code == 200 and status["leader_available"] and code2 == 200 and len(workers["workers"]) >= min_workers else None
    wait_for(lambda: healthy(2))
    crashed_task = None
    paused_task = None
    try:
        workflow, crashed_task = job(9000)
        running = wait_for(lambda: task(crashed_task) if task(crashed_task)["state"] == "RUNNING" else None)
        generation = running["generation"]
        names = {"demo-worker-a": "task-worker-a", "demo-worker-b": "task-worker-b"}
        owner_service = names.get(running["owner"]["name"])
        if owner_service is None:
            raise RuntimeError("Recovery task is owned by a worker outside the demo allowlist.")
        compose(["kill", "-s", "SIGKILL", owner_service])
        recovered = wait_for(lambda: task(crashed_task) if task(crashed_task)["state"] == "SUCCEEDED" else None)
        assert recovered["generation"] > generation
        helper("--probe-stale", crashed_task, generation)
        print("Real demo-worker crash: ephemeral loss, abandoned-attempt recovery and stale-write rejection: PASS", flush=True)

        status = wait_for(healthy)
        old = status["leader"]
        services = {"scheduler-a": "task-scheduler-a", "scheduler-b": "task-scheduler-b"}
        old_service = services.get(old["name"])
        if old_service is None:
            raise RuntimeError("Leader is outside the local recovery allowlist.")
        compose(["kill", "-s", "SIGKILL", old_service])
        def new_leader():
            value = healthy()
            return value if value and value["leader"]["instance_id"] != old["instance_id"] and value["leader"]["epoch"] > old["epoch"] else None
        promoted = wait_for(new_leader)
        helper("--probe-leader", epoch=old["epoch"], leader_id=old["instance_id"])
        _, after_failover = job()
        wait_for(lambda: task(after_failover)["state"] == "SUCCEEDED")
        print("Real scheduler-leader crash: standby promotion, higher epoch and continued execution: PASS", flush=True)

        quorum_workflow, paused_task = job(30000)
        running = wait_for(lambda: task(paused_task) if task(paused_task)["state"] == "RUNNING" else None)
        old_generation = running["generation"]
        try:
            compose(["stop", "zookeeper-2", "zookeeper-3"])
            def disconnected():
                code, value = http("GET", f"/workflows/{quorum_workflow['workflow_id']}/graph")
                return value if code == 200 and not value["coordination_available"] else None
            wait_for(disconnected)
            code, _ = http("GET", "/health")
            assert code == 503
            helper("--probe-blocked", paused_task, old_generation)
            queued, queued_task = job()
            untouched = task(queued_task)
            assert untouched["state"] == "BLOCKED" and untouched["generation"] == 0 and untouched["result"] is None
            print("Real ZooKeeper quorum loss: lease updates stop; SQL graph definitions remain queued safely: PASS", flush=True)
        finally:
            compose(["up", "-d", "--wait", "--wait-timeout", "120", "zookeeper-2", "zookeeper-3"])
        wait_for(healthy)
        resumed = wait_for(lambda: task(paused_task) if task(paused_task)["state"] == "SUCCEEDED" else None, seconds=120)
        assert resumed["generation"] > old_generation
        wait_for(lambda: task(queued_task)["state"] == "SUCCEEDED")
        helper("--probe-stale", paused_task, old_generation)
        print("Quorum restoration: fresh sessions/leases recover work without accepting the expired generation: PASS", flush=True)
    finally:
        errors = []
        for services in (("zookeeper-2", "zookeeper-3"),
                         ("task-scheduler-a", "task-scheduler-b", "task-worker-a", "task-worker-b")):
            try:
                compose(["up", "-d", "--no-deps", "--wait", "--wait-timeout", "120", *services])
            except Exception:
                errors.append("A scoped service group could not be restored.")
        if errors:
            raise RuntimeError("Local recovery restoration needs attention; inspect project services.")
        wait_for(lambda: healthy(2))
        print("Local quorum, both scheduler replicas and both demo workers restored healthy.", flush=True)
    print(json.dumps({"result": "PASS", "worker_recovery_task_id": crashed_task,
                      "quorum_recovery_task_id": paused_task, "services_restored": True}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-local-scheduler-failures", action="store_true")
    parser.add_argument("--probe-stale", action="store_true")
    parser.add_argument("--probe-blocked", action="store_true")
    parser.add_argument("--probe-leader", action="store_true")
    parser.add_argument("--task-id")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--leader-id")
    parser.add_argument("--epoch", type=int)
    args = parser.parse_args()
    if args.probe_stale or args.probe_blocked:
        if args.task_id is None or args.generation is None or args.generation < 1:
            parser.error("A valid task ID and generation are required")
        args.task_id = str(uuid.UUID(args.task_id))
        private_probe(args)
    elif args.probe_leader:
        if args.leader_id is None or args.epoch is None or args.epoch < 1:
            parser.error("A leader ID and epoch are required")
        args.leader_id = str(uuid.UUID(args.leader_id))
        leader_probe(args)
    elif args.allow_local_scheduler_failures:
        run_recovery()
    else:
        parser.error("Use the documented --allow-local-scheduler-failures flag for isolated local fault testing")


if __name__ == "__main__":
    main()
