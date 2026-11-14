"""Small local heartbeat and HTTP helpers; lease proofs never enter logs."""
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


LEADER_HEARTBEAT = Path("/tmp/task-scheduler-heartbeat.json")
WORKER_HEARTBEAT = Path("/tmp/task-worker-heartbeat.json")


def heartbeat(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({**value, "at": time.time()}), encoding="utf-8")
    temporary.replace(path)


class WorkerHTTPError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__("Worker operation was not confirmed.")


def api_call(base, method, path, payload=None):
    request = Request(base + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=3) as response:
            return json.load(response)
    except HTTPError as error:
        raise WorkerHTTPError(error.code) from None
