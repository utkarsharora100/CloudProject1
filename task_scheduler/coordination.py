from common.deployment import zk_hosts
"""ZooKeeper presence, leader proof and worker-session lease validation."""
import hmac
import json
import logging
import os
import threading
import uuid
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NoNodeError
from kazoo.retry import KazooRetry
from werkzeug.exceptions import Conflict, ServiceUnavailable


ROOT = "/food-delivery/tasks-v1"


def lease_path(task_id, generation):
    return f"{ROOT}/leases/{task_id}/{generation}"


class Coordination:
    def __init__(self, *, strict_session=False, environ=None):
        env = os.environ if environ is None else environ
        hosts = zk_hosts(env)
        self.strict_session = strict_session
        self.invalidated = threading.Event()
        self.seen_connected = False
        logger = logging.getLogger("food-delivery.scheduler.zookeeper")
        logger.setLevel(logging.CRITICAL)
        self.client = KazooClient(hosts=hosts, timeout=6, read_only=False, logger=logger,
            command_retry=KazooRetry(max_tries=2, delay=0.1, max_delay=0.2, deadline=2),
            connection_retry=KazooRetry(max_tries=-1, delay=0.2, max_delay=1))
        self.client.add_listener(self._state)

    def _state(self, state):
        if state == KazooState.CONNECTED:
            self.seen_connected = True
        elif self.seen_connected and self.strict_session:
            self.invalidated.set()

    @property
    def live(self):
        return self.client.state == KazooState.CONNECTED and not self.invalidated.is_set()

    def guard(self):
        if not self.live:
            raise ServiceUnavailable("ZooKeeper coordination is unavailable; ownership work is paused.")

    def start(self):
        self.client.start(timeout=10)
        for suffix in ("", "/election", "/workers", "/leases"):
            self.client.ensure_path(ROOT + suffix)
        self.guard()
        return self

    def close(self):
        self.invalidated.set()
        self.client.stop()
        self.client.close()

    def read(self, path):
        self.guard()
        try:
            self.client.sync(ROOT)
            value, stat = self.client.get(path)
            self.guard()
            return json.loads(value), stat
        except NoNodeError:
            return None
        except (KazooException, ValueError, TypeError):
            raise ServiceUnavailable("ZooKeeper coordination could not be verified.") from None

    def workers(self):
        self.guard()
        try:
            self.client.sync(ROOT)
            ids = self.client.get_children(ROOT + "/workers")
            result = []
            for worker_id in ids:
                try:
                    if str(uuid.UUID(worker_id)) != worker_id:
                        continue
                    record = self.read(ROOT + "/workers/" + worker_id)
                    if record is None:
                        continue
                    value, stat = record
                    name = value.get("name")
                    capabilities = value.get("capabilities")
                    if (not stat.ephemeralOwner or not isinstance(name, str) or not 1 <= len(name) <= 64
                            or not isinstance(capabilities, list) or not capabilities
                            or any(kind not in {"demo", "notification", "payment", "kitchen_accept", "preparation", "courier_assignment", "pickup", "delivery", "refund"} for kind in capabilities)):
                        continue
                    result.append({"worker_id": worker_id, "name": name,
                                   "capabilities": capabilities, "session": str(stat.ephemeralOwner)})
                except (ValueError, TypeError, AttributeError):
                    continue
            return result
        except KazooException:
            raise ServiceUnavailable("Worker presence could not be verified.") from None

    def worker(self, worker_id, session=None):
        worker = next((item for item in self.workers() if item["worker_id"] == worker_id), None)
        if worker is None or (session is not None and worker["session"] != session):
            raise Conflict("Worker ownership is no longer valid.")
        return worker

    def verify_lease(self, task_id, generation, worker_id, session, token):
        self.worker(worker_id, session)
        record = self.read(lease_path(task_id, generation))
        if record is None:
            raise Conflict("Worker lease is no longer valid.")
        value, stat = record
        if (str(stat.ephemeralOwner) != session or value.get("worker_id") != worker_id
                or value.get("generation") != generation
                or not hmac.compare_digest(str(value.get("lease_token", "")), token)):
            raise Conflict("Worker lease proof does not match current ownership.")

    def leader(self):
        record = self.read(ROOT + "/leader")
        if record is None:
            return None
        value, stat = record
        return value if stat.ephemeralOwner else None

    def verify_leader(self, epoch, leader_id, *, own=False):
        record = self.read(ROOT + "/leader")
        if record is None:
            raise Conflict("No active scheduler leader.")
        value, stat = record
        if (value.get("epoch") != epoch or value.get("leader_id") != leader_id
                or not stat.ephemeralOwner or (own and stat.ephemeralOwner != self.client.client_id[0])):
            raise Conflict("Scheduler leadership changed.")

    def register_worker(self, worker_id, name, capabilities):
        self.guard()
        self.client.create(ROOT + "/workers/" + worker_id,
            json.dumps({"name": name, "capabilities": capabilities}).encode(), ephemeral=True)

    def create_lease(self, task_id, generation, worker_id, token):
        self.guard()
        self.client.create(lease_path(task_id, generation), json.dumps({"worker_id": worker_id,
            "generation": generation, "lease_token": token}).encode(), ephemeral=True, makepath=True)

    def remove_lease(self, task_id, generation):
        if self.live:
            try:
                self.client.delete(lease_path(task_id, generation))
            except KazooException:
                pass
