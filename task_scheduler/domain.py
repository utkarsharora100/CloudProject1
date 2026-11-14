"""Immutable task graphs, constrained handlers and safe operational inputs."""
from graphlib import CycleError, TopologicalSorter
import hashlib
import json
import re
import uuid
import unicodedata
from werkzeug.exceptions import BadRequest


BUSINESS_PROOFS = {"payment": "PAID", "kitchen_accept": "KITCHEN_ACCEPTED", "preparation": "READY",
                   "courier_assignment": "COURIER_ASSIGNED", "pickup": "PICKED_UP",
                   "delivery": "DELIVERED", "refund": "REFUNDED"}
KINDS = {"demo", "notification", *BUSINESS_PROOFS}
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED"}
ACTIVE = {"ASSIGNED", "RUNNING"}


def uuid_id(value):
    if not isinstance(value, str) or len(value) != 36:
        raise BadRequest("A UUID identifier is required.")
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise BadRequest("A UUID identifier is required.") from None


def number(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise BadRequest(f"{name} must be an integer within the allowed range.")
    return value


def key(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value):
        raise BadRequest("Task keys must be lowercase slugs of 1 to 64 characters.")
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def request_key(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}", value):
        raise BadRequest("Idempotency-Key must contain 8 to 128 permitted ASCII characters.")
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def graph_request(payload):
    if not isinstance(payload, dict) or set(payload) - {"order_id", "tasks"} or "tasks" not in payload:
        raise BadRequest("Provide tasks and an optional order_id only.")
    order_id = uuid_id(payload["order_id"]) if payload.get("order_id") is not None else None
    tasks = payload["tasks"]
    if not isinstance(tasks, list) or not 1 <= len(tasks) <= 50:
        raise BadRequest("A graph must contain 1 to 50 tasks.")
    cleaned = {}
    for task in tasks:
        if not isinstance(task, dict) or set(task) - {"key", "kind", "label", "depends_on", "when", "params", "max_failures"}:
            raise BadRequest("Unsupported task fields.")
        name = key(task.get("key"))
        if name in cleaned:
            raise BadRequest("Task keys must be unique.")
        kind = task.get("kind")
        if not isinstance(kind, str) or kind not in KINDS or (kind != "demo" and order_id is None):
            raise BadRequest("Choose a supported task kind; business tasks require an order.")
        label = task.get("label", name.replace("_", " "))
        if not isinstance(label, str) or not 1 <= len(label.strip()) <= 100 or any(unicodedata.category(c) in {"Cc", "Cs"} for c in label):
            raise BadRequest("Task labels must be short, valid text.")
        parents = task.get("depends_on", [])
        if not isinstance(parents, list) or any(not isinstance(parent, str) for parent in parents) or len(parents) != len(set(parents)):
            raise BadRequest("depends_on must contain unique task keys.")
        parents = sorted(key(parent) for parent in parents)
        condition = task.get("when", "always")
        if not isinstance(condition, str) or condition not in {"always", "refund_requested"} or (condition != "always" and order_id is None):
            raise BadRequest("Unsupported task branch condition.")
        params = task.get("params", {})
        if not isinstance(params, dict) or set(params) - ({"duration_ms", "fail_attempts"} if kind == "demo" else set()):
            raise BadRequest("Only constrained demo timing/failure parameters are supported here.")
        if kind == "demo":
            params = {"duration_ms": number(params.get("duration_ms", 100), "duration_ms", 0, 30000),
                      "fail_attempts": number(params.get("fail_attempts", 0), "fail_attempts", 0, 3)}
        cleaned[name] = {"key": name, "kind": kind, "label": label.strip(), "depends_on": parents,
                         "when": condition, "params": params,
                         "max_failures": number(task.get("max_failures", 3), "max_failures", 1, 5)}
    if sum(len(task["depends_on"]) for task in cleaned.values()) > 200:
        raise BadRequest("The graph has too many dependencies.")
    if any(parent not in cleaned for task in cleaned.values() for parent in task["depends_on"]):
        raise BadRequest("Every dependency must refer to a task in this graph.")
    try:
        tuple(TopologicalSorter({name: task["depends_on"] for name, task in cleaned.items()}).static_order())
    except CycleError:
        raise BadRequest("Task dependencies must be acyclic.") from None
    return {"order_id": order_id, "tasks": [cleaned[name] for name in sorted(cleaned)]}


def default_graph(order_id):
    nodes = [
        ("payment", "payment", [], "always"),
        ("kitchen_accept", "kitchen_accept", ["payment"], "always"),
        ("prepare", "preparation", ["kitchen_accept"], "always"),
        ("assign", "courier_assignment", ["prepare"], "always"),
        ("pickup", "pickup", ["prepare", "assign"], "always"),
        ("deliver", "delivery", ["pickup"], "always"),
        ("notify_paid", "notification", ["payment"], "always"),
        ("notify_ready", "notification", ["prepare"], "always"),
        ("notify_delivered", "notification", ["deliver"], "always"),
        ("refund", "refund", ["payment"], "refund_requested"),
    ]
    return graph_request({"order_id": order_id, "tasks": [
        {"key": name, "kind": kind, "depends_on": parents, "when": condition}
        for name, kind, parents, condition in nodes]})


def lease_request(payload, *, completion=False):
    allowed = {"worker_id", "generation", "lease_token"}
    if completion:
        allowed |= {"outcome", "retry_after"}
    if not isinstance(payload, dict) or set(payload) - allowed or not {"worker_id", "generation", "lease_token"} <= set(payload):
        raise BadRequest("A complete worker lease proof is required.")
    clean = {"worker_id": uuid_id(payload["worker_id"]),
             "generation": number(payload["generation"], "generation", 1, 2147483647),
             "lease_token": uuid_id(payload["lease_token"])}
    if completion:
        outcome = payload.get("outcome")
        if not isinstance(outcome, str) or outcome not in {"succeeded", "failed", "deferred"}:
            raise BadRequest("Choose succeeded, failed or deferred.")
        clean.update(outcome=outcome, retry_after=number(payload.get("retry_after", 2), "retry_after", 1, 60))
    return clean
