"""Order request contracts, canonical idempotency hashes and lifecycle rules."""
import hashlib
import json
import re
import uuid
from werkzeug.exceptions import BadRequest, Conflict


TRANSITIONS = {
    "PENDING_PAYMENT": {"PAID", "PAYMENT_FAILED", "CANCELLED"},
    "PAYMENT_FAILED": {"PENDING_PAYMENT", "CANCELLED"},
    "PAID": {"KITCHEN_ACCEPTED", "REFUND_PENDING"},
    "KITCHEN_ACCEPTED": {"PREPARING", "REFUND_PENDING"},
    "PREPARING": {"READY"}, "READY": {"COURIER_ASSIGNED"},
    "COURIER_ASSIGNED": {"PICKED_UP"}, "PICKED_UP": {"DELIVERED"},
    "REFUND_PENDING": {"REFUNDED"}, "DELIVERED": set(),
    "CANCELLED": set(), "REFUNDED": set(),
}
EVENT_TYPES = {
    "PENDING_PAYMENT": "OrderPaymentRetryRequested", "PAID": "OrderPaymentSucceeded",
    "PAYMENT_FAILED": "OrderPaymentFailed", "CANCELLED": "OrderCancelled",
    "KITCHEN_ACCEPTED": "OrderKitchenAccepted", "PREPARING": "OrderPreparationStarted",
    "READY": "OrderReady", "COURIER_ASSIGNED": "OrderCourierAssigned",
    "PICKED_UP": "OrderPickedUp", "DELIVERED": "OrderDelivered",
    "REFUND_PENDING": "OrderRefundRequested", "REFUNDED": "OrderRefunded",
}


def positive_int(value, field, maximum=2147483647):
    if type(value) is not int or not 1 <= value <= maximum:
        raise BadRequest(f"{field} must be a positive integer within the allowed range.")
    return value


def identifier(value, field):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
        raise BadRequest(f"{field} must be a valid string identifier.")
    return value


def order_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", value):
        raise BadRequest("order_id must be a UUID.")
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise BadRequest("order_id must be a UUID.") from None


def idempotency_key(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}", value):
        raise BadRequest("Idempotency-Key must contain 8 to 128 permitted ASCII characters.")
    return value


def create_request(payload):
    fields = {"customer_id", "address_id", "restaurant_id", "items"}
    if not isinstance(payload, dict) or set(payload) != fields:
        raise BadRequest("Provide customer_id, address_id, restaurant_id and items only; totals are calculated by the server.")
    items = payload["items"]
    if not isinstance(items, list) or not 1 <= len(items) <= 50:
        raise BadRequest("items must contain 1 to 50 distinct menu items.")
    clean = []
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"item_id", "quantity"}:
            raise BadRequest("Each item must contain item_id and quantity only.")
        key = identifier(item["item_id"], "item_id")
        if key in seen:
            raise BadRequest("Duplicate menu item identifiers are not allowed.")
        seen.add(key)
        clean.append({"item_id": key, "quantity": positive_int(item["quantity"], "quantity", 20)})
    return {"customer_id": positive_int(payload["customer_id"], "customer_id"),
            "address_id": positive_int(payload["address_id"], "address_id"),
            "restaurant_id": identifier(payload["restaurant_id"], "restaurant_id"),
            "items": sorted(clean, key=lambda item: item["item_id"])}


def transition_request(payload):
    if not isinstance(payload, dict) or set(payload) != {"to_state", "expected_version"}:
        raise BadRequest("Provide to_state and expected_version only.")
    target = payload["to_state"]
    if not isinstance(target, str) or target not in TRANSITIONS:
        raise BadRequest("to_state is not a recognized order state.")
    return {"to_state": target,
            "expected_version": positive_int(payload["expected_version"], "expected_version")}


def validate_transition(current, target):
    if target not in TRANSITIONS.get(current, set()):
        raise Conflict("The requested transition is not allowed from the current order state.")


def request_hash(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
