"""Human action inputs contain no customer details or arbitrary payloads."""
from werkzeug.exceptions import BadRequest
from order_service.domain import positive_int

STATES = {"WAITING", "ACCEPTED", "PREPARING", "READY", "REJECTED", "CANCELLED"}
REASONS = {"capacity", "item_unavailable", "closing", "kitchen_unavailable"}
ACTIONS = {"accept", "reject", "start_preparation", "ready"}
ALLOWED = {"accept": ("WAITING", "PAID"), "start_preparation": ("ACCEPTED", "KITCHEN_ACCEPTED"),
           "ready": ("PREPARING", "PREPARING")}

def action_request(payload):
    if not isinstance(payload, dict) or set(payload)-{"action", "expected_version", "reason"}:
        raise BadRequest("Provide action, expected_version and a rejection reason only.")
    action = payload.get("action")
    if not isinstance(action, str) or action not in ACTIONS:
        raise BadRequest("Choose accept, reject, start_preparation or ready.")
    result = {"action": action, "expected_version": positive_int(payload.get("expected_version"), "expected_version")}
    if action == "reject":
        reason = payload.get("reason", "kitchen_unavailable")
        if not isinstance(reason, str) or reason not in REASONS:
            raise BadRequest("Choose a documented kitchen rejection reason.")
        result["reason"] = reason
    elif "reason" in payload:
        raise BadRequest("reason is only permitted for rejection.")
    return result

LEGACY_STATUS = {"WAITING": "Awaiting_Kitchen_Acceptance", "ACCEPTED": "Accepted",
                 "PREPARING": "Preparing", "READY": "Ready", "REJECTED": "Rejected", "CANCELLED": "Cancelled"}
