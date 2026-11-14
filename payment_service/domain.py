"""Constrained simulation requests: no card data, client prices or credentials."""
from werkzeug.exceptions import BadRequest
from order_service.domain import positive_int

SCENARIOS = {"success", "decline", "decline_then_success", "timeout_once"}

def action_request(payload, *, create=False):
    fields = {"expected_version", "scenario", "refund_scenario"} if create else {"expected_version"}
    if not isinstance(payload, dict) or set(payload) - fields or "expected_version" not in payload:
        raise BadRequest("Provide the documented simulation fields and expected_version only.")
    result = {"expected_version": positive_int(payload["expected_version"], "expected_version")}
    if create:
        scenario = payload.get("scenario")
        refund = payload.get("refund_scenario", "success")
        if not isinstance(scenario, str) or scenario not in SCENARIOS:
            raise BadRequest("Choose a documented simulated payment scenario.")
        if not isinstance(refund, str) or refund not in {"success", "timeout_once"}:
            raise BadRequest("Choose success or timeout_once for the simulated refund.")
        result.update(scenario=scenario, refund_scenario=refund)
    return result
