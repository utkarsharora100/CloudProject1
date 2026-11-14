"""Constrained synthetic fleet and physical-action inputs; no contact data or keys."""
import unicodedata
from werkzeug.exceptions import BadRequest
from order_service.domain import identifier, positive_int, order_id

SCENARIOS = {"manual", "normal", "no_courier", "capacity_release", "competition", "presence_loss", "recovery"}
PRESENCE_SECONDS = 30

def fields(payload, required, optional=()):
    if not isinstance(payload,dict) or not set(required)<=set(payload) or set(payload)-set(required)-set(optional):
        raise BadRequest("Provide the documented delivery fields only.")
    return payload

def zone_request(payload):
    fields(payload,{"zone","scenario"})
    scenario=payload["scenario"]
    if not isinstance(scenario,str) or scenario not in SCENARIOS:
        raise BadRequest("Choose a documented simulation scenario.")
    return {"zone":identifier(payload["zone"],"zone"),"scenario":scenario}

def courier_request(payload):
    fields(payload,{"name","zone"})
    name=payload["name"]
    if not isinstance(name,str) or not 1<=len(name.strip())<=64 or any(unicodedata.category(c) in {"Cc","Cs"} for c in name):
        raise BadRequest("Courier name must be short valid synthetic text.")
    return {"name":name.strip(),"zone":identifier(payload["zone"],"zone")}

def presence_request(payload):
    fields(payload,{"expected_version","online","accepting_work"})
    if type(payload["online"]) is not bool or type(payload["accepting_work"]) is not bool:
        raise BadRequest("Presence flags must be booleans.")
    if not payload["online"] and payload["accepting_work"]:
        raise BadRequest("An offline courier cannot accept new work.")
    return {**payload,"expected_version":positive_int(payload["expected_version"],"expected_version")}

def dispatch_request(payload):
    fields(payload,{"expected_version","zone"})
    return {"expected_version":positive_int(payload["expected_version"],"expected_version"),
            "zone":identifier(payload["zone"],"zone")}

def action_request(payload):
    fields(payload,{"action","expected_version","courier_id","assignment_id"})
    if not isinstance(payload["action"],str) or payload["action"] not in {"pickup","deliver"}:
        raise BadRequest("Choose pickup or deliver.")
    return {"action":payload["action"],"expected_version":positive_int(payload["expected_version"],"expected_version"),
            "courier_id":order_id(payload["courier_id"]),"assignment_id":order_id(payload["assignment_id"])}
