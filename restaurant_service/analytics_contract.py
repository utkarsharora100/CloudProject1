"""Public aggregate contract: counts and window bounds only, never customer or order records."""
from datetime import datetime,timezone
import hashlib
import json
import re
FIELDS={"schema_version","source","restaurant_id","window_start","window_end","window_seconds","unique_order_count","event_count","stage_counts"}
STATES={"PENDING_PAYMENT","PAYMENT_FAILED","PAID","KITCHEN_ACCEPTED","PREPARING","READY","COURIER_ASSIGNED","PICKED_UP","DELIVERED","CANCELLED","REFUND_PENDING","REFUNDED"}
def validate(raw):
    if not isinstance(raw,bytes) or len(raw)>32768:raise ValueError("result_size")
    try:
        value=json.loads(raw)
        if not isinstance(value,dict) or set(value)!=FIELDS or type(value["schema_version"]) is not int or value["schema_version"]!=1 or value["source"]!="restaurant-flink-event-time-v1":raise ValueError()
        if not isinstance(value["restaurant_id"],str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}",value["restaurant_id"]):raise ValueError()
        for key in ("window_seconds","unique_order_count","event_count"):
            if type(value[key]) is not int or value[key]<0:raise ValueError()
        if not 5<=value["window_seconds"]<=3600 or value["unique_order_count"]>value["event_count"]:raise ValueError()
        stamps=[datetime.fromisoformat(value[key].replace("Z","+00:00")) for key in ("window_start","window_end")]
        if any(stamp.tzinfo is None for stamp in stamps) or (stamps[1]-stamps[0]).total_seconds()!=value["window_seconds"]:raise ValueError()
        if int(stamps[0].timestamp())%value["window_seconds"]:raise ValueError()
        if not isinstance(value["stage_counts"],dict) or set(value["stage_counts"])-STATES:raise ValueError()
        if any(type(n) is not int or n<0 for n in value["stage_counts"].values()) or sum(value["stage_counts"].values())!=value["event_count"]:raise ValueError()
        for key,stamp in zip(("window_start","window_end"),stamps):value[key]=stamp.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
        return value
    except (KeyError,TypeError,ValueError,AttributeError):raise ValueError("invalid_result_contract") from None
def identity(value):
    text=value["restaurant_id"]+"|"+value["window_start"]+"|"+str(value["window_seconds"])
    return hashlib.sha256(text.encode()).hexdigest()
def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
