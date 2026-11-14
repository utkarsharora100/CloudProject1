"""Notification Service's asynchronous Cloud Function. No SQL, Kafka or contact credentials."""
import base64
import binascii
import json
import os
import re
import uuid
from pathlib import Path
from datetime import datetime,timezone
import functions_framework
from google.api_core.exceptions import PreconditionFailed
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage


def log(name,**fields):
    data={"service":"notification-function","event":name,**fields}
    line=json.dumps(data,separators=(",",":"));print(line,flush=True)
    directory=os.environ.get("LOG_DIR","")
    if directory:
        path=Path(directory)/("notification-function-"+re.sub(r"[^A-Za-z0-9_-]","-",os.environ.get("HOSTNAME","instance"))+".jsonl")
        with path.open("a",encoding="utf-8") as stream:stream.write(line+"\n")

def decode(event):
    try:
        message=event.data.get("message",{})
        raw=base64.b64decode(message["data"],validate=True)
        if len(raw)>4096:raise ValueError()
        value=json.loads(raw)
        if set(value)!={"schema_version","notification_id","payload_digest"} or type(value.get("schema_version")) is not int or value["schema_version"]!=1:
            raise ValueError()
        if str(uuid.UUID(value["notification_id"]))!=value["notification_id"] or not re.fullmatch(r"[0-9a-f]{64}",value["payload_digest"]):
            raise ValueError()
        return value
    except (KeyError,ValueError,TypeError,AttributeError,binascii.Error):
        raise ValueError("Unsupported notification event.") from None

def receipt(payload,bucket,*,clock=None):
    name="notifications/v1/"+payload["notification_id"]+".json"
    data={**payload,"delivered_at":(clock or (lambda:datetime.now(timezone.utc)))().isoformat().replace("+00:00","Z")}
    blob=bucket.blob(name)
    try:
        blob.upload_from_string(json.dumps(data,separators=(",",":")),content_type="application/json",if_generation_match=0,timeout=8)
        return "created"
    except PreconditionFailed:
        previous=json.loads(blob.download_as_text(timeout=8))
        if set(previous)!={"schema_version","notification_id","payload_digest","delivered_at"} or any(previous.get(k)!=v for k,v in payload.items()) or not isinstance(previous.get("delivered_at"),str):
            raise ValueError("Stable notification receipt identity conflict.") from None
        stamp=datetime.fromisoformat(previous["delivered_at"].replace("Z","+00:00"))
        if stamp.tzinfo is None:raise ValueError("Receipt timestamp requires a time zone.")
        return "replayed"

def bucket():
    project=os.environ["PROJECT_ID"]
    mode=os.environ.get("APP_ENV")
    if mode=="local":
        if project!="demo-food-delivery" or os.environ.get("STORAGE_EMULATOR_HOST")!="http://gcs:4443":
            raise RuntimeError("The local function requires its isolated object emulator.")
        client=storage.Client(project=project,credentials=AnonymousCredentials())
    elif mode=="cloud" and os.environ.get("CLOUD_DEPLOYMENT_ACK")=="assignment-private-network-v1":
        if os.environ.get("STORAGE_EMULATOR_HOST") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            raise RuntimeError("Cloud function credentials must come from its service identity.")
        client=storage.Client(project=project)
    else:raise RuntimeError("Explicit local or reviewed cloud function configuration is required.")
    return client.bucket(os.environ["NOTIFICATION_RECEIPTS_BUCKET"])

@functions_framework.cloud_event
def deliver_notification(event):
    try:payload=decode(event)
    except ValueError:
        # A malformed poison event is acknowledged, with sanitized diagnostic metadata.
        log("event_rejected",reason="invalid_contract")
        return
    try:result=receipt(payload,bucket())
    except ValueError:
        log("event_rejected",reason="receipt_identity_conflict");return
    except Exception as error:
        log("receipt_unconfirmed",reason=type(error).__name__);raise
    log("transport_receipt",outcome=result)
