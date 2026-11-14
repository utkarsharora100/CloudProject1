"""Publish only a stable ID/digest; accept delivery only after the function's GCS receipt."""
import json
import os
from datetime import datetime,timezone
from google.api_core.exceptions import NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage,pubsub_v1
from werkzeug.exceptions import Conflict,ServiceUnavailable
from common.deployment import require_profile
from notification_service.provider import SimulatedTransport

class FunctionTransport:
    def __init__(self,engine,*,publisher=None,bucket=None,environ=None):
        env=os.environ if environ is None else environ
        require_profile(environ=env)
        self.engine=engine
        project=env.get("PROJECT_ID","")
        if env.get("APP_ENV")=="local":
            if env.get("PUBSUB_EMULATOR_HOST") not in {"pubsub:8085","127.0.0.1:8089","localhost:8089"} or env.get("STORAGE_EMULATOR_HOST") not in {"http://gcs:4443","http://127.0.0.1:4443","http://localhost:4443"}:
                raise RuntimeError("Local function transport requires both isolated emulators.")
            credentials=AnonymousCredentials()
            client=storage.Client(project=project,credentials=credentials)
            self.publisher=publisher or pubsub_v1.PublisherClient(credentials=credentials)
        else:
            client=storage.Client(project=project)
            self.publisher=publisher or pubsub_v1.PublisherClient()
        self.bucket=bucket or client.bucket(env["NOTIFICATION_RECEIPTS_BUCKET"])
        self.topic=self.publisher.topic_path(project,env["NOTIFICATION_DISPATCH_TOPIC"])
    def deliver(self,notification_id,payload_digest,now):
        blob=self.bucket.blob("notifications/v1/"+notification_id+".json")
        try:
            data=json.loads(blob.download_as_text(timeout=5))
        except NotFound:
            payload={"schema_version":1,"notification_id":notification_id,"payload_digest":payload_digest}
            self.publisher.publish(self.topic,json.dumps(payload,separators=(",",":")).encode()).result(timeout=5)
            raise ServiceUnavailable("The event function has not yet confirmed a transport receipt.") from None
        if set(data)!={"schema_version","notification_id","payload_digest","delivered_at"} or data["schema_version"]!=1 or data["notification_id"]!=notification_id or data["payload_digest"]!=payload_digest:
            raise Conflict("The serverless receipt does not match this logical notification.")
        try:stamp=datetime.fromisoformat(data["delivered_at"].replace("Z","+00:00"))
        except (ValueError,TypeError):raise Conflict("The serverless receipt timestamp is invalid.") from None
        if stamp.tzinfo is None:raise Conflict("The serverless receipt must include a time zone.")
        # SQL proof is materialized only from a confirmed function-created object.
        return SimulatedTransport(self.engine).deliver(notification_id,payload_digest,stamp.astimezone(timezone.utc).replace(tzinfo=None))
