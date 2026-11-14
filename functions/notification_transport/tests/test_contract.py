import base64
import json
import unittest
from datetime import datetime,timezone
from types import SimpleNamespace
from google.api_core.exceptions import PreconditionFailed
from main import decode,receipt
from unittest.mock import patch
import functions_framework

PAYLOAD={"schema_version":1,"notification_id":"01234567-1234-4234-8234-012345678901","payload_digest":"a"*64}
class Blob:
    def __init__(self):self.data=None
    def upload_from_string(self,value,**kwargs):
        assert kwargs["if_generation_match"]==0
        if self.data is not None:raise PreconditionFailed("existing synthetic object")
        self.data=value
    def download_as_text(self,**kwargs):return self.data
class Bucket:
    def __init__(self):self.object=Blob();self.name=None
    def blob(self,name):self.name=name;return self.object
class Contract(unittest.TestCase):
    def event(self,value):return SimpleNamespace(data={"message":{"data":base64.b64encode(json.dumps(value).encode()).decode()}})
    def test_explicit_minimal_contract(self):
        self.assertEqual(decode(self.event(PAYLOAD)),PAYLOAD)
    def test_rejects_contacts_extra_fields_and_invalid_digest(self):
        for value in ({**PAYLOAD,"email":"synthetic@example.test"},{**PAYLOAD,"schema_version":True},{**PAYLOAD,"payload_digest":"bad"},{**PAYLOAD,"notification_id":"../../escape"}):
            with self.assertRaises(ValueError):decode(self.event(value))
    def test_malformed_event_is_bounded(self):
        for value in (SimpleNamespace(data=None),SimpleNamespace(data={"message":{"data":"!"}})):
            with self.assertRaises(ValueError):decode(value)
    def test_receipt_replay_preserves_first_confirmation(self):
        bucket=Bucket();clock=lambda:datetime(2026,1,1,tzinfo=timezone.utc)
        self.assertEqual(receipt(PAYLOAD,bucket,clock=clock),"created");first=bucket.object.data
        self.assertEqual(receipt(PAYLOAD,bucket),"replayed");self.assertEqual(bucket.object.data,first)
        self.assertEqual(set(json.loads(first)),{"schema_version","notification_id","payload_digest","delivered_at"})
    def test_cloud_event_decorator_works_with_http_trigger_signature(self):
        app=functions_framework.create_app(target="deliver_notification",source="/function/main.py",signature_type="http")
        body={"message":{"data":base64.b64encode(json.dumps(PAYLOAD).encode()).decode()}}
        with patch("main.bucket",return_value=Bucket()):
            response=app.test_client().post("/",json=body,headers={"ce-specversion":"1.0","ce-type":"google.cloud.pubsub.topic.v1.messagePublished","ce-source":"//pubsub.googleapis.com/projects/demo-food-delivery/topics/notification-dispatch","ce-id":"contract-test"})
        self.assertEqual(response.status_code,200)
    def test_changed_digest_cannot_reuse_receipt_identity(self):
        bucket=Bucket();receipt(PAYLOAD,bucket)
        with self.assertRaises(ValueError):receipt({**PAYLOAD,"payload_digest":"b"*64},bucket)
if __name__=="__main__":unittest.main()
