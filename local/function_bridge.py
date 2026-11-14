"""Local Eventarc equivalent: pull emulator messages and invoke the actual CloudEvent function."""
import json
import time
from pathlib import Path
from urllib.request import Request,urlopen
from local.assignment_init import clients
from common.logging import event
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable
from task_scheduler.runtime import heartbeat

HEARTBEAT=Path("/tmp/function-bridge-heartbeat.json")
def main():
    _,subscriber,_=clients()
    subscription=subscriber.subscription_path("demo-food-delivery","notification-function-local")
    while True:
        heartbeat(HEARTBEAT,{"running":True})
        try:response=subscriber.pull(request={"subscription":subscription,"max_messages":1},timeout=10)
        except DeadlineExceeded:continue
        except ServiceUnavailable:
            event("notification-function-bridge","pull_deferred",reason="emulator_unavailable")
            time.sleep(2);continue
        for received in response.received_messages:
            message=received.message
            import base64
            body=json.dumps({"message":{"data":base64.b64encode(message.data).decode(),"messageId":message.message_id},"subscription":subscription}).encode()
            request=Request("http://notification-function:8080",data=body,headers={"Content-Type":"application/json","ce-specversion":"1.0","ce-type":"google.cloud.pubsub.topic.v1.messagePublished","ce-source":"//pubsub.googleapis.com/projects/demo-food-delivery/topics/notification-dispatch","ce-id":message.message_id},method="POST")
            try:
                with urlopen(request,timeout=12) as result:
                    if result.status!=200:raise RuntimeError("Function outcome unavailable.")
                subscriber.acknowledge(request={"subscription":subscription,"ack_ids":[received.ack_id]},timeout=5)
                event("notification-function-bridge","event_acknowledged",outcome="confirmed")
            except Exception:
                subscriber.modify_ack_deadline(request={"subscription":subscription,"ack_ids":[received.ack_id],"ack_deadline_seconds":2},timeout=5)
                event("notification-function-bridge","event_deferred",reason="function_unconfirmed")
        time.sleep(.1)
if __name__=="__main__":main()
