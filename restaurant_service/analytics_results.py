"""Read-committed Kafka results -> idempotent Firestore projection; durable poison quarantine."""
import hashlib
import os
import time
from pathlib import Path
from confluent_kafka import Consumer,TopicPartition
from google.cloud import firestore
from common.deployment import firestore_client,require_profile
from common.kafka import kafka_config
from common.logging import event
from task_scheduler.runtime import heartbeat
from restaurant_service.analytics_contract import validate,identity,fingerprint

HEARTBEAT=Path("/tmp/restaurant-analytics-results-heartbeat.json")
def project(client,value):
    ref=client.collection("restaurant_analytics_v1").document(identity(value));digest=fingerprint(value)
    @firestore.transactional
    def write(transaction):
        snapshot=ref.get(transaction=transaction,retry=None,timeout=5)
        if snapshot.exists:
            if snapshot.to_dict().get("result_digest")!=digest:raise ValueError("result_identity_conflict")
            return False
        transaction.create(ref,{**value,"result_digest":digest,"received_at":firestore.SERVER_TIMESTAMP})
        return True
    return write(client.transaction(max_attempts=5))
def consume(client,topic,partition,offset,raw):
    try:value=validate(raw)
    except ValueError:
        key=hashlib.sha256(f"{topic}:{partition}:{offset}".encode()).hexdigest()
        client.collection("restaurant_analytics_quarantine_v1").document(key).set({"source_topic":topic,"partition":partition,"offset":offset,"payload_digest":hashlib.sha256(raw or b"").hexdigest(),"reason":"invalid_result_contract"},retry=None,timeout=5)
        return
    try:project(client,value)
    except ValueError:
        key=hashlib.sha256(f"{topic}:{partition}:{offset}".encode()).hexdigest()
        client.collection("restaurant_analytics_quarantine_v1").document(key).set({"source_topic":topic,"partition":partition,"offset":offset,"payload_digest":fingerprint(value),"reason":"result_identity_conflict"},retry=None,timeout=5)
        return
    event("restaurant-analytics-results","window_projected",window_start=value["window_start"],window_end=value["window_end"],count=value["unique_order_count"])
def main():
    require_profile();client=firestore_client()
    config=kafka_config("restaurant-analytics-results")
    config.update({"group.id":"restaurant-analytics-results-v1","auto.offset.reset":"earliest","enable.auto.commit":False,"isolation.level":"read_committed","log_level":0,"error_cb":lambda error:None})
    while True:
        consumer=Consumer(config);consumer.subscribe([os.environ.get("ANALYTICS_RESULTS_TOPIC","restaurant_analytics_results")])
        try:
            while True:
                heartbeat(HEARTBEAT,{"running":True})
                message=consumer.poll(.5)
                if message is None or message.error():continue
                try:
                    consume(client,message.topic(),message.partition(),message.offset(),message.value())
                    consumer.commit(message=message,asynchronous=False)
                except Exception:
                    consumer.seek(TopicPartition(message.topic(),message.partition(),message.offset()))
                    event("restaurant-analytics-results","projection_deferred",reason="destination_unconfirmed");time.sleep(1)
        except KeyboardInterrupt:return
        except Exception:event("restaurant-analytics-results","stream_rejoining")
        finally:consumer.close()
if __name__=="__main__":main()
