"""Actual Flink contract/recovery test; synthetic events go only to dedicated test topics."""
import argparse
import json
import os
from pathlib import Path
import time
import uuid
from datetime import datetime,timezone
from urllib.request import urlopen
from confluent_kafka import Producer,Consumer,TopicPartition
from confluent_kafka.admin import AdminClient,NewTopic
from common.kafka import kafka_config
from restaurant_service.analytics_contract import validate
CONTEXT=Path("/evidence/analytics-context.json")
INPUT=os.environ.get("ANALYTICS_INPUT_TOPIC","analytics_contract_input")
OUTPUT=os.environ.get("ANALYTICS_RESULTS_TOPIC","analytics_contract_results")
GROUP=os.environ.get("ANALYTICS_GROUP","restaurant-analytics-contract-v1")
def stamp(value):return datetime.fromtimestamp(value,timezone.utc).isoformat().replace("+00:00","Z")
def wait(check,seconds=120):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        value=check()
        if value:return value
        time.sleep(1)
    raise AssertionError("Timed out waiting for actual Flink/Kafka proof.")
def event(rid,order,status,at):
    return {"schema_version":2,"event_id":str(uuid.uuid4()),"order_id":order,"restaurant_id":rid,"status":status,"timestamp":stamp(at)}
def publish(values):
    producer=Producer(kafka_config("analytics-contract-producer"));offsets=[]
    def confirmed(error,message):
        if error:raise AssertionError("Fixture was not acknowledged by Kafka.")
        offsets.append(message.offset()+1)
    for value in values:
        producer.produce(INPUT,json.dumps(value).encode(),partition=0,on_delivery=confirmed)
    assert producer.flush(15)==0 and len(offsets)==len(values)
    return max(offsets)
def ready():
    try:
        data=json.load(urlopen("http://flink-contract:8081/jobs/overview",timeout=4))
        return next((v["jid"] for v in data["jobs"] if v["state"]=="RUNNING"),None)
    except Exception:return None
def checkpoint_offset(offset):
    consumer=Consumer({**kafka_config("analytics-checkpoint-proof"),"group.id":GROUP,"enable.auto.commit":False})
    try:return consumer.committed([TopicPartition(INPUT,0)],timeout=5)[0].offset>=offset
    finally:consumer.close()
def main():
    parser=argparse.ArgumentParser();parser.add_argument("phase",choices=["init","prepare","finish"]);args=parser.parse_args()
    assert os.environ["APP_ENV"]=="local" and os.environ["KAFKA_BOOTSTRAP_SERVERS"]=="kafka:9092"
    if args.phase=="init":
        import re
        assert re.fullmatch(r"analytics_contract_input_[a-f0-9]{12}",INPUT) and re.fullmatch(r"analytics_contract_results_[a-f0-9]{12}",OUTPUT)
        admin=AdminClient(kafka_config("analytics-contract-init"))
        for future in admin.create_topics([NewTopic(name,num_partitions=1,replication_factor=1) for name in (INPUT,OUTPUT)]).values():future.result(15)
        print("Dedicated run-specific test topics created.",flush=True);return
    jid=wait(ready)
    if args.phase=="prepare":
        start=(int(time.time())//60-2)*60
        rid="contract-"+uuid.uuid4().hex
        first=event(rid,str(uuid.uuid4()),"PENDING_PAYMENT",start+5)
        second=event(rid,first["order_id"],"PAID",start+10)
        third=event(rid,str(uuid.uuid4()),"PENDING_PAYMENT",start+15)
        offset=publish([first,first,second,{"schema_version":"invalid"}])
        wait(lambda:checkpoint_offset(offset))
        CONTEXT.parent.mkdir(parents=True,exist_ok=True)
        CONTEXT.write_text(json.dumps({"restaurant_id":rid,"start":start,"third":third,"checkpoint_input_offset":offset,"job_id":jid}))
        print("Duplicate source event and partial window reached a completed checkpoint: PASS",flush=True)
    else:
        value=json.loads(CONTEXT.read_text());assert value["job_id"]==jid
        # Restarting only the task manager must restore the prior checkpointed state.
        publish([value["third"],event("watermark-"+uuid.uuid4().hex,str(uuid.uuid4()),"PENDING_PAYMENT",time.time())])
        consumer=Consumer({**kafka_config("analytics-result-proof"),"group.id":"analytics-proof-"+uuid.uuid4().hex,"enable.auto.commit":False,"auto.offset.reset":"earliest","isolation.level":"read_committed"})
        consumer.subscribe([OUTPUT]);found=[]
        deadline=time.monotonic()+140
        try:
            while time.monotonic()<deadline:
                message=consumer.poll(1)
                if message is None or message.error():continue
                row=validate(message.value())
                if row["restaurant_id"]==value["restaurant_id"]:
                    found.append(row)
                    assert row["event_count"]==3 and row["unique_order_count"]==2
                    assert row["stage_counts"]=={"PAID":1,"PENDING_PAYMENT":2}
                    assert row["window_start"]==stamp(value["start"]) and row["window_seconds"]==60
                    break
            assert len(found)==1,"No committed checkpoint-restored aggregate."
            # Allow another checkpoint to reveal accidental duplicate committed outputs.
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                message=consumer.poll(1)
                if message and not message.error() and json.loads(message.value()).get("restaurant_id")==value["restaurant_id"]:
                    raise AssertionError("Duplicate committed aggregate after checkpoint recovery.")
        finally:consumer.close()
        report={"result":"PASS","window_seconds":60,"event_count":3,"unique_order_count":2,"source_duplicates_deduplicated":True,"partial_window_restored_after_taskmanager_restart":True,"committed_result_count":1,"observed_at":stamp(time.time())}
        Path("/evidence/flink.json").write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report),flush=True)
if __name__=="__main__":main()
