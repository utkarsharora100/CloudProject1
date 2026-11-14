"""Commit Kafka offsets only after SQL materialization or durable quarantine."""
import time
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition
from common.kafka import kafka_config
from order_service.storage import create_storage_app
from order_service.models import db
from task_scheduler.runtime import heartbeat
from notification_service.ingest import consume_record

HEARTBEAT=Path("/tmp/notification-consumer-heartbeat.json")

def cycle(app):
    config=kafka_config("notification-consumer")
    if __import__("os").environ.get("APP_ENV")!="cloud" and (config["bootstrap.servers"]!="kafka:9092" or config["security.protocol"]!="PLAINTEXT"):
        raise RuntimeError("Notification consumer requires the isolated local broker.")
    config.update({"group.id":"notification-service-v1","enable.auto.commit":False,
        "auto.offset.reset":"earliest","log_level":0,"error_cb":lambda error:None})
    consumer=Consumer(config)
    consumer.subscribe(["orders","delivery_assignments"])
    try:
        with app.app_context():
            while True:
                heartbeat(HEARTBEAT,{"running":True})
                message=consumer.poll(0.5)
                if message is None or message.error():continue
                try:
                    consume_record(message.topic(),message.partition(),message.offset(),message.value())
                    consumer.commit(message=message,asynchronous=False)
                except Exception:
                    db.session.rollback()
                    # A failed seek after rebalance restarts this consumer cycle.
                    # SQL receipts make both offset replay and unknown commits safe.
                    consumer.seek(TopicPartition(message.topic(),message.partition(),message.offset()))
                    print("Notification event processing deferred safely.",flush=True)
                    time.sleep(1)
    finally:
        consumer.close()

def main():
    app=create_storage_app()
    while True:
        try:cycle(app)
        except KeyboardInterrupt:return
        except Exception:
            print("Notification consumer reconnecting safely.",flush=True)
            time.sleep(0.5)

if __name__=="__main__":
    main()
