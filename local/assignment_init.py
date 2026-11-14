"""Create only isolated emulator resources; never reaches a cloud project."""
import os
from google.api_core.exceptions import AlreadyExists
from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1,storage
from confluent_kafka.admin import AdminClient,NewTopic
from common.kafka import kafka_config
from local.environment import require_local_emulator

def clients():
    require_local_emulator()
    if os.environ.get("PUBSUB_EMULATOR_HOST")!="pubsub:8085" or os.environ.get("STORAGE_EMULATOR_HOST")!="http://gcs:4443":
        raise RuntimeError("Assignment tools require both local event/object emulators.")
    credential=AnonymousCredentials()
    return pubsub_v1.PublisherClient(credentials=credential),pubsub_v1.SubscriberClient(credentials=credential),storage.Client(project="demo-food-delivery",credentials=credential)
def main():
    publisher,subscriber,objects=clients()
    topic=publisher.topic_path("demo-food-delivery","notification-dispatch")
    subscription=subscriber.subscription_path("demo-food-delivery","notification-function-local")
    for create,kwargs in [(publisher.create_topic,{"name":topic}),(subscriber.create_subscription,{"name":subscription,"topic":topic,"ack_deadline_seconds":30})]:
        try:create(request=kwargs,timeout=8)
        except AlreadyExists:pass
    try:objects.create_bucket("food-notification-receipts",timeout=8)
    except __import__("google.api_core.exceptions",fromlist=["Conflict"]).Conflict:pass
    admin=AdminClient(kafka_config("assignment-init"))
    topics=["restaurant_analytics_results","analytics_contract_input","analytics_contract_results"]
    for name,future in admin.create_topics([NewTopic(name,num_partitions=1,replication_factor=1) for name in topics]).items():
        try:future.result(10)
        except Exception as error:
            from confluent_kafka import KafkaException,KafkaError
            if not isinstance(error,KafkaException) or error.args[0].code()!=KafkaError.TOPIC_ALREADY_EXISTS:raise RuntimeError("Local topic initialization failed.") from None
    print("Local event subscription, durable receipt bucket and analytics topics are ready.",flush=True)
if __name__=="__main__":
    import time
    for attempt in range(12):
        try:
            main();break
        except Exception:
            if attempt==11:raise RuntimeError("Local assignment resources were not ready after bounded retries.") from None
            print("Waiting for local event/object resources.",flush=True);time.sleep(2)

