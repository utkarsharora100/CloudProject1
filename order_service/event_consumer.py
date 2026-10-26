import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from google.cloud import firestore

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET")
PROJECT_ID = os.environ.get("PROJECT_ID", "cloud-project-512416")

if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET]):
    raise ValueError("Missing Kafka configuration environment variables.")

kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
}

# Consumer specific configuration
consumer_conf = kafka_conf.copy()
consumer_conf.update({
    'group.id': 'kitchen-workflow-group',
    'auto.offset.reset': 'earliest'
})

# Initialize Kafka Clients and Firestore
consumer = Consumer(consumer_conf)
producer = Producer(kafka_conf)
db = firestore.Client(project=PROJECT_ID)

def delivery_report(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err is not None:
        print(f"Kitchen status update failed: {err}")
    else:
        print(f"Kitchen status updated in {msg.topic()} [{msg.partition()}]")

def process_order(order_data):
    """Saves the ticket to NoSQL and emits a status update."""
    order_id = str(order_data['order_id'])
    restaurant_id = str(order_data['restaurant_id'])
    
    # 1. Write to Managed NoSQL (Firestore)
    doc_ref = db.collection('restaurants').document(restaurant_id).collection('kitchen_tickets').document(order_id)
    ticket_state = {
        'order_id': order_id,
        'customer_id': order_data['customer_id'],
        'status': 'Accepted_By_Kitchen',
        'received_at': datetime.utcnow().isoformat(),
        'estimated_prep_time_mins': 15
    }
    doc_ref.set(ticket_state)
    print(f"Ticket saved to Firestore for Order {order_id}")

    # 2. Publish to Kitchen Status Topic
    status_event = {
        'order_id': order_id,
        'status': 'Accepted_By_Kitchen',
        'timestamp': datetime.utcnow().isoformat()
    }
    producer.produce(
        topic='kitchen_status',
        key=order_id,
        value=json.dumps(status_event),
        callback=delivery_report
    )
    producer.poll(0)

def main():
    consumer.subscribe(['orders'])
    print("Kitchen Workflow Service is polling for orders...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
                    
            # Message Processing
            try:
                order_event = json.loads(msg.value().decode('utf-8'))
                print(f"Received order event: {order_event['order_id']}")
                process_order(order_event)
                
                # Commit offset only after successful processing
                consumer.commit(asynchronous=False)
            except Exception as e:
                print(f"Failed to process message: {e}")
                
    except KeyboardInterrupt:
        print("Service shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()