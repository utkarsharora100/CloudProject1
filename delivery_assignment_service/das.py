import os
import json
from flask import Flask, request, jsonify
from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout
from confluent_kafka import Producer

app = Flask(__name__)

# Updated for GKE: Defaults to standard Kubernetes internal DNS for a ZooKeeper service
ZK_HOSTS = os.environ.get("ZK_HOSTS", "zookeeper-client:2181")
zk = KazooClient(hosts=ZK_HOSTS)
zk.start()

# Ensure base ZooKeeper paths exist
zk.ensure_path("/delivery/locks/orders")
zk.ensure_path("/delivery/locks/drivers")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET")

# Initialize Kafka Producer
kafka_producer = None
if KAFKA_BOOTSTRAP_SERVERS and KAFKA_API_KEY and KAFKA_API_SECRET:
    kafka_producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
        'client.id': 'delivery-assignment-service'
    })

def delivery_report(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "zookeeper_connected": zk.connected,
        "service": "delivery-assignment"
    }), 200

@app.route('/assign', methods=['POST'])
def assign_delivery():
    data = request.get_json()
    order_id = data.get('order_id')
    driver_id = data.get('driver_id')

    if not order_id or not driver_id:
        return jsonify({"error": "Missing order_id or driver_id"}), 400

    # Define distributed locks
    order_lock = zk.Lock(f"/delivery/locks/orders/{order_id}", "assignment_worker")
    driver_lock = zk.Lock(f"/delivery/locks/drivers/{driver_id}", "assignment_worker")

    try:
        # Attempt to acquire both locks with a 2-second timeout to prevent deadlocks
        if order_lock.acquire(timeout=2) and driver_lock.acquire(timeout=2):
            
            # CRITICAL SECTION: Safe to assign the driver
            assignment_record = {
                "order_id": order_id,
                "driver_id": driver_id,
                "status": "Assigned_To_Driver"
            }
            
            # Publish the assignment event to Kafka for downstream consumers
            if kafka_producer:
                kafka_producer.produce(
                    topic='delivery_assignments',
                    key=str(order_id),
                    value=json.dumps(assignment_record),
                    callback=delivery_report
                )
                kafka_producer.poll(0) # Trigger callback processing
            
            return jsonify({"status": "success", "data": assignment_record}), 200
        else:
            return jsonify({"error": "Resource is locked; assignment in progress elsewhere"}), 409
            
    except LockTimeout:
        return jsonify({"error": "Timeout acquiring ZooKeeper locks"}), 409
        
    finally:
        # Guarantee locks are released regardless of success or failure
        if order_lock.is_acquired:
            order_lock.release()
        if driver_lock.is_acquired:
            driver_lock.release()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082)