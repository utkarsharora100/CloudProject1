import os
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from confluent_kafka import Producer

app = Flask(__name__)

# Database Configuration
DB_USER = os.environ.get("DB_USER", "order_service_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "dev_password")
PRIVATE_IP = os.environ.get("DB_PRIVATE_IP", "127.0.0.1")
DB_NAME = os.environ.get("DB_NAME", "orders_db")

app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{PRIVATE_IP}:3306/{DB_NAME}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Kafka Configuration (Injected from Terraform outputs)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET")

if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET]):
    raise ValueError("Missing Kafka configuration environment variables.")

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'client.id': 'order-service-producer'
}
kafka_producer = Producer(producer_conf)

class IdempotencyKey(db.Model):
    key = db.Column(db.String(36), primary_key=True)
    response_body = db.Column(db.Text, nullable=False)
    status_code = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.Integer, nullable=False)
    restaurant_id = db.Column(db.Integer, nullable=False)
    total_amount = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), default="Pending")

with app.app_context():
    db.create_all()

def delivery_report(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "order-service"}), 200

@app.route('/orders', methods=['POST'])
def create_order():
    idem_key = request.headers.get('Idempotency-Key')
    if not idem_key:
        return jsonify({"error": "Idempotency-Key header is required"}), 400
        
    existing_request = IdempotencyKey.query.filter_by(key=idem_key).first()
    if existing_request:
        return jsonify(json.loads(existing_request.response_body)), existing_request.status_code
        
    data = request.get_json()
    if not data or not all(k in data for k in ("customer_id", "restaurant_id", "total_amount")):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        new_order = Order(
            customer_id=data['customer_id'],
            restaurant_id=data['restaurant_id'],
            total_amount=data['total_amount']
        )
        db.session.add(new_order)
        db.session.flush() 
        
        response_data = {"status": "success", "order_id": new_order.id, "message": "Order created"}
        
        idem_record = IdempotencyKey(
            key=idem_key,
            response_body=json.dumps(response_data),
            status_code=201
        )
        db.session.add(idem_record)
        db.session.commit()
        
        # Publish Event to Kafka
        event_payload = {
            "order_id": new_order.id,
            "customer_id": new_order.customer_id,
            "restaurant_id": new_order.restaurant_id,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "OrderCreated"
        }
        
        kafka_producer.produce(
            topic='orders',
            key=str(new_order.id),
            value=json.dumps(event_payload),
            callback=delivery_report
        )
        kafka_producer.poll(0) # Trigger callback processing
        
        return jsonify(response_data), 201
        
    except Exception as e:
        db.session.rollback()
        print(f"Error creating order: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081)