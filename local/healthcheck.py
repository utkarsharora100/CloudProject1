"""Small readiness probes used by the local Compose stack."""
import json
import os
import sys
from urllib.request import urlopen


def main():
    service = sys.argv[1]
    if service in {"order", "delivery", "customer", "restaurant-http", "task-api", "payment", "kitchen-api", "notification"}:
        port = {"order": 8081, "delivery": 8082, "customer": 8083, "restaurant-http": 8084, "task-api": 8085, "payment": 8086, "kitchen-api": 8087, "notification": 8088}[service]
        with urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as response:
            data = json.load(response)
        if service == "customer" and (data.get("status") != "healthy" or data.get("service") != "customer-service"):
            raise RuntimeError("Customer Service is not ready.")
        if service == "restaurant-http" and (data.get("service") != "restaurant-api" or not data.get("grpc_connected")):
            raise RuntimeError("Restaurant HTTP/gRPC connection is not ready.")
        if service == "task-api" and (data.get("service") != "task-api" or not data.get("zookeeper_connected")):
            raise RuntimeError("Task API coordination is not ready.")
        if service == "payment" and (data.get("service") != "payment-service" or not data.get("zookeeper_connected")):
            raise RuntimeError("Payment API is not ready.")
        if service == "kitchen-api" and (data.get("service") != "kitchen-api" or not data.get("zookeeper_connected")):
            raise RuntimeError("Kitchen API is not ready.")
        if service == "delivery" and not data.get("zookeeper_connected"):
            raise RuntimeError("ZooKeeper is disconnected.")
        if service == "notification" and (data.get("service") != "notification-service" or not data.get("zookeeper_connected")):
            raise RuntimeError("Notification API is not ready.")
    elif service in {"notification-worker", "notification-consumer", "notification-reconcile"}:
        import time
        if service == "notification-worker":
            from notification_service.worker import HEARTBEAT
        elif service == "notification-consumer":
            from notification_service.consumer import HEARTBEAT
        else:
            from notification_service.reconcile import HEARTBEAT
        data=json.loads(HEARTBEAT.read_text(encoding="utf-8"))
        if time.time()-data["at"]>15 or not data.get("connected",data.get("running",False)):
            raise RuntimeError("Notification process heartbeat is stale.")
    elif service in {"kitchen-stage-worker", "kitchen-sync"}:
        import time
        if service == "kitchen-stage-worker":
            from kitchen_workflow.worker import HEARTBEAT
        else:
            from kitchen_workflow.sync import HEARTBEAT
        data = json.loads(HEARTBEAT.read_text(encoding="utf-8"))
        if time.time() - data["at"] > 15 or not data.get("connected", data.get("running", False)):
            raise RuntimeError("Kitchen worker heartbeat is stale.")
    elif service == "delivery-worker":
        import time
        from delivery_assignment_service.worker import HEARTBEAT
        data = json.loads(HEARTBEAT.read_text(encoding="utf-8"))
        if time.time() - data["at"] > 15 or not data["connected"]:
            raise RuntimeError("Delivery worker heartbeat is stale.")
    elif service == "delivery-outbox":
        import time
        from delivery_assignment_service.outbox import HEARTBEAT
        from delivery_assignment_service.models import Event
        from order_service.models import db
        from order_service.storage import create_storage_app
        if not HEARTBEAT.exists() or time.time()-HEARTBEAT.stat().st_mtime > 30:
            raise RuntimeError("Delivery outbox heartbeat is stale.")
        app = create_storage_app()
        with app.app_context():
            db.session.query(Event.event_id).limit(1).all()
    elif service == "payment-worker":
        import time
        from payment_service.worker import HEARTBEAT
        data = json.loads(HEARTBEAT.read_text(encoding="utf-8"))
        if time.time() - data["at"] > 10 or not data["connected"]:
            raise RuntimeError("Payment worker heartbeat is stale.")
    elif service in {"task-leader", "task-worker"}:
        import time
        from task_scheduler.runtime import LEADER_HEARTBEAT, WORKER_HEARTBEAT
        path = LEADER_HEARTBEAT if service == "task-leader" else WORKER_HEARTBEAT
        data = json.loads(path.read_text(encoding="utf-8"))
        if time.time() - data["at"] > 10 or not data["connected"]:
            raise RuntimeError("Task coordination heartbeat is stale or disconnected.")
        if service == "task-leader":
            from task_scheduler.models import LeaderState, db
            from task_scheduler.storage import create_storage_app, database_now
            app = create_storage_app()
            with app.app_context():
                row = db.session.get(LeaderState, 1)
                if data["role"] == "leader" and (row.leader_id != data["instance_id"] or row.epoch != data["epoch"] or row.expires_at <= database_now()):
                    raise RuntimeError("Scheduler leader heartbeat is stale.")
    elif service == "order-outbox":
        import time
        from order_service.outbox import HEARTBEAT
        from order_service.storage import create_storage_app
        from order_service.models import Outbox, db
        if not HEARTBEAT.exists() or time.time() - HEARTBEAT.stat().st_mtime > 30:
            raise RuntimeError("Order outbox heartbeat is stale.")
        app = create_storage_app()
        with app.app_context():
            db.session.query(Outbox.event_id).limit(1).all()
    elif service == "restaurant":
        import grpc
        from restaurant_service import restaurant_pb2, restaurant_pb2_grpc
        with grpc.insecure_channel("127.0.0.1:50051") as channel:
            restaurant_pb2_grpc.RestaurantServiceStub(channel).ListRestaurants(
                restaurant_pb2.ListRequest(page_size=1), timeout=3)
    elif service == "kitchen":
        from confluent_kafka.admin import AdminClient
        from common.kafka import kafka_config
        from google.cloud import firestore
        from local.environment import require_local_emulator
        from common.deployment import firestore_client
        database=firestore_client()
        metadata = AdminClient(kafka_config("kitchen-health")).list_topics(timeout=3)
        if "orders" not in metadata.topics:
            raise RuntimeError("The orders topic is not ready.")
        if not database.collection("restaurants").document("1").get(retry=None, timeout=3).exists:
            raise RuntimeError("Firestore seed data is not ready.")
    else:
        raise ValueError("Unknown local readiness probe.")


if __name__ == "__main__":
    main()
