"""Ordered, additive schema initialization; never drops tables or existing fixtures."""
import importlib
import os
from common.deployment import require_profile,firestore_client
def main():
    require_profile()
    from customer_service.customer_service import create_app as customer_app
    from customer_service.models import db as customers
    app=customer_app({"AUTO_CREATE_SCHEMA":False})
    with app.app_context():customers.create_all()
    for name in ["order_service.schema","payment_service.schema","kitchen_workflow.schema","delivery_assignment_service.schema","notification_service.schema","task_scheduler.schema"]:
        importlib.import_module(name).main()
    # Required bridge readiness document, deliberately synthetic and created only when absent.
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import firestore
    client=firestore_client()
    try:client.collection("restaurants").document("1").create({"name":"Campus Kitchen","description":"Synthetic assignment menu","currency":"INR","is_open":True,"estimated_prep_time_mins":15,"version":1,"menu":[{"item_id":"veg-meal","name":"Veg meal","price_minor":12900,"available":True,"description":""}],"created_at":firestore.SERVER_TIMESTAMP,"updated_at":firestore.SERVER_TIMESTAMP},timeout=5)
    except AlreadyExists:pass
    print("Ordered additive schemas and synthetic bridge fixture are ready.",flush=True)
if __name__=="__main__":main()
