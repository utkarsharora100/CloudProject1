"""Monotonic Firestore view updates preserve the established paid-event identity."""
from google.cloud import firestore

def should_apply(existing, incoming):
    if existing is None:
        return True
    if existing.get("source_event_id") != incoming["source_event_id"]:
        raise ValueError("Kitchen projection source requires reconciliation.")
    version = existing.get("kitchen_version", 0)
    if type(version) is not int:
        raise ValueError("Kitchen projection version requires reconciliation.")
    return version < incoming["kitchen_version"]

class ProjectionRepository:
    def __init__(self, client):
        self.client = client

    def put(self, payload):
        ref = self.client.collection("restaurants").document(payload["restaurant_id"]).collection("kitchen_tickets").document(payload["order_id"])
        @firestore.transactional
        def update(transaction):
            snapshot = ref.get(transaction=transaction, retry=None, timeout=3)
            existing = snapshot.to_dict() if snapshot.exists else None
            if not should_apply(existing, payload):
                return False
            fields = {key: value for key, value in payload.items() if key != "restaurant_id"}
            if existing is not None:
                # Retain the original bridge receipt time and unrelated fields.
                fields.pop("received_at", None)
                transaction.update(ref, fields)
            else:
                transaction.create(ref, fields)
            return True
        return update(self.client.transaction(max_attempts=5))
