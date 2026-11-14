"""Idempotent sample restaurant initialization for the local emulator only."""
import time
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists, DeadlineExceeded, ServiceUnavailable
from local.environment import require_local_emulator


def main():
    project = require_local_emulator()
    db = firestore.Client(project=project)
    restaurant = db.collection("restaurants").document("1")
    data = {
        "name": "Campus Kitchen", "is_open": True, "estimated_prep_time_mins": 15,
        "description": "Synthetic campus menu", "currency": "INR", "version": 1,
        "created_at": firestore.SERVER_TIMESTAMP, "updated_at": firestore.SERVER_TIMESTAMP,
        "menu": [
            {"item_id": "veg-meal", "name": "Veg meal", "price_minor": 12900, "available": True, "description": ""},
            {"item_id": "paneer-wrap", "name": "Paneer wrap", "price_minor": 9900, "available": True, "description": ""},
        ],
    }
    deadline = time.monotonic() + 60
    while True:
        try:
            restaurant.create(data, retry=None, timeout=3)
            print("Created local sample restaurant 1.", flush=True)
            return
        except AlreadyExists:
            print("Local restaurant 1 already exists; preserved its data.", flush=True)
            return
        except (DeadlineExceeded, ServiceUnavailable):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


if __name__ == "__main__":
    main()
