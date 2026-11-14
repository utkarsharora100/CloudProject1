from common.deployment import firestore_client
"""Firestore operations; each restaurant/menu change is a document transaction."""
from datetime import datetime, timezone
import os
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
from restaurant_service.domain import (DuplicateRecord, MissingRecord, StaleVersion,
                                      ValidationError, identifier, integer, menu_item,
                                      normalize, profile)


class RestaurantRepository:
    def __init__(self, client):
        self.client = client
        self.collection = client.collection("restaurants")

    def get(self, restaurant_id):
        snapshot = self.collection.document(identifier(restaurant_id)).get(retry=None, timeout=3)
        if not snapshot.exists:
            raise MissingRecord("Restaurant not found.")
        return normalize(snapshot)

    def list(self, page_size=20, page_token=""):
        integer(page_size, "page_size", 1, 50)
        query = self.collection.order_by("__name__").limit(page_size + 1)
        if page_token:
            query = query.start_after({"__name__": self.collection.document(identifier(page_token))})
        snapshots = list(query.stream(retry=None, timeout=3))
        page = [normalize(snapshot) for snapshot in snapshots[:page_size]]
        next_token = page[-1]["restaurant_id"] if len(snapshots) > page_size else ""
        return page, next_token

    def create(self, restaurant_id, payload):
        restaurant_id = identifier(restaurant_id)
        now = datetime.now(timezone.utc)
        data = {**profile(payload), "menu": [], "version": 1, "created_at": now, "updated_at": now}
        try:
            self.collection.document(restaurant_id).create(data, retry=None, timeout=3)
        except AlreadyExists:
            raise DuplicateRecord("Restaurant already exists.") from None
        return {"restaurant_id": restaurant_id, **data}

    def _change(self, restaurant_id, operation, expected_version=0):
        ref = self.collection.document(identifier(restaurant_id))
        integer(expected_version, "expected_version", 0, 9223372036854775806)

        @firestore.transactional
        def change(transaction):
            snapshot = ref.get(transaction=transaction, retry=None, timeout=3)
            if not snapshot.exists:
                raise MissingRecord("Restaurant not found.")
            data = normalize(snapshot)
            if expected_version and data["version"] != expected_version:
                raise StaleVersion("Restaurant version changed; reload before retrying.")
            updates = operation(data)
            updates.update(version=data["version"] + 1, updated_at=datetime.now(timezone.utc))
            transaction.update(ref, updates)
            return {**data, **updates}

        return change(self.client.transaction(max_attempts=5))

    def update(self, restaurant_id, payload, expected_version=0):
        clean = profile(payload, partial=True)
        return self._change(restaurant_id, lambda data: dict(clean), expected_version)

    def put_item(self, restaurant_id, item_id, payload, expected_version=0):
        item_id = identifier(item_id)
        item = {"item_id": item_id, **menu_item(payload)}

        def operation(data):
            items = list(data["menu"])
            for index, existing in enumerate(items):
                if existing["item_id"] == item_id:
                    items[index] = item
                    return {"menu": items}
            if len(items) >= 100:
                raise ValidationError("A restaurant can have at most 100 menu items.")
            return {"menu": [*items, item]}

        # Creation is inferred from the committed transaction's menu and a local
        # attempt result. Firestore may re-run the callback under contention.
        outcome = {}
        def with_outcome(data):
            outcome["created"] = not any(i["item_id"] == item_id for i in data["menu"])
            return operation(data)
        result = self._change(restaurant_id, with_outcome, expected_version)
        return result, item, outcome["created"]

    def remove_item(self, restaurant_id, item_id, expected_version=0):
        item_id = identifier(item_id)
        def operation(data):
            items = [item for item in data["menu"] if item["item_id"] != item_id]
            if len(items) == len(data["menu"]):
                raise MissingRecord("Menu item not found.")
            return {"menu": items}
        return self._change(restaurant_id, operation, expected_version)


def local_repository(environ=None):
    return RestaurantRepository(firestore_client(environ))
