from common.deployment import service_endpoint
"""Validate current Customer/Restaurant records without exposing their errors."""
from copy import deepcopy
import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import grpc
from werkzeug.exceptions import Conflict, ServiceUnavailable, UnprocessableEntity
from restaurant_service import restaurant_pb2 as pb, restaurant_pb2_grpc as rpc


class OrderDependencies:
    def __init__(self):
        self.customer_base = os.environ.get("CUSTOMER_API_URL", "http://127.0.0.1:8083")
        target = os.environ.get("RESTAURANT_GRPC_TARGET", "127.0.0.1:50051")
        service_endpoint(self.customer_base,{"http://customer-service:8083","http://127.0.0.1:8083","http://localhost:8083"})
        service_endpoint(target,{"restaurant-service:50051","127.0.0.1:50051","localhost:50051"},grpc=True)
        self.channel = grpc.insecure_channel(target)
        self.restaurant = rpc.RestaurantServiceStub(self.channel)

    def close(self):
        self.channel.close()

    def customer_get(self, path, field):
        try:
            with urlopen(self.customer_base + path, timeout=5) as response:
                return json.load(response)[field]
        except HTTPError as error:
            if error.code == 404:
                raise UnprocessableEntity("Customer or delivery address does not exist for this customer.") from None
            raise ServiceUnavailable("Customer validation is temporarily unavailable.") from None
        except (URLError, TimeoutError, ValueError, KeyError, TypeError):
            raise ServiceUnavailable("Customer validation is temporarily unavailable.") from None

    def quote(self, data):
        customer = self.customer_get(f"/customers/{data['customer_id']}", "customer")
        address = self.customer_get(f"/customers/{data['customer_id']}/addresses/{data['address_id']}", "address")
        if customer.get("id") != data["customer_id"] or address.get("id") != data["address_id"] or address.get("customer_id") != data["customer_id"]:
            raise ServiceUnavailable("Customer validation returned an inconsistent result.")
        try:
            menu = self.restaurant.GetMenu(pb.RestaurantRequest(restaurant_id=data["restaurant_id"]), timeout=5)
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.NOT_FOUND:
                raise UnprocessableEntity("Restaurant does not exist.") from None
            raise ServiceUnavailable("Restaurant validation is temporarily unavailable.") from None
        if not menu.is_open:
            raise Conflict("Restaurant is currently closed.")
        if menu.restaurant_id != data["restaurant_id"] or menu.currency != "INR" or menu.version < 1 or not 1 <= menu.estimated_prep_time_mins <= 240:
            raise ServiceUnavailable("Restaurant validation returned an unsupported result.")
        indexed = {item.item_id: item for item in menu.items}
        if len(indexed) != len(menu.items):
            raise ServiceUnavailable("Restaurant validation returned an inconsistent menu.")
        items = []
        for requested in data["items"]:
            item = indexed.get(requested["item_id"])
            if item is None:
                raise UnprocessableEntity("A requested menu item does not exist.")
            if not item.HasField("available") or not item.HasField("price_minor"):
                raise ServiceUnavailable("Restaurant menu contract is not supported.")
            if not item.available:
                raise Conflict("A requested menu item is unavailable.")
            if not 1 <= item.price_minor <= 100000000:
                raise ServiceUnavailable("Restaurant menu contains an invalid price.")
            items.append({"item_id": item.item_id, "name": item.name,
                          "quantity": requested["quantity"], "unit_price_minor": item.price_minor,
                          "line_total_minor": item.price_minor * requested["quantity"]})
        delivery = {key: deepcopy(address.get(key)) for key in (
            "label", "line1", "line2", "city", "region", "postal_code", "country_code")}
        delivery.update(address_id=data["address_id"], recipient_name=customer.get("name"), phone=customer.get("phone"))
        return {"delivery_snapshot": delivery,
                "restaurant_snapshot": {"restaurant_id": menu.restaurant_id, "name": menu.name,
                    "estimated_prep_time_mins": menu.estimated_prep_time_mins, "version": menu.version,
                    "currency": menu.currency},
                "items_snapshot": items, "total_minor": sum(item["line_total_minor"] for item in items),
                "currency": menu.currency}
