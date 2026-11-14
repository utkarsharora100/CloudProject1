from copy import deepcopy
from order_service.event_producer import create_app


PAYLOAD = {"customer_id": 1, "address_id": 1, "restaurant_id": "demo",
           "items": [{"item_id": "meal", "quantity": 2}]}


class DemoDependencies:
    def __init__(self):
        self.calls = 0
        self.failure = None
        self.price = 12999
        self.line1 = "Synthetic demo street"

    def quote(self, request):
        self.calls += 1
        if self.failure:
            raise self.failure
        items = [{"item_id": item["item_id"], "name": "Demo meal", "quantity": item["quantity"],
                  "unit_price_minor": self.price, "line_total_minor": self.price * item["quantity"]}
                 for item in request["items"]]
        return deepcopy({"delivery_snapshot": {"address_id": request["address_id"], "line1": self.line1,
            "city": "Demo City", "postal_code": "000000", "country_code": "IN", "recipient_name": "Demo", "phone": None},
            "restaurant_snapshot": {"restaurant_id": request["restaurant_id"], "name": "Demo Kitchen",
                "version": 2, "estimated_prep_time_mins": 18, "currency": "INR"},
            "items_snapshot": items, "total_minor": sum(item["line_total_minor"] for item in items), "currency": "INR"})


def make_app(dependencies=None):
    return create_app({"TESTING": True, "APP_ENV": "local", "SQLALCHEMY_DATABASE_URI": "sqlite://",
                       "AUTO_CREATE_SCHEMA": True}, dependencies=dependencies or DemoDependencies())
