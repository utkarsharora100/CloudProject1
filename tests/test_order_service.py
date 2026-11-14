from copy import deepcopy
import json
import os
import unittest
import uuid
from unittest.mock import Mock, patch
import grpc
from sqlalchemy.exc import SQLAlchemyError
from urllib.error import HTTPError, URLError
from werkzeug.exceptions import Conflict, ServiceUnavailable, UnprocessableEntity
from order_fixtures import DemoDependencies, PAYLOAD, make_app
from order_service.dependencies import OrderDependencies
from order_service.domain import create_request, request_hash
from order_service.event_consumer import kitchen_trigger
from order_service.models import Idempotency, Order, OrderEvent, Outbox, db
from order_service.storage import create_storage_app, database_url
from restaurant_service import restaurant_pb2 as pb


class OrderAPITests(unittest.TestCase):
    def setUp(self):
        self.dependencies = DemoDependencies()
        self.app = make_app(self.dependencies)
        self.context = self.app.app_context()
        self.context.push()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.context.pop()

    def create(self, payload=None, key="create-test-key"):
        return self.client.post("/orders", json=deepcopy(PAYLOAD if payload is None else payload),
                                headers={"Idempotency-Key": key})

    def transition(self, order, target, key=None, version=None):
        return self.client.post(f"/orders/{order['order_id']}/transitions",
            json={"to_state": target, "expected_version": order['version'] if version is None else version},
            headers={"Idempotency-Key": key or str(uuid.uuid4())})

    def test_create_commits_order_history_idempotency_and_outbox(self):
        response = self.create()
        self.assertEqual(response.status_code, 201)
        body = response.get_json()
        self.assertEqual((body["status"], body["total_minor"], body["currency"]), ("PENDING_PAYMENT", 25998, "INR"))
        self.assertEqual(str(uuid.UUID(body["order_id"])), body["order_id"])
        self.assertEqual([Order.query.count(), OrderEvent.query.count(), Outbox.query.count(), Idempotency.query.count()], [1, 1, 1, 1])
        self.assertIsNone(Outbox.query.first().published_at)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_same_request_replays_before_dependencies_are_called(self):
        first = self.create()
        self.dependencies.failure = ServiceUnavailable("Dependency unavailable.")
        repeated = self.create()
        self.assertEqual(repeated.status_code, 201)
        self.assertEqual(repeated.get_json(), first.get_json())
        self.assertEqual(repeated.headers["Idempotency-Replayed"], "true")
        self.assertEqual(self.dependencies.calls, 1)
        self.assertEqual(Order.query.count(), 1)

    def test_changed_payload_with_same_key_conflicts(self):
        self.create()
        changed = deepcopy(PAYLOAD)
        changed["items"][0]["quantity"] = 3
        self.assertEqual(self.create(changed).status_code, 409)
        self.assertEqual(self.dependencies.calls, 1)
        self.assertEqual(Order.query.count(), 1)

    def test_create_key_is_scoped_by_customer(self):
        first = self.create().get_json()
        other = self.create({**PAYLOAD, "customer_id": 2}).get_json()
        self.assertNotEqual(first["order_id"], other["order_id"])
        self.assertEqual(Order.query.count(), 2)

    def test_idempotency_key_values_are_case_sensitive(self):
        first = self.create(key="CaseSensitive-Key").get_json()
        second = self.create(key="casesensitive-key").get_json()
        self.assertNotEqual(first["order_id"], second["order_id"])
        self.assertEqual(Order.query.count(), 2)

    def test_prices_and_delivery_are_immutable_snapshots(self):
        order = self.create().get_json()
        self.dependencies.price = 19999
        self.dependencies.line1 = "Changed synthetic street"
        saved = self.client.get(f"/orders/{order['order_id']}").get_json()["order"]
        self.assertEqual(saved["items"][0]["unit_price_minor"], 12999)
        self.assertEqual(saved["delivery_snapshot"]["line1"], "Synthetic demo street")
        self.assertEqual(saved["total_minor"], 25998)

    def test_no_client_totals_or_invalid_item_requests_are_accepted(self):
        invalid = [{**PAYLOAD, "total_amount": 0}, {**PAYLOAD, "total_minor": 1},
                   {**PAYLOAD, "customer_id": True}, {**PAYLOAD, "restaurant_id": 1},
                   {**PAYLOAD, "items": []}, {**PAYLOAD, "items": [{"item_id": "meal", "quantity": True}]},
                   {**PAYLOAD, "items": [{"item_id": "meal", "quantity": -1}]},
                   {**PAYLOAD, "items": PAYLOAD["items"] * 2}]
        for payload in invalid:
            self.assertEqual(self.create(payload).status_code, 400)
        self.assertEqual(Order.query.count(), 0)

    def test_idempotency_key_is_required_and_bounded(self):
        for value in (None, "short", "x" * 129):
            headers = {} if value is None else {"Idempotency-Key": value}
            self.assertEqual(self.client.post("/orders", json=PAYLOAD, headers=headers).status_code, 400)
        self.assertEqual(self.dependencies.calls, 0)

    def test_dependency_failure_leaves_no_partial_records(self):
        self.dependencies.failure = ServiceUnavailable("Validation temporarily unavailable.")
        self.assertEqual(self.create().status_code, 503)
        self.assertEqual([Order.query.count(), Outbox.query.count(), Idempotency.query.count()], [0, 0, 0])

    def test_database_failure_rolls_back_entire_atomic_write_and_redacts_logs(self):
        private = "dummy-private-database-detail"
        with patch.object(db.session, "commit", side_effect=SQLAlchemyError(private)):
            with self.assertLogs(self.app.logger, level="ERROR") as logs:
                response = self.create()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(private, response.get_data(as_text=True))
        self.assertNotIn(private, " ".join(logs.output))
        self.assertEqual([Order.query.count(), OrderEvent.query.count(), Outbox.query.count(), Idempotency.query.count()], [0, 0, 0, 0])

    def test_failed_payment_can_retry_then_progress_through_delivery(self):
        current = self.create().get_json()
        states = ["PAYMENT_FAILED", "PENDING_PAYMENT", "PAID", "KITCHEN_ACCEPTED", "PREPARING", "READY",
                  "COURIER_ASSIGNED", "PICKED_UP", "DELIVERED"]
        for state in states:
            response = self.transition(current, state)
            self.assertEqual(response.status_code, 200)
            current = response.get_json()
            self.assertEqual(current["status"], state)
        events = self.client.get(f"/orders/{current['order_id']}/events").get_json()["events"]
        self.assertEqual([event["sequence"] for event in events], list(range(1, 11)))
        self.assertEqual(len({event["event_id"] for event in events}), 10)
        self.assertTrue(all(not event["publication"]["published"] for event in events))
        self.assertEqual(self.transition(current, "CANCELLED").status_code, 409)

    def test_invalid_or_stale_transition_adds_no_events(self):
        order = self.create().get_json()
        self.assertEqual(self.transition(order, "READY").status_code, 409)
        self.assertEqual(self.transition(order, "PAID", version=2).status_code, 409)
        self.assertEqual(OrderEvent.query.count(), 1)
        self.assertEqual(Outbox.query.count(), 1)

    def test_transition_replay_is_safe_and_conflicting_payload_is_rejected(self):
        order = self.create().get_json()
        first = self.transition(order, "PAID", key="transition-replay")
        replay = self.transition(order, "PAID", key="transition-replay")
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(first.get_json(), replay.get_json())
        self.assertEqual(OrderEvent.query.count(), 2)
        self.assertEqual(self.transition(order, "PAYMENT_FAILED", key="transition-replay").status_code, 409)

    def test_unpaid_cancellation_and_paid_refund_rules(self):
        first = self.create().get_json()
        cancelled = self.transition(first, "CANCELLED").get_json()
        self.assertEqual(cancelled["status"], "CANCELLED")
        self.assertEqual(self.transition(cancelled, "PAID").status_code, 409)
        paid = self.transition(self.create(key="second-create-key").get_json(), "PAID").get_json()
        self.assertEqual(self.transition(paid, "CANCELLED").status_code, 409)
        refund = self.transition(paid, "REFUND_PENDING").get_json()
        self.assertEqual(self.transition(refund, "REFUNDED").get_json()["status"], "REFUNDED")

    def test_order_listing_is_customer_scoped_and_paginated(self):
        first = self.create().get_json()
        self.create(key="second-create-key")
        other = self.create({**PAYLOAD, "customer_id": 2}, key="other-create-key").get_json()
        self.assertEqual(self.client.get("/orders").status_code, 400)
        page = self.client.get("/orders?customer_id=1&limit=1").get_json()
        self.assertEqual(len(page["orders"]), 1)
        next_page = self.client.get("/orders?customer_id=1&limit=1&after=" + page["next_after"]).get_json()
        self.assertEqual(len(next_page["orders"]), 1)
        self.assertIsNone(next_page["next_after"])
        self.assertNotEqual(page["orders"][0]["order_id"], next_page["orders"][0]["order_id"])
        self.assertEqual(self.client.get("/orders?customer_id=1&after=" + other["order_id"]).status_code, 400)

    def test_events_omit_delivery_and_contact_data(self):
        self.create()
        payload = Outbox.query.first().payload
        text = json.dumps(payload)
        self.assertNotIn("Synthetic demo street", text)
        self.assertNotIn("recipient_name", text)
        self.assertNotIn("phone", text)
        self.assertEqual(payload["schema_version"], 2)
        self.assertEqual(payload["event_type"], "OrderCreated")

    def test_reordered_distinct_items_have_the_same_request_hash(self):
        a = {**PAYLOAD, "items": [{"item_id": "a", "quantity": 1}, {"item_id": "b", "quantity": 2}]}
        b = {**a, "items": list(reversed(a["items"]))}
        self.assertEqual(request_hash(create_request(a)), request_hash(create_request(b)))

    def test_missing_orders_and_malformed_json_are_handled(self):
        self.assertEqual(self.client.get("/orders/" + str(uuid.uuid4())).status_code, 404)
        self.assertEqual(self.client.get("/orders/not-a-uuid").status_code, 400)
        self.assertEqual(self.client.post("/orders", data="text", content_type="text/plain").status_code, 415)
        self.assertEqual(self.client.post("/orders", data="{bad", content_type="application/json").status_code, 400)
        self.assertEqual(self.client.post("/orders", json={"x": "x" * 33000}).status_code, 413)

    def test_health_checks_database_without_kafka(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["publication"], "transactional-outbox")


class DependencyContractTests(unittest.TestCase):
    def setUp(self):
        self.client = OrderDependencies.__new__(OrderDependencies)
        self.client.customer_base = "http://customer-service:8083"
        self.client.customer_get = Mock(side_effect=lambda path, field: {"id": 1, "name": "Demo", "phone": None} if field == "customer" else {
            "id": 1, "customer_id": 1, "line1": "Synthetic street", "city": "Demo City", "country_code": "IN"})
        self.client.restaurant = Mock()
        self.menu = pb.MenuResponse(restaurant_id="demo", name="Demo", currency="INR", version=2,
            is_open=True, estimated_prep_time_mins=18,
            items=[pb.MenuItem(item_id="meal", name="Demo meal", price=1.0, price_minor=12999, available=True)])
        self.client.restaurant.GetMenu.return_value = self.menu

    def test_quote_uses_integer_menu_prices_and_address_snapshot(self):
        result = self.client.quote(PAYLOAD)
        self.assertEqual(result["total_minor"], 25998)
        self.assertEqual(result["items_snapshot"][0]["unit_price_minor"], 12999)
        self.assertEqual(result["restaurant_snapshot"]["estimated_prep_time_mins"], 18)
        self.assertNotIn("email", result["delivery_snapshot"])

    def test_wrong_address_customer_is_rejected(self):
        self.client.customer_get.side_effect = lambda path, field: {"id": 1} if field == "customer" else {"id": 1, "customer_id": 2}
        with self.assertRaises(ServiceUnavailable):
            self.client.quote(PAYLOAD)

    def test_closed_restaurant_and_unavailable_item_are_rejected(self):
        self.menu.is_open = False
        with self.assertRaises(Conflict):
            self.client.quote(PAYLOAD)
        self.menu.is_open = True
        self.menu.items[0].available = False
        with self.assertRaises(Conflict):
            self.client.quote(PAYLOAD)

    def test_missing_item_or_missing_minor_price_is_rejected(self):
        with self.assertRaises(UnprocessableEntity):
            self.client.quote({**PAYLOAD, "items": [{"item_id": "missing", "quantity": 1}]})
        self.menu.items[0].ClearField("price_minor")
        with self.assertRaises(ServiceUnavailable):
            self.client.quote(PAYLOAD)

    def test_customer_http_errors_are_sanitized(self):
        for error, expected in ((HTTPError("local", 404, "dummy-private", None, None), UnprocessableEntity),
                                (URLError("dummy-private"), ServiceUnavailable)):
            with patch("order_service.dependencies.urlopen", side_effect=error):
                with self.assertRaises(expected) as raised:
                    OrderDependencies.customer_get(self.client, "/customers/1", "customer")
            self.assertNotIn("dummy-private", raised.exception.description)


class OrderConfigurationTests(unittest.TestCase):
    def test_public_mode_and_cloud_db_host_are_rejected(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            create_storage_app({"SQLALCHEMY_DATABASE_URI": "sqlite://"})
        with self.assertRaises(ValueError):
            database_url({"APP_ENV": "local", "DB_PASSWORD": "dummy-local-only", "DB_PRIVATE_IP": "remote.invalid"})

    def test_password_is_required_and_preserves_special_characters(self):
        with self.assertRaises(ValueError):
            database_url({})
        dummy = "local-test-only:@/#?"
        self.assertEqual(database_url({"DB_PASSWORD": dummy}).password, dummy)

    def test_kitchen_waits_for_v2_payment_confirmation(self):
        self.assertFalse(kitchen_trigger({"schema_version": 2, "event_type": "OrderCreated", "status": "PENDING_PAYMENT"}))
        self.assertFalse(kitchen_trigger({"status": "OrderCreated"}))
        self.assertTrue(kitchen_trigger({"schema_version": 2, "event_type": "OrderPaymentSucceeded", "status": "PAID"}))


if __name__ == "__main__":
    unittest.main()
