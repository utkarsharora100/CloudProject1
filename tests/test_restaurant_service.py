from datetime import datetime, timezone
from types import SimpleNamespace
import os
import unittest
from unittest.mock import Mock, patch
import grpc
from google.api_core.exceptions import ServiceUnavailable
from restaurant_service import restaurant_pb2 as pb, restaurant_pb2_grpc as rpc
from restaurant_service.domain import (InvalidStoredData, MissingRecord, StaleVersion,
                                      ValidationError, identifier, legacy_price_minor,
                                      menu_item, normalize, profile)
from restaurant_service.http_api import create_app
from restaurant_service.repository import RestaurantRepository, local_repository
from restaurant_service.restaurant_service import build_server


def fixture():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return {"restaurant_id": "demo", "name": "Demo Kitchen", "description": "",
            "is_open": True, "estimated_prep_time_mins": 15, "currency": "INR",
            "version": 2, "created_at": now, "updated_at": now,
            "menu": [{"item_id": "meal", "name": "Demo meal", "description": "",
                      "price_minor": 12999, "available": True}]}


class RestaurantValidationTests(unittest.TestCase):
    def test_create_defaults_and_patch_preserves_false(self):
        data = profile({"name": " Demo ", "estimated_prep_time_mins": 15})
        self.assertEqual((data["name"], data["is_open"], data["currency"]), ("Demo", False, "INR"))
        self.assertEqual(profile({"is_open": False}, partial=True), {"is_open": False})

    def test_invalid_profiles_and_estimates_are_rejected(self):
        for payload in ({}, [], {"name": "Demo"}, {"name": "", "estimated_prep_time_mins": 1},
                        {"name": "Demo", "estimated_prep_time_mins": True},
                        {"name": "Demo", "estimated_prep_time_mins": 241},
                        {"name": "Demo", "estimated_prep_time_mins": 1, "currency": "USD"},
                        {"name": "Demo", "estimated_prep_time_mins": 1, "is_open": "true"}):
            with self.subTest(kind=type(payload).__name__), self.assertRaises(ValidationError):
                profile(payload)
        with self.assertRaises(ValidationError):
            profile({"currency": "INR"}, partial=True)

    def test_price_minor_is_an_integer_and_availability_is_a_boolean(self):
        self.assertEqual(menu_item({"name": "Demo", "price_minor": 12999})["price_minor"], 12999)
        self.assertFalse(menu_item({"name": "Demo", "price_minor": 1, "available": False})["available"])
        for value in (True, 12.99, "12999", 0, -1, 100000001):
            with self.subTest(kind=type(value).__name__), self.assertRaises(ValidationError):
                menu_item({"name": "Demo", "price_minor": value})

    def test_item_identity_and_unknown_fields_are_not_mutable_body_fields(self):
        with self.assertRaises(ValidationError):
            menu_item({"name": "Demo", "price_minor": 10, "item_id": "other"})
        for value in ("", "a/b", "..", "a" * 65, 1):
            with self.assertRaises(ValidationError):
                identifier(value)

    def test_legacy_rupee_prices_convert_exactly_and_invalid_values_fail(self):
        self.assertEqual(legacy_price_minor(129.0), 12900)
        self.assertEqual(legacy_price_minor(129.99), 12999)
        for value in (12.345, float("nan"), float("inf"), True, -1):
            with self.assertRaises(InvalidStoredData):
                legacy_price_minor(value)

    def test_original_seed_document_remains_readable(self):
        now = datetime.now(timezone.utc)
        old = {"name": "Campus Kitchen", "is_open": True, "estimated_prep_time_mins": 15,
               "menu": [{"item_id": "meal", "name": "Demo meal", "price": 129.0}]}
        snap = SimpleNamespace(id="1", to_dict=lambda: old, create_time=now, update_time=now)
        result = normalize(snap)
        self.assertEqual(result["menu"][0]["price_minor"], 12900)
        self.assertTrue(result["menu"][0]["available"])
        self.assertEqual(result["version"], 1)
        self.assertNotIn("price_minor", old["menu"][0])

    def test_duplicate_stored_item_ids_are_not_silently_accepted(self):
        data = fixture()
        data["menu"] *= 2
        snap = SimpleNamespace(id="demo", to_dict=lambda: data, create_time=data["created_at"], update_time=data["updated_at"])
        with self.assertRaises(InvalidStoredData):
            normalize(snap)

    def test_original_wire_field_numbers_are_preserved(self):
        self.assertEqual(pb.MenuItem.DESCRIPTOR.fields_by_name["price"].number, 3)
        self.assertEqual(pb.MenuResponse.DESCRIPTOR.fields_by_name["items"].number, 3)
        self.assertEqual(pb.StatusRequest.DESCRIPTOR.fields_by_name["is_open"].number, 2)
        self.assertEqual(pb.StatusResponse.DESCRIPTOR.fields_by_name["success"].number, 1)

    def test_local_repository_rejects_cloud_settings_before_client_creation(self):
        with patch("restaurant_service.repository.firestore.Client") as client:
            for values in ({}, {"APP_ENV": "production"}, {"APP_ENV": "local", "PROJECT_ID": "real-project", "FIRESTORE_EMULATOR_HOST": "firestore:8080"}):
                with self.assertRaises(RuntimeError):
                    local_repository(values)
            client.assert_not_called()


class RestaurantHTTPAndRPCTests(unittest.TestCase):
    def setUp(self):
        self.data = fixture()
        self.repository = Mock(spec=RestaurantRepository)
        self.repository.get.return_value = self.data
        self.repository.update.return_value = self.data
        self.repository.list.return_value = ([self.data], "next")
        self.repository.create.side_effect = lambda key, data: {**self.data, **data, "restaurant_id": key}
        self.repository.put_item.return_value = (self.data, self.data["menu"][0], True)
        self.repository.remove_item.return_value = self.data
        self.server, port = build_server(self.repository, "127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.stub = rpc.RestaurantServiceStub(self.channel)
        self.app = create_app({"TESTING": True}, stub=self.stub)
        self.client = self.app.test_client()

    def tearDown(self):
        self.channel.close()
        self.server.stop(0).wait()

    def test_health_traverses_rpc_and_list_is_paginated(self):
        self.assertTrue(self.client.get("/health").get_json()["grpc_connected"])
        response = self.client.get("/restaurants?limit=2&after=demo")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["next_page_token"], "next")
        self.repository.list.assert_called_with(2, "demo")

    def test_invalid_list_queries_are_rejected(self):
        for query in ("limit=0", "limit=51", "limit=abc", "after=a/b", "unexpected=1"):
            self.assertEqual(self.client.get("/restaurants?" + query).status_code, 400)
        self.repository.list.assert_not_called()

    def test_restaurant_creation_uses_server_generated_id(self):
        response = self.client.post("/restaurants", json={"name": "Demo", "estimated_prep_time_mins": 15})
        self.assertEqual(response.status_code, 201)
        key, payload = self.repository.create.call_args.args
        self.assertEqual(len(key), 32)
        self.assertEqual(payload["name"], "Demo")
        self.assertEqual(response.headers["Location"], "/restaurants/" + key)

    def test_get_and_patch_retain_boolean_false_and_version_header(self):
        self.assertEqual(self.client.get("/restaurants/demo").headers["ETag"], '"2"')
        response = self.client.patch("/restaurants/demo", json={"is_open": False}, headers={"If-Match": '"2"'})
        self.assertEqual(response.status_code, 200)
        self.repository.update.assert_called_with("demo", {"is_open": False}, 2)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_stale_edits_map_to_precondition_failed(self):
        self.repository.update.side_effect = StaleVersion("Reload before retrying.")
        response = self.client.patch("/restaurants/demo", json={"is_open": False}, headers={"If-Match": '"1"'})
        self.assertEqual(response.status_code, 412)

    def test_invalid_etag_does_not_invoke_write(self):
        response = self.client.patch("/restaurants/demo", json={"is_open": False}, headers={"If-Match": "*"})
        self.assertEqual(response.status_code, 400)
        self.repository.update.assert_not_called()

    def test_menu_uses_integer_prices_and_returns_availability(self):
        response = self.client.get("/restaurants/demo/menu")
        item = response.get_json()["items"][0]
        self.assertEqual(item["price_minor"], 12999)
        self.assertNotIn("price", item)
        self.assertTrue(item["available"])

    def test_menu_put_and_delete_cross_rpc_boundary(self):
        response = self.client.put("/restaurants/demo/menu/meal", json={"name": "Demo meal", "price_minor": 12999, "available": False})
        self.assertEqual(response.status_code, 201)
        self.repository.put_item.assert_called_with("demo", "meal", {"name": "Demo meal", "description": "", "price_minor": 12999, "available": False}, 0)
        response = self.client.delete("/restaurants/demo/menu/meal")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.data, b"")
        self.repository.remove_item.assert_called_once_with("demo", "meal", 0)

    def test_invalid_json_media_type_and_body_limit(self):
        self.assertEqual(self.client.post("/restaurants", json=[]).status_code, 400)
        self.assertEqual(self.client.post("/restaurants", data="text", content_type="text/plain").status_code, 415)
        self.assertEqual(self.client.post("/restaurants", data="{broken", content_type="application/json").status_code, 400)
        self.assertEqual(self.client.post("/restaurants", json={"name": "x" * 33000}).status_code, 413)
        self.repository.create.assert_not_called()

    def test_missing_restaurant_is_not_created_by_legacy_status_update(self):
        self.repository.update.side_effect = MissingRecord("Restaurant not found.")
        with self.assertRaises(grpc.RpcError) as raised:
            self.stub.UpdateStatus(pb.StatusRequest(restaurant_id="missing", is_open=True), timeout=3)
        self.assertEqual(raised.exception.code(), grpc.StatusCode.NOT_FOUND)
        self.repository.create.assert_not_called()

    def test_empty_rpc_update_mask_and_missing_price_are_rejected(self):
        for method, request in ((self.stub.UpdateRestaurant, pb.UpdateRestaurantRequest(restaurant=pb.Restaurant(restaurant_id="demo"))),
                                (self.stub.UpsertMenuItem, pb.MenuItemRequest(restaurant_id="demo", item=pb.MenuItem(item_id="meal", name="Demo", price=12.0)))):
            with self.assertRaises(grpc.RpcError) as raised:
                method(request, timeout=3)
            self.assertEqual(raised.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.repository.update.assert_not_called()
        self.repository.put_item.assert_not_called()

    def test_storage_failures_are_redacted_in_logs_and_http(self):
        dummy_private = "dummy-private-storage-detail"
        self.repository.get.side_effect = ServiceUnavailable(dummy_private)
        with self.assertLogs("restaurant_service.restaurant_service", level="ERROR") as logs:
            response = self.client.get("/restaurants/demo")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(dummy_private, response.get_data(as_text=True))
        self.assertNotIn(dummy_private, " ".join(logs.output))

    def test_public_http_mode_is_not_enabled(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            create_app(stub=self.stub)


if __name__ == "__main__":
    unittest.main()
