import os
import unittest
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError
from customer_service.customer_service import create_app, database_url
from customer_service.models import Address, db


class CustomerAPITests(unittest.TestCase):
    def setUp(self):
        self.app = create_app({"TESTING": True, "APP_ENV": "local",
                               "SQLALCHEMY_DATABASE_URI": "sqlite://",
                               "AUTO_CREATE_SCHEMA": True})
        self.context = self.app.app_context()
        self.context.push()
        self.client = self.app.test_client()
        self.customer = self.client.post("/customers", json={
            "name": "Demo Customer", "email": "customer@example.test"}).get_json()["customer"]
        self.path = f"/customers/{self.customer['id']}"

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.context.pop()

    def address(self, **changes):
        return {"label": "Demo home", "line1": "Synthetic demo block", "city": "Demo City",
                "postal_code": "000000", "country_code": "in", **changes}

    def add_address(self, **changes):
        response = self.client.post(self.path + "/addresses", json=self.address(**changes))
        self.assertEqual(response.status_code, 201)
        return response.get_json()["address"]

    def addresses(self):
        return self.client.get(self.path + "/addresses").get_json()["addresses"]

    def test_profile_read_update_and_utc_timestamps(self):
        response = self.client.patch(self.path, json={"name": " Revised Demo ", "phone": "+00 000 000 0000"})
        self.assertEqual(response.status_code, 200)
        customer = self.client.get(self.path).get_json()["customer"]
        self.assertEqual(customer["name"], "Revised Demo")
        self.assertEqual(customer["email"], "customer@example.test")
        self.assertTrue(customer["created_at"].endswith("Z"))
        self.assertTrue(customer["updated_at"].endswith("Z"))
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")

    def test_profile_creation_normalizes_email_and_returns_location(self):
        response = self.client.post("/customers", json={"name": " New Demo ", "email": " NEW@EXAMPLE.TEST "})
        self.assertEqual(response.status_code, 201)
        customer = response.get_json()["customer"]
        self.assertEqual(customer["email"], "new@example.test")
        self.assertEqual(response.headers["Location"], f"/customers/{customer['id']}")

    def test_duplicate_email_is_a_conflict_and_preserves_existing_customer(self):
        response = self.client.post("/customers", json={"name": "Duplicate", "email": "CUSTOMER@EXAMPLE.TEST"})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.client.get(self.path).get_json()["customer"]["name"], "Demo Customer")

    def test_email_update_conflict_rolls_back_all_changes(self):
        self.client.post("/customers", json={"name": "Other", "email": "other@example.test"})
        response = self.client.patch(self.path, json={"name": "Must roll back", "email": "OTHER@EXAMPLE.TEST"})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.client.get(self.path).get_json()["customer"]["name"], "Demo Customer")

    def test_invalid_profiles_do_not_create_records(self):
        for payload in ([], {}, {"name": "Missing email"}, {"name": "", "email": "valid@example.test"},
                        {"name": "Demo", "email": "not-an-email"},
                        {"name": "x" * 101, "email": "valid@example.test"},
                        {"name": "Demo", "email": "valid@example.test", "phone": True},
                        {"name": "Demo", "email": "valid@example.test", "role": "admin"}):
            with self.subTest(payload_type=type(payload).__name__):
                self.assertEqual(self.client.post("/customers", json=payload).status_code, 400)

    def test_invalid_patch_does_not_modify_profile(self):
        response = self.client.patch(self.path, json={"name": "Changed", "email": None})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get(self.path).get_json()["customer"]["name"], "Demo Customer")

    def test_optional_profile_fields_can_be_cleared(self):
        self.client.patch(self.path, json={"phone": "+00 000 000 0000"})
        self.assertIsNone(self.client.patch(self.path, json={"phone": None}).get_json()["customer"]["phone"])

    def test_first_address_is_default_and_switch_is_exclusive(self):
        first = self.add_address(is_default=False)
        second = self.add_address(label="Demo campus")
        self.assertTrue(first["is_default"])
        self.assertFalse(second["is_default"])
        response = self.client.patch(f"{self.path}/addresses/{second['id']}", json={"is_default": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([a["id"] for a in self.addresses() if a["is_default"]], [second["id"]])

    def test_default_cannot_be_cleared_without_a_replacement(self):
        first = self.add_address()
        response = self.client.patch(f"{self.path}/addresses/{first['id']}", json={"is_default": False})
        self.assertEqual(response.status_code, 409)
        self.assertTrue(self.addresses()[0]["is_default"])

    def test_default_deletion_promotes_oldest_remaining_address(self):
        first = self.add_address()
        second = self.add_address(label="Demo campus")
        response = self.client.delete(f"{self.path}/addresses/{first['id']}")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.data, b"")
        self.assertEqual([(a["id"], a["is_default"]) for a in self.addresses()], [(second["id"], True)])
        self.assertEqual(self.client.delete(f"{self.path}/addresses/{second['id']}").status_code, 204)
        self.assertEqual(self.addresses(), [])

    def test_address_update_and_country_normalization(self):
        address = self.add_address()
        response = self.client.patch(f"{self.path}/addresses/{address['id']}", json={"city": "Updated Demo", "line2": "", "region": None})
        self.assertEqual(response.status_code, 200)
        saved = self.client.get(f"{self.path}/addresses/{address['id']}").get_json()["address"]
        self.assertEqual(saved["city"], "Updated Demo")
        self.assertEqual(saved["country_code"], "IN")
        self.assertIsNone(saved["line2"])

    def test_address_cannot_be_accessed_through_another_customer_path(self):
        address = self.add_address()
        other = self.client.post("/customers", json={"name": "Other", "email": "other@example.test"}).get_json()["customer"]
        wrong_path = f"/customers/{other['id']}/addresses/{address['id']}"
        self.assertEqual(self.client.get(wrong_path).status_code, 404)
        self.assertEqual(self.client.patch(wrong_path, json={"label": "Changed"}).status_code, 404)
        self.assertEqual(self.client.delete(wrong_path).status_code, 404)
        self.assertEqual(self.addresses()[0]["label"], "Demo home")

    def test_invalid_address_fields_are_rejected(self):
        for changes in ({"country_code": "IND"}, {"country_code": "12"}, {"postal_code": None},
                        {"line1": "x" * 201}, {"is_default": "true"}, {"customer_id": 99}):
            with self.subTest(fields=tuple(changes)):
                self.assertEqual(self.client.post(self.path + "/addresses", json=self.address(**changes)).status_code, 400)
        self.assertEqual(self.addresses(), [])

    def test_saved_address_limit_is_enforced(self):
        for index in range(20):
            db.session.add(Address(customer_id=self.customer["id"],
                                   is_default=index == 0, **self.address(label=f"Demo {index}")))
        db.session.commit()
        self.assertEqual(self.client.post(self.path + "/addresses", json=self.address()).status_code, 409)
        self.assertEqual(len(self.addresses()), 20)

    def test_unknown_ids_and_removed_order_endpoint(self):
        for path in ("/customers/0", "/customers/999", "/customers/999999999999999999999", self.path + "/addresses/999"):
            self.assertEqual(self.client.get(path).status_code, 404)
        self.assertEqual(self.client.post("/orders", json={}).status_code, 404)

    def test_json_media_type_malformed_json_and_body_limit(self):
        self.assertEqual(self.client.post("/customers", data="text", content_type="text/plain").status_code, 415)
        self.assertEqual(self.client.post("/customers", data="{broken", content_type="application/json").status_code, 400)
        self.assertEqual(self.client.post("/customers", json={"name": "x" * 33000, "email": "x@example.test"}).status_code, 413)

    def test_health_queries_database_and_redacts_failures(self):
        self.assertEqual(self.client.get("/health").get_json()["service"], "customer-service")
        dummy_detail = "dummy-private-connection-value"
        with patch.object(db.session, "execute", side_effect=SQLAlchemyError(dummy_detail)):
            with self.assertLogs(self.app.logger, level="ERROR") as logs:
                response = self.client.get("/health")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(dummy_detail, response.get_data(as_text=True))
        self.assertNotIn(dummy_detail, " ".join(logs.output))
        self.assertEqual(self.client.get("/health").status_code, 200)


class CustomerConfigurationTests(unittest.TestCase):
    def test_password_is_required_and_special_characters_are_literal(self):
        with self.assertRaisesRegex(ValueError, "DB_PASSWORD"):
            database_url({})
        dummy = "local-test-only:@/#?"
        self.assertEqual(database_url({"DB_PASSWORD": dummy}).password, dummy)

    def test_invalid_ports_and_cloud_host_in_local_mode_are_rejected(self):
        for value in ("bad", "0", "65536"):
            with self.assertRaisesRegex(ValueError, "DB_PORT"):
                database_url({"DB_PASSWORD": "local-test-only", "DB_PORT": value})
        with self.assertRaisesRegex(ValueError, "local database host"):
            database_url({"DB_PASSWORD": "local-test-only", "APP_ENV": "local", "DB_PRIVATE_IP": "remote.invalid"})

    def test_public_mode_is_not_enabled_before_authentication(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "local-demo only"):
                create_app({"SQLALCHEMY_DATABASE_URI": "sqlite://", "AUTO_CREATE_SCHEMA": False})


if __name__ == "__main__":
    unittest.main()
