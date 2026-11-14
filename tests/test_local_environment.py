import os
from pathlib import Path
import unittest
from unittest.mock import patch
from local.environment import require_local_emulator, require_approved_recovery_root


class LocalToolGuardTests(unittest.TestCase):
    def valid(self):
        return {"APP_ENV": "local", "PROJECT_ID": "demo-food-delivery",
                "FIRESTORE_EMULATOR_HOST": "firestore:8080", "DB_PRIVATE_IP": "mysql"}

    def test_local_settings_are_accepted(self):
        with patch.dict(os.environ, self.valid(), clear=True):
            self.assertEqual(require_local_emulator(), "demo-food-delivery")

    def test_real_project_and_nonlocal_mode_are_rejected(self):
        for change in [{"PROJECT_ID": "production-example-project"}, {"APP_ENV": "production"}]:
            with self.subTest(change=change), patch.dict(os.environ, {**self.valid(), **change}, clear=True):
                with self.assertRaises(RuntimeError):
                    require_local_emulator()

    def test_cloud_firestore_host_is_rejected(self):
        with patch.dict(os.environ, {**self.valid(), "FIRESTORE_EMULATOR_HOST": "firestore.googleapis.com:443"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_local_emulator()

    def test_nonlocal_database_host_is_rejected(self):
        with patch.dict(os.environ, {**self.valid(), "DB_PRIVATE_IP": "database.example.com"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_local_emulator()


class RecoveryRootGuardTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[1]

    def test_explicit_matching_checkout_is_accepted(self):
        with patch.dict(os.environ, {"FOOD_DELIVERY_RECOVERY_ROOT": str(self.root)}, clear=True):
            self.assertEqual(require_approved_recovery_root(self.root), self.root)

    def test_missing_approval_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                require_approved_recovery_root(self.root)

    def test_another_checkout_is_rejected(self):
        other = self.root.parent / "another-checkout"
        with patch.dict(os.environ, {"FOOD_DELIVERY_RECOVERY_ROOT": str(other)}, clear=True):
            with self.assertRaises(RuntimeError):
                require_approved_recovery_root(self.root)
            with self.assertRaises(RuntimeError):
                require_approved_recovery_root(other)

    def test_relative_approval_is_rejected(self):
        with patch.dict(os.environ, {"FOOD_DELIVERY_RECOVERY_ROOT": "."}, clear=True):
            with self.assertRaises(RuntimeError):
                require_approved_recovery_root(self.root)

    def test_nonlocal_compose_project_is_rejected(self):
        with patch.dict(os.environ, {"FOOD_DELIVERY_RECOVERY_ROOT": str(self.root)}, clear=True):
            with patch.object(Path, "read_text", return_value="name: production\n"):
                with self.assertRaises(RuntimeError):
                    require_approved_recovery_root(self.root)


if __name__ == "__main__":
    unittest.main()
