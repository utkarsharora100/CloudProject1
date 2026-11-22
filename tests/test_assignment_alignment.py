"""Regression checks for the isolated cloud assignment additions."""
from datetime import datetime, timezone
import json
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
import tempfile
import unittest
from unittest.mock import patch

from deploy import application, cloud_log_check, managed_kafka
from deploy.configure import validate


class AssignmentAlignment(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = self.root / "config.team.json"
        self.inputs = {
            "project_id": "assignment-test-123",
            "region": "asia-south1",
            "zone": "asia-south1-a",
            "prefix": "food",
            "public_host": "food.example.test",
            "git_repository": "https://github.com/team/food",
            "git_revision": "assignment-v1",
            "image_tag": "assignment-v1",
            "dns_zone": "",
            "admin_cidr": "192.0.2.1/32",
            "create_firestore": True,
            "enable_dataproc_job_logging": True,
            "state_bucket": "assignment-test-123-state",
            "operator_user": "operator",
            **dict.fromkeys(managed_kafka.KAFKA_FIELDS, ""),
        }
        self.config.write_text(json.dumps(self.inputs), encoding="utf-8")
        self.output = {
            "gcp_project_id": self.inputs["project_id"],
            "prefix": "food",
            "kafka_cluster_id": "lkc-dummy",
            "kafka_rest_endpoint": "https://kafka.example.test:443",
            "kafka_bootstrap": "kafka.example.test:9092",
            "application": {"kafka_api_key": "demo-app", "kafka_api_secret": "dummy-app-secret"},
            "topic_admin": {"kafka_api_key": "demo-admin", "kafka_api_secret": "dummy-admin-secret"},
        }

    def test_only_bootstrap_accepts_all_blank_kafka_fields(self):
        with self.assertRaises(ValueError):
            validate(self.inputs)
        incomplete = {**self.inputs, "kafka_cluster_id": "lkc-dummy"}
        with self.assertRaises(ValueError):
            validate(incomplete, allow_unprovisioned_kafka=True)
        managed_kafka.prepare(self.config, self.root, "AWS", "us-east-2")
        directory = self.root / ".local-build/config/team"
        self.assertTrue((directory / "state.tfvars.json").is_file())
        self.assertIn('prefix = "food/confluent"', (directory / "confluent-backend.hcl").read_text())

    def test_managed_kafka_binds_separate_credentials_and_refuses_rebinding(self):
        with (
            patch.object(managed_kafka, "resolve", side_effect=lambda args: args),
            patch.object(managed_kafka.subprocess, "run", return_value=CompletedProcess(
                [], 0, json.dumps(self.output)
            )) as run,
            patch.object(managed_kafka, "operator_password_hash", return_value="dummy-hash"),
        ):
            managed_kafka.bind(self.config, self.root)
            self.assertTrue(run.call_args.kwargs["capture_output"])
            with self.assertRaises(ValueError):
                managed_kafka.bind(self.config, self.root)
            self.assertEqual(run.call_count, 1)
        linked = json.loads(self.config.read_text())
        self.assertEqual(linked["kafka_cluster_id"], self.output["kafka_cluster_id"])
        self.assertFalse(set(linked) & set(managed_kafka.KEY_FIELDS))
        self.assertIs(linked["enable_dataproc_job_logging"], True)
        roots = self.root / "deploy/terraform"
        for root, key in (("cloud", "application"), ("kafka", "topic_admin")):
            secret = json.loads((roots / root / "secrets.auto.tfvars.json").read_text())
            self.assertEqual(secret["kafka_api_key"], self.output[key]["kafka_api_key"])

    def test_cloud_log_presence_requires_each_source_and_reports_errors_safely(self):
        queries = cloud_log_check.selectors(
            self.inputs["project_id"], "food", "food-analytics-" + "a" * 20, datetime.now(timezone.utc)
        )
        self.assertEqual(len(queries), 10)
        self.assertIn("dataproc.job.yarn.container", queries["Restaurant Flink"])
        self.assertIn("transport_receipt", queries["Notification delivery function"])
        responses = iter([CompletedProcess([], 0, '[{"insertId":"example"}]')] * 8 + [
            CompletedProcess([], 0, "[]"), CalledProcessError(1, [], stderr="provider-private-data")
        ])

        def respond(*args, **kwargs):
            self.assertTrue(kwargs["capture_output"])
            result = next(responses)
            if isinstance(result, Exception):
                raise result
            return result

        with (
            patch.object(cloud_log_check, "resolve", side_effect=lambda args: args),
            patch.object(cloud_log_check.subprocess, "run", side_effect=respond),
        ):
            statuses = cloud_log_check.check(self.inputs["project_id"], queries)
        self.assertEqual(statuses["Customer"], "observed")
        self.assertEqual(statuses["Notification delivery function"], "missing")
        self.assertEqual(statuses["Restaurant Flink"], "query_error")
        self.assertNotIn("provider-private-data", json.dumps(statuses))

    def test_cloud_notification_does_not_start_with_a_sql_only_transport(self):
        with patch.dict(application.os.environ, {
            "APP_ENV": "cloud", "CLOUD_DEPLOYMENT_ACK": "assignment-private-network-v1",
            "PROJECT_ID": self.inputs["project_id"], "SERVICE_NAME": "notification",
            "NOTIFICATION_TRANSPORT": "sql",
        }, clear=True):
            with self.assertRaisesRegex(RuntimeError, "serverless delivery function"):
                application.create()


if __name__ == "__main__":
    unittest.main()
