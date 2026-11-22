"""Check fresh cloud log coverage without exporting log payloads."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import re
import subprocess

from deploy.commands import resolve

APPLICATIONS = {
    "Customer": "customer",
    "Restaurant": "restaurant-api",
    "Order": "order",
    "Scheduler": "task-api",
    "Payment": "payment",
    "Kitchen": "kitchen",
    "Delivery": "delivery",
    "Notification": "notification",
}


def selectors(project, prefix, job_id, since):
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
        raise ValueError("Use a GCP project ID.")
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,14}", prefix):
        raise ValueError("Use the deployment prefix.")
    if not re.fullmatch(r"food-analytics-[a-f0-9]{20}", job_id):
        raise ValueError("Use the current Dataproc job ID from the deployed Flink run.")
    if since.tzinfo is None:
        raise ValueError("The start time requires a time zone.")

    cutoff = f'timestamp >= "{since.astimezone(timezone.utc).isoformat()}"'
    k8s = (f'{cutoff} AND resource.type="k8s_container" '
           f'AND resource.labels.cluster_name="{prefix}-gke" '
           'AND resource.labels.namespace_name="food" AND resource.labels.container_name="application" '
           'AND jsonPayload.event="http_request"')
    queries = {name: f'{k8s} AND jsonPayload.service="{service}"'
               for name, service in APPLICATIONS.items()}
    queries["Notification delivery function"] = (
        f'{cutoff} AND resource.type="cloud_run_revision" '
        f'AND resource.labels.service_name="{prefix}-notification-transport" '
        'AND jsonPayload.service="notification-function" AND jsonPayload.event="transport_receipt"'
    )
    queries["Restaurant Flink"] = (
        f'{cutoff} AND resource.type="cloud_dataproc_job" AND resource.labels.job_id="{job_id}" '
        'AND log_id("dataproc.job.yarn.container") AND "food_flink_window_emitted"'
    )
    return queries


def check(project, queries):
    results = {}
    for name, query in queries.items():
        try:
            response = subprocess.run(
                resolve(["gcloud", "logging", "read", query, "--project", project, "--limit", "1",
                         "--format=json(insertId)", "--quiet"]),
                capture_output=True, encoding="utf-8", check=True, timeout=30,
            )
            entries = json.loads(response.stdout)
            if not isinstance(entries, list):
                raise ValueError("Expected a log-entry list.")
            results[name] = "observed" if entries else "missing"
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
            results[name] = "query_error"
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--flink-job", required=True)
    parser.add_argument("--since", type=datetime.fromisoformat,
                        default=datetime.now(timezone.utc) - timedelta(minutes=30))
    args = parser.parse_args()
    try:
        queries = selectors(args.project, args.prefix, args.flink_job, args.since)
    except ValueError as error:
        parser.error(str(error))
    results = check(args.project, queries)
    passed = all(status == "observed" for status in results.values())
    print(json.dumps({"result": "PASS" if passed else "INCOMPLETE", "coverage": results}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
