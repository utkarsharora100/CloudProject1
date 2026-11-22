"""Bind a freshly Terraform-provisioned Kafka cluster to a new deployment."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from deploy.commands import resolve
from deploy.configure import KAFKA_FIELDS, ROOT, validate, write
from deploy.secret_inputs import operator_password_hash, write_private_input

KEY_FIELDS = ("kafka_api_key", "kafka_api_secret")


def read_new_config(config_path):
    config = validate(json.loads(config_path.read_text(encoding="utf-8")), allow_unprovisioned_kafka=True)
    if any(config[key] for key in KAFKA_FIELDS):
        raise ValueError("Start with blank Kafka fields in a fresh deployment configuration.")
    return config


def prepare(config_path, root, cloud, region):
    config = read_new_config(config_path)
    inputs = root / ".local-build/config/team"
    write(inputs / "state.tfvars.json", {key: config[key] for key in ("project_id", "region", "state_bucket")})
    write(inputs / "confluent.tfvars.json", {
        "gcp_project_id": config["project_id"], "prefix": config["prefix"], "cloud": cloud, "region": region
    })
    (inputs / "confluent-backend.hcl").write_text(
        f'bucket = "{config["state_bucket"]}"\nprefix = "food/confluent"\n', encoding="utf-8"
    )


def bind(config_path, root):
    config = read_new_config(config_path)

    terraform = root / "deploy/terraform"
    cloud_secret = terraform / "cloud/secrets.auto.tfvars.json"
    kafka_secret = terraform / "kafka/secrets.auto.tfvars.json"
    if cloud_secret.exists() or kafka_secret.exists():
        raise ValueError("A fresh deployment must have no existing Kafka input files.")

    result = subprocess.run(
        resolve(["terraform", f"-chdir={terraform / 'confluent'}", "output", "-json", "deployment"]),
        cwd=root,
        capture_output=True,
        check=True,
        encoding="utf-8",
    )
    output = json.loads(result.stdout)
    if output["prefix"] != config["prefix"] or output["gcp_project_id"] != config["project_id"]:
        raise ValueError("The Terraform output belongs to a different deployment.")
    updated = validate({**config, **{key: output[key] for key in KAFKA_FIELDS}})
    application = {key: output["application"][key] for key in KEY_FIELDS}
    topic_admin = {key: output["topic_admin"][key] for key in KEY_FIELDS}
    if any(not isinstance(value, str) or not value for values in (application, topic_admin)
           for value in values.values()) or application["kafka_api_key"] == topic_admin["kafka_api_key"]:
        raise ValueError("Distinct application and topic administration keys are required.")
    application["operator_password_hash"] = operator_password_hash()

    created = []
    try:
        for target, values in ((cloud_secret, application), (kafka_secret, topic_admin)):
            write_private_input(target, values)
            created.append(target)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=config_path.parent, delete=False, suffix=".tfvars.json"
        ) as stream:
            temporary = Path(stream.name)
            try:
                json.dump(updated, stream, indent=2)
                stream.write("\n")
            except BaseException:
                stream.close()
                temporary.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary, config_path)
        finally:
            temporary.unlink(missing_ok=True)
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", help="Render private inputs for the state and Confluent stages.")
    parser.add_argument("--cloud", choices=("AWS", "GCP", "AZURE"), default="AWS")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--config", type=Path, default=ROOT / "deploy/config.team.json")
    args = parser.parse_args()
    try:
        if args.prepare:
            prepare(args.config, ROOT, args.cloud, args.region)
            print("Prepared Terraform state and Confluent inputs. Plan each stage before applying.")
            return
        bind(args.config, ROOT)
    except (KeyError, ValueError, OSError, RuntimeError, subprocess.CalledProcessError):
        raise SystemExit(
            "Kafka binding failed. Check the dedicated Terraform state, blank Kafka fields, "
            "and unused private input paths. Provider data was suppressed."
        ) from None
    print("Created separate ignored application and topic-admin inputs. Render the configuration next.")


if __name__ == "__main__":
    main()
