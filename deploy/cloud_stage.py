"""Explicit Terraform stages: plan first, then apply that exact reviewed file. No automatic deployment."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from deploy.commands import resolve
from deploy.configure import ROOT, validate
from deploy.lifecycle_state import LifecycleError, guard_running, GCSStore, context_key

def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def plan_inputs(config_path, folder, variables, backend):
    paths = [config_path, variables, ROOT/"deploy/profiles.json", *folder.glob("*.tf.json"), *folder.glob("*.tf"), *folder.glob(".terraform.lock.hcl")]
    private = folder/"secrets.auto.tfvars.json"
    if private.exists():
        paths.append(private)
    if backend:
        paths.append(backend)
    return {str(path.relative_to(ROOT)).replace("\\", "/"): file_hash(path) for path in paths}

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["state", "confluent", "cloud", "kafka", "kubeconfig", "controller", "gitops"])
    parser.add_argument("--apply", "--apply-reviewed", dest="apply", action="store_true",
                        help="Apply an existing, unchanged reviewed plan; never replan implicitly.")
    parser.add_argument("--allow-standard-capacity", action="store_true",
                        help="Explicitly acknowledge the larger standard profile; obtain project-owner agreement first.")
    args = parser.parse_args(argv)
    try:
        config_path = ROOT/"deploy/config.team.json"
        bootstrap = args.stage in {"state", "confluent"}
        config = validate(json.loads(config_path.read_text(encoding="utf-8")), allow_unprovisioned_kafka=bootstrap)
        if args.stage == "confluent" and config["kafka_cluster_id"]:
            raise LifecycleError("The Confluent bootstrap stage requires a fresh deployment with blank Kafka fields.")
        inputs = ROOT/".local-build/config/team"
        folder = ROOT/"deploy/terraform"/args.stage
        env = dict(os.environ)
        kube = ROOT/".local-build/cloud/kubeconfig"
        kube.parent.mkdir(parents=True, exist_ok=True)
        context = f"gke_{config['project_id']}_{config['zone']}_{config['prefix']}-gke"
        def run(command, **kwargs):
            return subprocess.run(resolve(command), cwd=ROOT, env=env, check=True, **kwargs)
        if args.stage == "kubeconfig":
            if args.apply:
                parser.error("kubeconfig does not use --apply.")
            env["KUBECONFIG"] = str(kube)
            run(["gcloud", "container", "clusters", "get-credentials", config["prefix"]+"-gke",
                 "--zone", config["zone"], "--project", config["project_id"]])
            print("Cloud kubeconfig stored only in the ignored workspace directory.")
            return
        if not bootstrap:
            guard_running(config)
        if args.stage == "cloud" and config.get("compute_profile", "demo") != "demo" and not args.allow_standard_capacity:
            raise LifecycleError("Larger standard capacity requires explicit owner agreement and --allow-standard-capacity.")
        command = ["terraform", "-chdir=" + str(folder)]
        backend = inputs/(args.stage+"-backend.hcl") if args.stage != "state" else None
        run(command+["init", "-input=false"]+(["-backend-config="+str(backend)] if backend else []))
        variables = inputs/(args.stage+".tfvars.json")
        if args.stage in {"controller", "gitops"}:
            value = {"kubeconfig_path": str(kube), "kube_context": context}
            if args.stage == "gitops":
                value.update(repository=config["git_repository"], revision=config["git_revision"])
            encoded = json.dumps(value, indent=2)+"\n"
            if not variables.exists() or variables.read_text(encoding="utf-8") != encoded:
                variables.write_text(encoded, encoding="utf-8")
        if args.stage in {"cloud", "kafka"} and not (folder/"secrets.auto.tfvars.json").exists():
            raise LifecycleError("Create the ignored private input with deploy.secret_inputs before planning.")
        plan = inputs/(args.stage+".tfplan")
        receipt_path = inputs/(args.stage+"-plan-review.json")
        current = plan_inputs(config_path, folder, variables, backend)
        if args.apply:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            if receipt.get("inputs") != current or receipt.get("plan_sha256") != file_hash(plan):
                raise LifecycleError("Plan or inputs changed. Create and review a new plan before applying.")
            if not bootstrap:
                guard_running(config)
            run(command+["apply", "-input=false", str(plan)])
            if args.stage == "cloud":
                result = run(command+["output", "-json"], capture_output=True, encoding="utf-8")
                # This root outputs resource IDs and version references, never secret values.
                (inputs/"cloud-outputs.json").write_text(result.stdout, encoding="utf-8")
            if args.stage == "kafka":
                store = GCSStore(config)
                _, generation = store.read("kafka_policy")
                policy = {"schema_version": 1, "context_key": context_key(config),
                    "cluster_id": config["kafka_cluster_id"], "broker": config["kafka_bootstrap"],
                    "retention_days": json.loads(variables.read_text(encoding="utf-8"))["retention_days"],
                    "applied_at": time.time(), "plan_sha256": receipt["plan_sha256"]}
                store.write("kafka_policy", policy, generation)
            print("The exact reviewed plan was applied. Verify resources before the next stage.")
        else:
            run(command+["plan", "-input=false", "-var-file="+str(variables), "-out="+str(plan)])
            receipt_path.write_text(json.dumps({"inputs": current, "plan_sha256": file_hash(plan)}, indent=2)+"\n",
                                    encoding="utf-8")
            print("Plan saved privately. Review it, then select --apply to apply exactly this file.")
    except (LifecycleError, OSError, ValueError) as error:
        if isinstance(error, LifecycleError):
            raise SystemExit(str(error))
        raise SystemExit("Deployment input or reviewed plan is unavailable. Check private configuration; no values were printed.")

if __name__ == "__main__":
    main()
