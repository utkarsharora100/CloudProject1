# Cloud deployment handoff and local source recovery

Prepared: 14 November 2026.

## 1. What is ready

The verified application baseline is commit **f82c4da7557d7c43993a50e06bf843182ed161a6**. It was published to both `main` and `dev` before this guide was written. This guide documents deployment and source recovery for that baseline.

The eight assigned services are implemented: Customer, Restaurant, Order, Task Scheduler, Kitchen Workflow, Delivery Assignment, Payment and Notification. Restaurant Service includes Flink streaming analytics. The UI has separate customer, kitchen, courier and operations pages, interactive charts and an execution graph showing real task prerequisites.

| Local verification completed | Result |
| --- | --- |
| Python application regressions | 290 passed |
| Notification function contracts | 6 passed |
| JavaScript behavior checks | 3 passed |
| End-to-end ordering, payment retry, kitchen, courier and notification flow | Passed |
| Isolated Flink duplicate handling and checkpoint recovery contract | Passed |
| Prometheus targets and local observability check | 12 targets; passed |
| Browser charts, graph interaction and live updates | Checked |

These results verify the local version. **GCP deployment, managed-service integration, public HTTPS and final cloud load/HPA acceptance remain to be completed before deployment can be considered finished.** Payments and notification delivery remain simulations for this assignment. Couriers use synthetic identities and explicit manual/scripted actions.

## 2. Source code preserved for independent recovery

A source-only recovery copy has been created locally:

- Local Git tag: `local-verified-f82c4da`, pointing to the verified commit above.
- Frozen source directory: `.local-build/recovery/food-delivery-f82c4da/verified-source/`.
- Source ZIP: `.local-build/recovery/food-delivery-f82c4da/verified-source.zip`.
- Per-file SHA-256 manifest: `.local-build/recovery/food-delivery-f82c4da/source-manifest.json`.
- Inventory: **247 tracked source/configuration files**.
- ZIP SHA-256: `5410adf2fb43125ea980a2e4a4c51608efe23f539a2238964454a3f7fc85501e`.

There are **no Docker image exports, private environment files, credentials, runtime database records or Git history in this source backup**. The recovery folder is ignored by Git and will not be pushed. The local tag has not been pushed.

Keep an additional copy of this small source ZIP and manifest somewhere outside the working checkout, such as your own backup folder. A Git-ignored folder can still be deleted by disk cleanup or `git clean -fdx`. Do not edit the frozen copy while deployment changes are being tested.

To check the ZIP in PowerShell, from the project root:

```powershell
Get-FileHash -Algorithm SHA256 -LiteralPath .local-build/recovery/food-delivery-f82c4da/verified-source.zip
git rev-parse local-verified-f82c4da
```

If later changes break the code, use the frozen source directory or extract the ZIP to a separate working folder. You can then build and run that baseline yourself using the local commands in section 3. The local demo does not require GCP or Confluent access.

This preserves application code. It does not restore previous orders, Kafka offsets, cloud resources or database contents. The local Firestore emulator holds its data in memory; replacing/stopping it can lose its current contents. Do not use `docker compose down -v` or Docker volume pruning to recover code.

## 3. Running the local baseline later

No new recovery stack was started and no images were exported for this handoff. The following commands are instructions for a future source rebuild if needed.

Use PowerShell in the recovered source folder, with Docker Desktop running Linux containers and Python installed. Close other full demo stacks deliberately if memory is limited; running several copies can exhaust Docker memory.

```powershell
python -m deploy.build
docker compose -p food-assignment --env-file .env.local.example --env-file .env.assignment.example -f compose.yaml -f compose.assignment.yaml up -d --wait --wait-timeout 240
python -m deploy.verify smoke
python -m deploy.verify telemetry
```

`deploy.build` builds the application's runtime images from source when you choose to run it. It does not deploy to GCP without the release/publish flags used later in this guide.

| Local interface | URL |
| --- | --- |
| Application | http://127.0.0.1:3010 |
| Grafana | http://127.0.0.1:13000 |
| Prometheus | http://127.0.0.1:19090 |
| Flink | http://127.0.0.1:18092 |

Use the checked-in example environment files for this isolated local demo. Do not substitute cloud credentials, point it at a shared database, or mount a cloud service-account file. The local examples contain dummy local settings.

The project name above is `food-assignment`. It deliberately uses the normal local stack's ports and named volumes. Run one copy of that stack at a time; a recovered checkout is not a separate database backup.

## 4. Deployment prerequisites

Use individual accounts. Do not share account passwords or put secrets in chat.

| Requirement | What to confirm before provisioning |
| --- | --- |
| GCP project | Correct project ID and access for the deployer |
| Billing | Intended billing account is linked, verified and usable; check credit expiry and spending personally |
| Permissions | Deployer can enable APIs, manage the resources below, create service accounts and set the narrowly scoped IAM bindings defined in Terraform |
| Quotas | Sufficient regional CPU, VM, GKE, Cloud SQL and Dataproc capacity for the actual Terraform plan |
| Region/zone | Both belong together and support the selected managed resources and pinned runtime versions |
| DNS | A real hostname under your control; permission to add its A record |
| Confluent | Existing Kafka cluster endpoints/ID plus separate application and topic-administration credentials |
| GitHub | Ability to push a deployment branch and an immutable release tag |
| Deployment owner | One person coordinates Terraform state, release changes, secret versions and cleanup |

The infrastructure includes GKE, Cloud SQL, Firestore, Cloud Storage, Artifact Registry, Secret Manager, Pub/Sub, a notification function/trigger, managed Dataproc/Flink, networking, DNS when configured, and observability. Review the plan against the assignment and your budget before applying it.

A deployer needs appropriate access to these systems; giving every deployer the Owner role is unnecessary. Runtime identities and their permissions are declared in `deploy/terraform/cloud/main.tf.json`.

Previously public Confluent credentials must be replaced **and the old keys revoked by their owner**. Any previously public database password must be changed if that account/database is still active. Removing credentials from Git does not invalidate them. Do not reuse them for deployment.

## 5. Prepare the deployment checkout and tools

Work on a deployment branch based on the verified application, keeping the baseline tag intact. The commands below are for the deployment operator to run; this handoff has not created that branch or deployed anything.

```powershell
git fetch origin
git switch -c cloud-deploy f82c4da7557d7c43993a50e06bf843182ed161a6
```

If `cloud-deploy` already exists, inspect/use it instead of overwriting it. Obtain this guide separately if its documentation change has not yet been published.

Install Git, Python 3.12, Docker with Linux containers, Terraform 1.6 or newer, Helm, Google Cloud CLI, kubectl and the GKE authentication plugin. Node.js is needed for the local JavaScript verification scripts. Java/Maven compilation runs inside the build container.

Create a private Python environment without changing PowerShell execution policy:

```powershell
py -3.12 -m venv .venv
$deployPython = Join-Path (Get-Location).Path '.venv\Scripts\python.exe'
& $deployPython -m pip install -r requirements.txt
gcloud auth login
gcloud auth application-default login
```

Keep this PowerShell session, or recreate `$deployPython` when opening another one. The deployment verification helper also invokes `python` internally; put the virtual environment's Scripts directory first on this session's PATH:

```powershell
$env:PATH = (Join-Path (Get-Location).Path '.venv\Scripts') + [IO.Path]::PathSeparator + $env:PATH
```

Do not download service-account JSON keys for routine deployment. Use your authorized account and the workload identities created by the infrastructure.

## 6. Fill one non-secret configuration file

Copy the template:

```powershell
Copy-Item -LiteralPath deploy/config.example.json -Destination deploy/config.team.json
```

Edit `deploy/config.team.json` locally. It is ignored by Git. All 17 fields below must be present with their original JSON types.

| Field | Meaning |
| --- | --- |
| `project_id` | Actual GCP project ID |
| `region` | Region, for example `asia-south1` if suitable |
| `zone` | Zone within that region |
| `prefix` | Short resource prefix; keep stable after provisioning |
| `public_host` | Real DNS hostname, without `https://` |
| `git_repository` | Credential-free HTTPS repository URL |
| `git_revision` | Use a new release tag, for example `cloud-release-v1` |
| `image_tag` | Use the same unique immutable release label; never `latest`, `dev` or `local` |
| `kafka_bootstrap` | Confluent broker hostname and `:9092` |
| `kafka_cluster_id` | Actual `lkc-...` cluster ID |
| `kafka_rest_endpoint` | Credential-free HTTPS cluster REST endpoint |
| `dns_zone` | Existing GCP managed-zone name, or an empty string for externally managed DNS |
| `admin_cidr` | Deployer's actual public IPv4 with `/32`, or another deliberately restricted CIDR |
| `create_firestore` | JSON `true` for a new default database; `false` if the intended database already exists |
| `state_bucket` | Globally unique private Terraform state-bucket name |
| `operator_user` | Non-secret username for the shared assignment operator gate |

Do not use template placeholder values. Do not add API keys/passwords to this file.

Choose `cloud-release-v1` before rendering, but create its Git tag only after the generated deployment manifests are committed in section 9. This makes the release tag resolve to the commit containing its own deployment configuration. Do not point the first deployment directly at the baseline commit: that commit does not contain the generated `deploy/environments/team` files.

```powershell
$config = Get-Content -Raw -LiteralPath deploy/config.team.json | ConvertFrom-Json
gcloud config set project $config.project_id
& $deployPython -m deploy.configure
& $deployPython -m deploy.secret_inputs cloud
& $deployPython -m deploy.secret_inputs kafka
```

The private prompts collect application Kafka credentials and a new operator password for the cloud root, and topic-administration credentials for the Kafka root. Input is not echoed. Their ignored private files must not be staged. The helper refuses to overwrite an existing private-input file; inspect its existence privately before deciding whether to replace it.

Cloud SQL's new application password is generated by Terraform. Application secrets are stored in Secret Manager, and the generated deployment refers to specific versions.

## 7. Plan and apply infrastructure in order

Terraform state and saved plans can contain secrets. Keep `.local-build`, all state/plan files, `secrets.auto.tfvars.json` and all private Terraform input files out of Git and screenshots.

The stage helper's default command **only plans**. Its `--apply` flag creates a fresh plan and immediately applies that newly generated plan; it does not simply reuse the plan from a previous command. To apply exactly the plan you reviewed, define this PowerShell function once:

```powershell
function Apply-FoodPlan {
    param(
        [ValidateSet('state','cloud','kafka','controller','gitops')]
        [string]$Stage
    )
    $planPath = (Resolve-Path -LiteralPath ".local-build/config/team/$Stage.tfplan").Path
    terraform "-chdir=deploy/terraform/$Stage" apply -input=false $planPath
    if ($LASTEXITCODE -ne 0) { throw "Terraform apply failed: $Stage" }
    if ($Stage -eq 'cloud') {
        $outputs = terraform "-chdir=deploy/terraform/cloud" output -json
        if ($LASTEXITCODE -ne 0) { throw 'Could not capture cloud outputs.' }
        $outputPath = Join-Path (Get-Location).Path '.local-build/config/team/cloud-outputs.json'
        [IO.File]::WriteAllText($outputPath, ($outputs -join [Environment]::NewLine), [Text.UTF8Encoding]::new($false))
    }
}
```

Run each pair separately. **Review each plan before its corresponding apply.** Stop on errors or unexpected destruction.

```powershell
& $deployPython -m deploy.cloud_stage state
# Review the state-bucket plan, then:
Apply-FoodPlan state

& $deployPython -m deploy.cloud_stage cloud
# Review resources, IAM, networking, region and cost, then:
Apply-FoodPlan cloud

& $deployPython -m deploy.cloud_stage kafka
# Review topic changes, then:
Apply-FoodPlan kafka
```

The state root bootstraps the private state bucket. Subsequent roots use separate prefixes in that remote backend. Preserve the bootstrap root's local state and provide a private state handover to the designated deployment owner; cloning source does not recover Terraform ownership.

If expected Kafka topics already exist, import them into the correct Terraform resource addresses after checking their configuration. Do not delete/recreate shared topics just to make apply succeed. The application key needs the actual topic, consumer-group and transactional-ID permissions required by the application and Flink, while topic administration uses its separate key.

Do not run the older service-specific Terraform fragments. Use the unified roots under `deploy/terraform/`.

## 8. Render the real deployment and publish release artifacts

After cloud apply succeeds and its outputs have been captured:

```powershell
& $deployPython -m deploy.configure --cloud-outputs .local-build/config/team/cloud-outputs.json
& $deployPython -m deploy.build --release --publish
```

This publishes the application image to the configured Artifact Registry and the compiled Restaurant analytics JAR to the artifact bucket. The function source ZIP is assembled from an explicit allowlist for Terraform's function deployment. Keep the build contexts restricted; do not add the whole checkout or private configuration.

The generated deployment files are:

- `deploy/environments/team/food-values.json`
- `deploy/environments/team/applications.yaml`

They contain cloud resource identifiers and pinned Secret Manager version references, not secret values. Verify that both point to the intended project, hostname, repository revision and release artifact. Do not share the private `cloud-outputs.json` wholesale as a substitute for reviewing these files.

## 9. Publish the reviewed GitOps configuration

Stage only the intended public configuration. Review paths, contents and credentials before committing; do not use `git add .`.

```powershell
git add -- deploy/environments/team/food-values.json deploy/environments/team/applications.yaml
git diff --cached --name-only
git diff --cached --check
# Inspect the staged public configuration privately and scan it for secrets.
git commit -m "Configure first cloud deployment"
git tag cloud-release-v1
git push -u origin cloud-deploy
git push origin cloud-release-v1
```

Use your actual chosen release label if it differs from `cloud-release-v1`. Never move an existing release tag to a different commit. Publish the branch/tag before bootstrapping Argo CD, so its configured revision is available.

Secrets, local data, credentials, Terraform state/plans, image exports and local recovery files must remain excluded. This guide is the only new Markdown exception; other local planning/API documents remain ignored.

## 10. Connect the cluster and bootstrap GitOps

```powershell
& $deployPython -m deploy.cloud_stage kubeconfig
& $deployPython -m deploy.cloud_stage controller
# Review the controller plan, then:
Apply-FoodPlan controller

& $deployPython -m deploy.cloud_stage gitops
# Review the root Application plan, then:
Apply-FoodPlan gitops
```

The generated kubeconfig is private and ignored at `.local-build/cloud/kubeconfig`. Terraform bootstraps the controllers/root Application; Argo CD owns the application manifests. Avoid competing manual `kubectl apply` or Helm releases for those same workloads.

Use explicit kubeconfig/context for cloud inspection:

```powershell
$config = Get-Content -Raw -LiteralPath deploy/config.team.json | ConvertFrom-Json
$cloudKubeconfig = Join-Path (Get-Location).Path '.local-build/cloud/kubeconfig'
$cloudContext = "gke_$($config.project_id)_$($config.zone)_$($config.prefix)-gke"
kubectl --kubeconfig $cloudKubeconfig --context $cloudContext get applications -n argocd
kubectl --kubeconfig $cloudKubeconfig --context $cloudContext get pods,jobs,hpa,ingress -n food
kubectl --kubeconfig $cloudKubeconfig --context $cloudContext get externalsecrets -n food
```

Check resources in the actual plan/outputs for the reserved external IP. If `dns_zone` was empty, create the hostname's A record through its DNS owner. Wait for DNS to resolve correctly and the managed certificate to become Active before opening the real `https://` hostname.

Keep internal APIs, databases, Kafka access, ZooKeeper, metrics and administrative consoles private. Access operational consoles with authorized port forwarding. The browser must never receive cloud keys or service passwords.

## 11. Cloud acceptance checklist

Do not call deployment complete just because pods are Running.

1. Root/platform/food Argo Applications are Synced and Healthy at the intended release.
2. Schema/bootstrap jobs complete and all eight services become ready.
3. Three ZooKeeper members form the intended ensemble; scheduler leadership, worker ownership and lease recovery are visible.
4. External Secrets resolve the pinned versions; Cloud SQL and Firestore reads/writes succeed.
5. Create synthetic customer/address/menu data through the application. Place an order and verify payment, kitchen acceptance/preparation, courier assignment, pickup and delivery.
6. Test a payment decline with a safe retry and a no-courier case. Pickup must wait for both preparation and a valid assignment.
7. Notification tasks finish only after durable transport receipts. Source events, identities and customer/courier streams must remain correctly scoped.
8. Managed Flink is RUNNING with successful checkpoints. Real committed results appear in Restaurant analytics; duplicates do not double-count.
9. Verify the cream/black multi-page UI, charts, execution graph, browser refresh and live updates through HTTPS.
10. Verify actual metrics/log ingestion and dashboards without exposing customer addresses, secrets or private lease/session proofs.
11. Complete the cloud load/scaling run below and retain honest evidence.

The cloud operator password is a shared assignment access gate. This is not a production identity system with individual end-user authorization.

### Cloud load and HPA evidence

The Helm load job is disabled by default. After basic acceptance, review `loadTest` and `hpa` in `deploy/charts/food/values.yaml` and the generated deployment values. Enable the load test in the deployment values, choose a fresh `loadTest.runId`, review its traffic/duration, and commit a new GitOps release. The Job name is `food-load-<runId>`.

The release configuration must remain internally consistent: update the relevant Git revisions/root configuration for a new release and follow the managed Flink release rules below. Do not move the first release tag merely to trigger a load job.

Capture against the actual completed cloud job:

```powershell
$loadJob = Read-Host 'Actual food-load-<runId> Job name'
& $deployPython -m deploy.capture_scaling --kubeconfig $cloudKubeconfig --context $cloudContext --job $loadJob --scope gke
```

The capture checks actual GCE-backed nodes, CPU and memory HPA metrics, both selected services scaling out, successful k6 completion and nonnegative timing bounds. Keep the resulting private evidence under `.local-build/evidence/`. Local unit tests and local scale-out are not substitutes for this GKE result.

## 12. Troubleshooting and controlled changes

| Symptom | Check first |
| --- | --- |
| Permission/API/billing failure | Correct account/project, linked verified billing, API permissions and quotas |
| Argo cannot fetch/render | Release tag exists remotely; deployment files are committed at that tag; charts/dependencies are available |
| Pending pods | Actual scheduling events, node capacity, PVCs and resource requests |
| Secret resolution failure | Workload identity bindings, Secret Manager access and the specific enabled secret versions |
| SQL errors | Proxy identity, instance connection name, schema job and private networking |
| Kafka/Flink auth errors | Correct broker/cluster, application key, topics, consumer groups and transaction permissions |
| No final analytics window | Job/checkpoints, valid event times, watermark progress and read-committed results; a quiet window can wait for progress |
| Notification remains pending | Function trigger, Pub/Sub and receipt storage; do not manually force task success |
| HPA shows unknown metrics | Metrics availability, pod requests and the target workload; retain failed evidence |
| HTTPS/certificate pending | Real hostname A record, reserved IP, ingress and certificate status |

For deployment changes, prefer the config renderer and the generated deployment Helm values. Keep business logic, task dependencies, idempotency, payment/refund ownership and delivery capacity rules intact. Do not edit source to bypass a failed readiness or security check.

### Cloud rollback is separate from source recovery

The preserved local source is your independent fallback. Cloud rollback additionally requires coordination with live databases, Kafka, secrets, migrations and Flink state.

Argo CD's native rollback cannot be used while automated sync is enabled. Prefer a reviewed GitOps release/revert, or an explicitly coordinated change to sync policy. See [Argo CD automated sync documentation](https://argo-cd.readthedocs.io/en/stable/user-guide/auto_sync/).

Do not restore revoked secret versions when rolling back code. Do not assume a code rollback undoes database writes or Terraform changes.

Managed analytics uses stable release identities. An active matching release is reused; overlapping active releases and terminal reused identities are rejected. For a new stateful analytics release, take a durable Flink savepoint, coordinate stopping the prior job, use a new immutable release identity and set `ANALYTICS_SAVEPOINT` in `managedFlink.env`. Verify restored state and committed output. An old image tag alone is not a complete Flink rollback.

### Cost and cleanup

Review the actual plan and current pricing before provisioning. GKE nodes, Dataproc nodes, Cloud SQL, networking/NAT, load balancers, disks and retained storage can continue incurring costs even when the browser is closed. A credit balance alone does not establish how long this configuration can run.

Set billing alerts and assign a cleanup owner. An **alerts-only budget does not cap spending**; check the scope and availability of any separate spend-cap feature rather than assuming it covers this deployment. See [Google Cloud budgets and alerts](https://docs.cloud.google.com/billing/docs/how-to/budgets).

Do not use Terraform destroy or deletion of the Argo root Application as a casual rollback. Plan cleanup with the owner, understand cascading deletion, preserve required state/backups and handle deletion protection deliberately.

## 13. Final deployment handover

Provide release commit/tag and image identity, public application URL, resource inventory, deployment ownership, where protected remote state is held, successful cloud acceptance/load evidence, remaining limitations, and a cost/cleanup plan.

Pass access through IAM and approved secret storage, not credential-bearing documents or screenshots. Personal names/student IDs and `task_divison.txt` remain deferred until the required information is supplied.

Keep the source baseline and its local recovery tag unchanged while the cloud work proceeds.
