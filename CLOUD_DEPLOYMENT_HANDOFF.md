# Cloud deployment handoff and local source recovery

Prepared: 14 November 2026. Updated: 18 November 2026.

## 1. What is ready

The preserved local application baseline is commit **f82c4da7557d7c43993a50e06bf843182ed161a6**. Earlier capacity and lifecycle helpers were published to `main` in commit **2f05f2f76f2bacf5ede91378503d2345f7cc6dcb**. The verified cloud deployment adds managed-service fixes on branch `deploy`, with application release tag **cloud-release-20261118-v15** at commit **fbc65d21ac10ec27af1c981ca64584722cb75de9**. Use that cloud source for deployment or presentation recovery; use the older frozen baseline for independent local recovery. The current deployment uses project `cloudproject-515012` in `us-central1` / `us-central1-a`. The old source backups remain intact.

The eight assigned services are implemented: Customer, Restaurant, Order, Task Scheduler, Kitchen Workflow, Delivery Assignment, Payment and Notification. Restaurant Service includes Flink streaming analytics. The UI has separate customer, kitchen, courier and operations pages, interactive charts and an execution graph showing real task prerequisites.

| Local verification completed | Result |
| --- | --- |
| Python application regressions | 366 passed in the final source |
| Notification function contracts | 6 passed |
| JavaScript behavior checks | 3 passed |
| End-to-end ordering, payment retry, kitchen, courier and notification flow | Passed |
| Isolated Flink duplicate handling and checkpoint recovery contract | Passed |
| Prometheus targets and local observability check | 12 targets; passed |
| Browser charts, graph interaction and live updates | Checked |

The final source has **366 passing Python tests**, including the actual k6 export regression, lifecycle and bounded database-pool contracts. The preserved baseline retains its original Compose/Flink evidence. The live cloud checks below are separate measurements. Payments and notification delivery remain simulations for this assignment; the cloud function confirms in-app transport receipts without sending real email or SMS. Couriers use synthetic identities and explicit manual/scripted actions.

### Current cloud instance and evidence (18 November 2026)

- Public HTTPS address: [Supper](https://food-136-69-72-1.sslip.io).
- **Current state: running for the requested customer-access review and tutorial.** Public browsing is enabled. Pause after the demonstration to save credits; local Docker is not needed to use the cloud website.
- Final source-and-documentation tag: `cloud-handoff-20261118-customers`; the active application/GitOps release is `cloud-release-20261118-v15`.
- Source branch: `deploy`; current GitOps revision: `cloud-release-20261118-v15`.
- Application image release: `cloud-release-20261118-v15`; analytics artifact release: `cloud-release-20261118-v3`. Web changes preserve the running analytics identity.
- Capacity: three `e2-standard-2` GKE workers, one `e2-standard-2` analytics VM and `db-g1-small` SQL. No larger compute profile was selected.
- Credentials are supplied by pinned Secret Manager versions. Private configuration, operator unlock data, state, plans and raw evidence stay outside Git.

| Actual cloud verification | Observed result |
| --- | --- |
| HTTPS access gate, customer/menu/cart and checkout replay | Passed |
| Payment decline/retry, kitchen stages, no-courier wait and pickup AND dependency | Passed |
| Courier pickup/delivery and customer/courier receipt-confirmed notifications | Passed |
| Desktop/mobile charts, graph inspector, dependency highlighting and zoom | Passed; black `rgb(0,0,0)` on cream `rgb(247,242,231)`; no page overflow or JavaScript errors |
| Managed Flink | Real checkpoints and committed results verified. A driver memory-protection failure was recovered from retained checkpoint 239; the corrected run reports restored state and new completed checkpoints. |
| Worker/scheduler replacement | Real task changed `ABANDONED` to `SUCCEEDED` under a new owner; a new scheduler epoch was elected |
| Observability | All 15 observed application scrape targets healthy; structured request logs found in Google Cloud Logging |
| 60-VU sustained probe | 26,437 requests; zero HTTP/business errors; both APIs scaled from one to four; **failed** the two-second latency target with p95 6,658.79 ms |
| 12-VU sustained probe | **Passed**: 31,485 requests, zero HTTP/business errors, p95 983.80 ms; Customer 2 to 4 replicas, Order 3 to 4; 62 saved samples |
| Normal savepoint pause/resume | Passed: canonical savepoint restored, both Flink tasks running and 102 new checkpoints observed; old order, workspace and committed windows retained. Resume took 26 minutes 13 seconds. |
| Current v15 operator acceptance | Passed: a fresh delivered order, 10 customer and 3 courier notifications, desktop/mobile browser checks, exact courier handoffs and receipt-confirmed graph tasks. Total run: 370.8 seconds. |
| Current v15 public accounts and restart | Passed: anonymous browsing, real guest delivery with 8 customer / 3 courier receipts, staff form sign-in/sign-out, guest history and address retained at signup, returning sign-in, two-account isolation, and persistent authenticated sessions/order data after a real web-pod replacement. |
| Previous v11 functional acceptance | Passed on `v11`: fresh delivered order, 10 customer and 3 courier notifications, one durable attempt each; desktop/mobile browser checks passed. |
| Final fresh analytics | Passed: two committed windows, distinct-order counting and interactive stream selection; restored Flink run had 188 new completed checkpoints before final pause. |
| Recorded v11 pause verification | Passed: zero actual/target GKE workers, SQL `NEVER` with no pending operations, analytics `STOPPED`, all 24 application Deployments and ZooKeeper at zero; GitOps and analytics submission suspended, savepoint metadata verified and operator lease released. Final pause took 9 minutes 39 seconds. |

The final readiness fix was measured on the live `v11` container: the Notification reconciliation heartbeat probe completed in 0.106 to 0.222 seconds against its eight-second timeout, with the deployed source hash matching the verified source. Heartbeat freshness and connectivity checks remain enforced; SQL-backed outbox and scheduler-owner checks remain unchanged.

The small analytics VM also hosts managed Hadoop daemons. The deployment therefore sets the Flink JobManager process to `768m` and TaskManager process to `1024m`; the recovered run used 1,792 MiB of YARN allocation. Keep the existing global Java options from the managed image: adding a nested `env.java.opts.client` conflicts with its legacy scalar `env.java.opts`. The launcher uses the separate `env.java.default-opts.client` namespace. See [Flink memory configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/) and [Google memory-protection troubleshooting](https://docs.cloud.google.com/managed-spark/docs/support/troubleshoot-oom-errors). A completed external checkpoint is a crash-recovery input. The separate normal canonical-savepoint pause/resume rehearsal also passed, including verification of restored Flink state and retained application data.

Private evidence is under `.local-build/cloud/` and `.local-build/evidence/`. The first probe's full metric samples were lost when its old summary exporter rejected a literal newline escape; its measured k6 summary and replica-transition log are retained. The fixed exporter saves samples throughout subsequent runs and still marks failed Jobs or invalid timing as failures.

### Public customer access and restricted operator tools

The cloud configuration supports `customer_access: "enabled"` (the renderer default) or `"disabled"` for the earlier protected demonstration. The Helm flag injects `WEB_CUSTOMER_ACCESS` only into the web workload. The existing local Compose demo defaults to disabled; optional local public-mode settings use `WEB_CUSTOMER_ACCESS`, `LOCAL_WEB_OPERATOR_USER` and a dedicated `LOCAL_WEB_OPERATOR_PASSWORD_HASH`. Keep actual cloud credentials out of local containers.

Guests can open the homepage and menus without the browser login prompt. Adding a meal starts a private guest table; checkout provides address entry without requiring an account. `/signup` and `/signin` provide optional customer accounts, and sign-out invalidates the server session. Sign-in preserves a guest cart and grants access to that account's saved details, addresses, order history and customer inbox. Return to checkout after signing in to use the retained cart.

Sign-up and sign-in forms use explicit POST actions and keep submission disabled until the JavaScript handler is attached. That handler binds before account-data requests, so a slow inbox/context request cannot fall back to a native GET submission. A browser regression delays both the module and account-data loading and checks that credential fields never appear in URLs.

Account sessions rotate their opaque cookie and CSRF token after authentication. Passwords are stored only as salted scrypt hashes. Login attempts and password-hashing concurrency are bounded. Account identity, session ownership, per-restaurant table/fleet bindings and rate limits are stored in additive tables in the existing private web SQLite volume. Customer profiles, addresses and domain order records remain in Cloud SQL. The web workload stays at one replica; moving it to multiple writable replicas requires a deliberate shared-store design. A planned web-pod replacement can briefly return gateway errors while the public load balancer updates its backend. This small configuration does not promise zero downtime. Restart acceptance waits for the new pod and sustained authenticated HTTPS responses before checking account history and isolation.

Each guest/account sees only owned customer records. Server-side checks protect direct API requests as well as navigation. Kitchen, Courier, Operations, courier inboxes and demonstration-workspace creation require the existing trusted operator account. There are no self-registered staff roles; individual restaurant/courier staff-account provisioning is not part of this release. The operator can select a customer table to prepare and deliver that customer's order through the same domain APIs.

Tables keep the restaurant and its courier fleet stable when a customer orders from another restaurant or uses two sessions. An unconfirmed checkout blocks sign-in/session changes until reconciled; after an account session expires, committed orders can be recovered from the owning customer's authoritative Order records. Earlier operator fixtures remain private and their data is preserved.

Use a unique demo passphrase of at least 15 characters. Email ownership verification, automated password recovery and real payment/email/SMS delivery are not provided. The website login is separate from GCP and Confluent access, and no backend credentials are shipped to the browser. See [OWASP authentication guidance](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html) and [Flask web-security guidance](https://flask.palletsprojects.com/en/stable/web-security/) for the controls used here.

Local verification: **366 Python tests passed**, including customer/guest isolation, session rotation/revocation/expiry, own-recipient inbox commands, staff-route enforcement, account persistence, lost-response recovery and multiple-restaurant carts. Three existing JavaScript behavior checks and desktop/mobile account-flow browser checks passed. Actual v15 cloud acceptance passed separately: the original operator flow, public guest checkout through delivered food, 8 customer and 3 courier receipts for that guest order, guest-to-account history/address retention, two-account isolation, returning sign-in and an actual web-pod replacement. Both sessions were authenticated before replacement; afterward the same customer cookie remained valid, the delivered order was retained and the second account was still denied access. Five consecutive authenticated HTTPS responses were required after the new pod became ready. The run reported no JavaScript errors. The separate focused restart diagnostic also passed.

### Using the existing deployment from this computer

Use the existing checkout and protected local settings. Do not repeat first-deployment provisioning just to start the presentation. Local Docker Desktop is not required to run these cloud lifecycle commands.

```powershell
Set-Location 'C:\Users\Ankit\Projects\CloudProject1-dev'
$deployPython = '.\.local-build\solo-venv\Scripts\python.exe'
$env:CLOUDSDK_COMPUTE_SSH_KEY_FILE = 'C:\Users\Ankit\Projects\CloudProject1-dev\.local-build\cloud\ssh\operator'
& {
    $lifecycleText = & $deployPython -m deploy.lifecycle status
    if ($LASTEXITCODE -ne 0) { throw 'Could not read cloud status. Stop and inspect authentication.' }
    $lifecycleState = $lifecycleText | ConvertFrom-Json
    if ($lifecycleState.phase -ne 'running') {
        & $deployPython -m deploy.lifecycle resume --execute
        if ($LASTEXITCODE -ne 0) { throw 'Resume did not pass. Stop before opening the demo.' }
    }
    & $deployPython -m deploy.lifecycle check
    if ($LASTEXITCODE -ne 0) { throw 'Readiness did not pass. Stop and inspect the reported state.' }
    & $deployPython .local-build/cloud/open_demo.py
}
```

The local `open_demo.py` helper opens a separate public customer Chrome window. Add `--staff` to open the operator view using this Windows user's protected unlock data; the helper signs in without printing or copying the password. The ignored helper and unlock data stay on this computer, outside the public source archive. Another computer can browse the public URL, create its own customer account, or use separately authorized website operator access. If the CLI reports a reauthentication requirement, run `gcloud auth login` with the intended cloud account before retrying the interrupted lifecycle command. Website visitors do not need that CLI login.

The measured rehearsal needed about 26 minutes to resume all dependencies. Start 30 to 45 minutes before a presentation and allow extra time for provider delays; this is a planning margin, not a startup guarantee. Keep the release and local configuration unchanged while paused. After completing a fresh demonstration order, close the separate browser and pause again:

```powershell
& $deployPython -m deploy.lifecycle pause --execute
& $deployPython -m deploy.lifecycle status
```

The app is unavailable while paused. GCP credits were reported to expire on 29 November 2026, with the presentation on 24 November. Check those dates and the separate Confluent trial/billing status personally; GCP credit does not pay Confluent charges.

### Implementation scope and deployment responsibilities

The agreed application features, deployment helpers and managed-service fixes are implemented, published and exercised on the cloud instance above. No additional service, UI page or business workflow is planned for this scope. For another account or a fresh environment, the operator must still configure permissions/secrets, review infrastructure plans, deploy and collect new evidence; results from this instance do not automatically verify another deployment.

| Area | Already supplied | Deployment responsibility |
| --- | --- | --- |
| Application | Eight domain services, simulated payments/notification delivery and synthetic courier actions | Run the complete order flow against the deployed services |
| Interface | Separate customer, kitchen, courier and operations pages, charts and task-dependency graph | Verify HTTPS access, interactions and live updates |
| Coordination | ZooKeeper ensemble, scheduling, ownership/leases, dependencies and recovery logic | Verify the actual GKE ensemble and abandoned-work recovery |
| Streaming analytics | Restaurant-owned Flink pipeline and savepoint/restart helpers | Verify managed Dataproc execution, committed results and restored state |
| Infrastructure | Unified Terraform roots, Helm charts, GitOps manifests and private secret-input prompts | Supply the reviewed settings, arrange permissions and apply the plans |
| Scaling and operations | Load workload, two HPAs, telemetry and pause/resume helpers | Run the required sustained load and a real pause/resume rehearsal |

Use this execution order: **section 5 checkout/tools -> section 6 settings/secrets -> section 7 infrastructure -> section 8 release build -> section 9 Git publication -> section 10 GitOps/DNS -> section 11 cloud acceptance -> section 14 pause/resume rehearsal**. Run stages individually and stop on the first failure. Plan/apply pairs require review between the two commands; do not paste the entire guide into one unattended shell run.

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

### Final cloud source recovery

A separate frozen archive of the final cloud source and this handoff, tagged `cloud-handoff-20261118-customers`, is created after publication under `.local-build/recovery/cloud-verified-20261118-<commit>/verified-source.zip`. The local record `.local-build/cloud/final-source-recovery.json` gives its exact path, source commit/tag, byte count and SHA-256. A neighboring `.zip.sha256` file verifies the ZIP, and the ZIP includes a per-file `SOURCE_MANIFEST.json`.

The archive excludes credentials, private configuration, Terraform state/plans, Git history, runtime data and Docker images. It preserves source, including deployment code and non-secret manifests. It supports rebuilding an independent local demo; recreating cloud infrastructure still requires authorized account configuration, state access and fresh/private secrets. Keep the cloud source tag and recovery archive alongside the older local baseline.

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

### Information and access to arrange first

Collect non-secret identifiers and grant access through individual accounts before starting. This guide must never become a place to record actual passwords or API secrets.

| Item | What the deployment operator needs | Where it is used |
| --- | --- | --- |
| GCP | Project ID, region/zone, intended billing account, usable credits and required quotas | Non-secret configuration and reviewed infrastructure plan |
| GCP deployer | Permission to enable APIs, create the declared resources and manage the required service accounts/IAM; IAP/OS Login access for the lifecycle workflow | Individual CLI login and Application Default Credentials; see section 14 for lifecycle access |
| Network administration | The deployer's restricted public IPv4 CIDR; update it deliberately if their network changes | `admin_cidr` restricts cluster administration |
| Existing Firestore | Whether the intended default database already exists | Set `create_firestore` deliberately; do not attempt to recreate a shared database |
| Existing Confluent cluster | Cluster ID, Kafka bootstrap address and Kafka REST endpoint | Non-secret configuration |
| Confluent application identity | A fresh cluster-specific application API key/secret and the permissions used by the producers, consumers and Flink transactions | Hidden `deploy.secret_inputs cloud` prompts |
| Confluent topic administration | A separate authorized cluster-specific key/secret for topic administration | Hidden `deploy.secret_inputs kafka` prompts |
| Git repository | Read access for GitOps and write access for the deployment branch/release tag | Credential-free repository URL and published immutable release |
| DNS | A real hostname and ability to set its A record; an existing managed-zone name if using Cloud DNS | `public_host` and optional `dns_zone` |
| Release and state | A new unused release/image label, stable resource prefix and globally unique private state-bucket name | `git_revision`, `image_tag`, `prefix` and `state_bucket` |
| Application access | A non-secret operator username and a newly chosen passphrase | Username in configuration; passphrase entered only through the hidden prompt |

GCP CLI access and Application Default Credentials are configured with the two login commands in section 5. Use authorized individual access; routine deployment does not require sharing personal login credentials or downloading a service-account JSON key. See [Google local ADC setup](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment).

The Kafka Terraform root configures four topics in an **existing** Confluent cluster: `orders`, `kitchen_status`, `delivery_assignments` and `restaurant_analytics_results`. New topics default to three partitions. Before adopting existing topics, record their actual partition counts and writable settings in the ignored Kafka input using `topic_partitions` and `topic_configs`, then import each existing topic into the selected remote state. Preserve its partition count; Kafka cannot reduce it. The root keeps topic deletion protection enabled and manages retention separately through `retention_days`. Review the resulting plan to confirm that existing topics are updated in place and no topics are replaced or deleted. It does not create the Confluent account/cluster, issue API keys or grant the application's Kafka ACLs. The Confluent owner must arrange that access beforehand, including consumer-group and transactional-ID permissions. Creating an API key alone is not a substitute for granting its principal the required permissions. See [Confluent API-key documentation](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html).

Argo CD must be able to read the selected repository and release tag. If using a private repository, arrange its controller-side read credentials separately through protected configuration; never embed a token in the repository URL.

The supplied analytics job runs on GCP Dataproc. No Confluent-managed Flink service or managed connector is required by this deployment. The infrastructure creates a new Cloud SQL application password; existing local database credentials are not deployment inputs. Check both providers' actual billing/trial expiry before a presentation or long pause.

## 5. Prepare the deployment checkout and tools

For a fresh deployment, use a deployment branch based on the current published source and keep the recovery baseline tag intact. The existing instance uses the recorded deployment branch and immutable releases above. To operate that instance, keep its existing configuration and use the lifecycle commands; do not rerun initial provisioning merely to resume it.

On a new machine, first clone into a new folder. Skip this block if already in the intended checkout:

```powershell
git clone https://github.com/utkarsharora100/CloudProject1.git CloudProject1-cloud
Set-Location CloudProject1-cloud
```

From that checkout, check for local changes before switching branches. Preserve any existing work:

```powershell
git status --short
git fetch origin --tags
git switch deploy
```

On a fresh clone, `git switch deploy` creates a local branch tracking `origin/deploy`. If it already exists, inspect/use it instead of overwriting it. Confirm its source includes `deploy/lifecycle.py` and `deploy/profiles.json` before proceeding. The older recovery commit does not include the later lifecycle helpers.

Install Git, Python 3.12, Docker with Linux containers, Terraform 1.6 or newer, Helm, Google Cloud CLI, kubectl and the GKE authentication plugin. Start Docker Desktop before building release artifacts. Node.js is needed for the local JavaScript verification scripts. Java/Maven compilation runs inside the build container.

Check tool availability before authenticating or provisioning:

```powershell
$requiredTools = 'git', 'py', 'docker', 'terraform', 'helm', 'gcloud', 'kubectl', 'gke-gcloud-auth-plugin'
foreach ($tool in $requiredTools) {
    Get-Command $tool -ErrorAction Stop | Select-Object Name, CommandType
}
py -3.12 --version
```

`$ErrorActionPreference = 'Stop'` alone does not make Windows PowerShell stop on every native CLI error. Check each command's result and stop if it fails; in an automation script, test `$LASTEXITCODE` after each native command.

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

Edit `deploy/config.team.json` locally. It is ignored by Git. Use the fields below with their original JSON types. Older configuration files may omit `compute_profile`; it defaults to `demo`. The optional `analytics_release` is useful when only the web/application image changes.

| Field | Meaning |
| --- | --- |
| `project_id` | Actual GCP project ID |
| `region` | Region, for example `asia-south1` if suitable |
| `zone` | Zone within that region |
| `prefix` | Short resource prefix; keep stable after provisioning |
| `public_host` | Real DNS hostname, without `https://` |
| `git_repository` | Credential-free HTTPS repository URL |
| `git_revision` | Use a new release tag, for example `cloud-release-v1` |
| `image_tag` | Unique immutable application image label; never `latest`, `dev` or `local` |
| `analytics_release` (optional) | Immutable analytics JAR/release label; defaults to `image_tag`. Keep the existing value for a web-only update so the active Flink run retains its identity. |
| `kafka_bootstrap` | Confluent broker hostname and `:9092` |
| `kafka_cluster_id` | Actual `lkc-...` cluster ID |
| `kafka_rest_endpoint` | Credential-free HTTPS cluster REST endpoint |
| `dns_zone` | Existing GCP managed-zone name, or an empty string for externally managed DNS |
| `admin_cidr` | Deployer's actual public IPv4 with `/32`, or another deliberately restricted CIDR |
| `create_firestore` | JSON `true` for a new default database; `false` if the intended database already exists |
| `state_bucket` | Globally unique private Terraform state-bucket name |
| `operator_user` | Non-secret username for the shared assignment operator gate |
| `compute_profile` | Use `demo` for the small presentation deployment; `standard` requires explicit owner agreement |

Do not use template placeholder values. Do not add API keys/passwords to this file.

An existing running analytics job must keep its configured `analytics_release` during a web-only update. Changing that value requires a separate savepoint migration; never start a second job over the same consumer group/checkpoint state. Keep all configuration unchanged during a pause/resume cycle.

For the first deployment, replace the template's `git_revision` and `image_tag` with the same new, unused release label, such as `cloud-release-v1`. The template's `dev` revision is not the immutable release used by this runbook. Choose the label before rendering, but create its Git tag only after the generated deployment manifests are committed in section 9. This makes the release tag resolve to the commit containing its own deployment configuration. Do not point the first deployment directly at the baseline commit: that commit does not contain the generated `deploy/environments/team` files.

```powershell
$config = Get-Content -Raw -LiteralPath deploy/config.team.json | ConvertFrom-Json
gcloud config set project $config.project_id
gcloud auth application-default set-quota-project $config.project_id
gcloud services enable storage.googleapis.com --project $config.project_id
& $deployPython -m deploy.configure
& $deployPython -m deploy.secret_inputs cloud
& $deployPython -m deploy.secret_inputs kafka
```

The private prompts collect application Kafka credentials and a new operator password for the cloud root, and topic-administration credentials for the Kafka root. Input is not echoed. Their ignored private files must not be staged. The helper refuses to overwrite an existing private-input file; inspect its existence privately before deciding whether to replace it.

Cloud SQL's new application password is generated by Terraform. Application secrets are stored in Secret Manager, and the generated deployment refers to specific versions.

## 7. Plan and apply infrastructure in order

Terraform state and saved plans can contain secrets. Keep `.local-build`, all state/plan files, `secrets.auto.tfvars.json` and all private Terraform input files out of Git and screenshots.

The stage helper's default command **only plans**. After review, `--apply-reviewed` (also available as `--apply`) applies that exact saved file. It does not create another plan. A private SHA-256 receipt detects a changed plan, configuration, capacity policy or private input and requires a fresh review.

Use the helper for every stage so its lifecycle guard can reject changes while the application is paused. Do not bypass it with direct Terraform apply commands.

Run each pair separately. **Review each plan before its corresponding apply.** Stop on errors or unexpected destruction.

```powershell
& $deployPython -m deploy.cloud_stage state
# Review the state-bucket plan, then:
& $deployPython -m deploy.cloud_stage state --apply-reviewed

& $deployPython -m deploy.cloud_stage cloud
# Review resources, IAM, networking, region and cost, then:
& $deployPython -m deploy.cloud_stage cloud --apply-reviewed

& $deployPython -m deploy.cloud_stage kafka
# Review topic changes, then:
& $deployPython -m deploy.cloud_stage kafka --apply-reviewed
```

For the default `demo` capacity, no capacity acknowledgment flag is needed. A reviewed `standard` cloud plan/apply additionally requires `--allow-standard-capacity`; do not select it without owner agreement.

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
git push -u origin deploy
git push origin cloud-release-v1
```

Use your actual chosen release label if it differs from `cloud-release-v1`. Never move an existing release tag to a different commit. Publish the branch/tag before bootstrapping Argo CD, so its configured revision is available.

Secrets, local data, credentials, Terraform state/plans, image exports and local recovery files must remain excluded. This guide is the only new Markdown exception; other local planning/API documents remain ignored.

## 10. Connect the cluster and bootstrap GitOps

```powershell
& $deployPython -m deploy.cloud_stage kubeconfig
& $deployPython -m deploy.cloud_stage controller
# Review the controller plan, then:
& $deployPython -m deploy.cloud_stage controller --apply-reviewed

& $deployPython -m deploy.cloud_stage gitops
# Review the root Application plan, then:
& $deployPython -m deploy.cloud_stage gitops --apply-reviewed
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

### Handling a first-deployment failure

1. Stop at the failing stage. Record the stage, timestamp, release commit/tag, resource type and a sanitized error summary. Keep raw logs, state, plans, kubeconfig and credential-bearing responses private.
2. Check account/project, billing/quota, API enablement, IAM, endpoints, secret versions and DNS before changing application code. A Kafka permission failure requires correcting access for the intended principal; it does not justify using a shared personal login.
3. For a settings correction, edit the ignored non-secret configuration and rerun the renderer as described in sections 6 and 8. Generate and review a fresh Terraform plan when its inputs change. Respect any existing resources or state after a partial apply; do not destroy them merely to retry.
4. If managed-runtime compatibility or an application defect requires a code change, make a focused fix on the deployment branch and run the relevant checks. Build and publish the affected release artifacts with a new immutable release label. Keep Git, image and analytics release identities consistent; use the stateful release procedure for a running Flink job.
5. Check the affected service/integration again, then continue from the correct stage. Keep failed evidence and mark unresolved checks as failed or not run. Do not disable required services, ZooKeeper members, HPAs, probes or credential safeguards to obtain a passing screen.

Keep the default `demo` profile while diagnosing. If measured capacity is insufficient, report scheduling events or measured resource pressure and review a deliberate sizing change; do not silently switch to the larger profile. Never change release identity or infrastructure while a lifecycle pause/resume operation is unfinished.

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

Return a sanitized deployment record with the items below. Use explicit **passed**, **failed** or **not run** status and actual test timestamps; unexecuted cloud checks must remain not run.

| Handover item | Evidence to provide |
| --- | --- |
| Release | Git commit/tag, application image identity and analytics artifact identity |
| Environment | Project ID, region/zone, deployed capacity profile and resource inventory |
| Public access | Working HTTPS URL, DNS and certificate readiness |
| Application | Readiness of all eight services and a synthetic order's payment, kitchen, assignment, pickup, delivery and notification results |
| Failure/recovery behavior | Payment retry, unavailable-courier behavior and scheduler ownership/lease recovery results |
| Managed integrations | Cloud SQL/Firestore persistence, receipt-confirmed function transport and actual Flink checkpoints/results |
| Scaling | Actual sustained-load result and scale-out evidence for both required HPAs |
| Pause/resume | A completed cloud rehearsal, restored analytics state and healthy services afterward |
| Operations | Metrics/log visibility, actual observed usage/cost, credit-expiry dates and a pause/cleanup owner |
| Ownership and gaps | Protected state location and access owner, bootstrap-state handover, unresolved issues and checks not run |

Keep full state, plans, private inputs, raw evidence/logs and recovery journals in protected storage. Share only sanitized evidence; omit customer contact/address data, private lease/session proofs and credentials. A fresh source clone does not transfer Terraform state ownership or the local recovery ZIPs.

Pass access through IAM and approved secret storage, not credential-bearing documents or screenshots. Personal names/student IDs and `task_divison.txt` remain deferred until the required information is supplied.

Keep the source baseline and its local recovery tag unchanged while the cloud work proceeds.

## 14. Low-cost demonstration profile and pause/resume

Published to `main` on 15 November 2026. The default `compute_profile` is `demo`. Build the release application image from this updated source before using the lifecycle workflow.

| Setting | Demo default |
| --- | --- |
| GKE worker pool | Three `e2-standard-2` nodes; normal and maximum node count both three |
| GKE boot disks | 30 GB standard persistent disks per node |
| Managed analytics | One `e2-standard-2` master/worker VM; zero separate worker VMs |
| Managed analytics disk | 30 GB standard persistent disk |
| Cloud SQL | `db-g1-small`, 10 GB storage, maximum 50 connections |
| Kafka event retention | 21 days by default; applied-policy receipt required before pausing |
| Application MySQL pools | Two connections per API process and one per SQL background process, without overflow; Notification uses four plus one overflow slot for two fenced senders, independent receipts and inbox reads |
| Application resource requests | Normally 50m CPU / 128 MiB; small background workers request 25m CPU |
| Required coordination | Three ZooKeeper members and two scheduler/worker replicas are preserved |
| Autoscaling evidence | Customer and Order retain independent CPU + memory HPAs; demo maximum is four replicas each |

The single-node managed cluster is suitable for a small demonstration, but it is not a multi-machine analytics availability design. See [Google's single-node cluster documentation](https://docs.cloud.google.com/managed-spark/docs/concepts/configuring-clusters/single-node-clusters).

The demo capacity has passed local configuration, contract and regression checks and the live functional checks above. The 60-VU GKE probe failed latency despite zero request/business errors; the smaller 12-VU probe passed with p95 983.80 ms and both APIs scaling out. This is the measured presentation envelope. The canonical-savepoint pause/resume rehearsal passed separately. The load measurements used application image `cloud-release-20261118-v6`; later changes affect deployment controls and readiness probes, while the measured Customer and Order API behavior is unchanged. Do not treat successful scaling alone as a latency pass. If the required cloud test does not fit, review the cause and obtain the project owner's agreement before selecting larger capacity. The optional `standard` profile requires `--allow-standard-capacity` on the cloud stage; it is never selected automatically.

For an existing deployment, changing the analytics topology or machine type can replace its Dataproc cluster. This profile is intended for a reviewed first deployment. Do not apply a replacement plan over existing data without a separate savepoint/migration review.

### Operator prerequisites for lifecycle commands

Install the Python dependencies and CLI tools from section 5. Use your authorized personal account and ADC; do not create service-account key files.

The operator needs permission to inspect/control the selected GKE node pool, inspect the Compute Engine instance group, start/stop the selected SQL and Dataproc resources, and read/write the private lifecycle records in the checkpoint bucket. Its Kubernetes access must permit reading and temporarily scaling the owned workloads and patching the three owned Argo Applications.

Flink savepoints run through IAP SSH to the private master. The infrastructure adds only TCP 22 from the IAP range `35.235.240.0/20` to the Dataproc-tagged VMs, enables OS Login and enables the related APIs. The owner must grant the operator appropriate IAP Tunnel User, OS Login and Service Account User access to the analytics VM identity. Permission to create infrastructure does not necessarily include these operational permissions. See [IAP access requirements](https://docs.cloud.google.com/iap/docs/using-tcp-forwarding).

Use the isolated kubeconfig generated by `deploy.cloud_stage kubeconfig`. Lifecycle commands always specify its explicit context and check the actual cluster endpoint before a new pause.

### Safe offline previews

These commands require no GCP credentials, make no cloud requests and do not start Docker:

```powershell
& $deployPython -m deploy.lifecycle pause --dry-run --example
& $deployPython -m deploy.lifecycle resume --dry-run --example
```

For your actual configuration, omit `--example`. A pause/resume invocation without `--execute` also produces only an offline plan; it does not inspect or mutate cloud resources.

### Before the first pause

Complete the initial deployment, confirm a successful synthetic order and run the required cloud load test. Finish any active initialization, load or GitOps jobs. Apply the Kafka stage through the reviewed helper; after success it records the configured retention policy privately in GCS. Check Confluent consumer lag and clear any existing backlog before a long pause. Do not shorten topic retention or delete events while the application is paused.

Keep the release and configuration unchanged throughout a pause/resume cycle. Rendered values, the deployed image, the active analytics specification and the GitOps revision must match. The old baseline image does not contain this lifecycle protocol; publish a new immutable image/release from the updated source first.

```powershell
& $deployPython -m deploy.lifecycle status
& $deployPython -m deploy.lifecycle check
& $deployPython -m deploy.lifecycle pause --dry-run
```

`status` reads cloud state and reports node count, SQL activation, analytics cluster state and the recorded lifecycle phase. `check` verifies the expected release/capacity, application readiness, ZooKeeper quorum and GitOps health. Neither command changes cloud resources.

### Pause between sessions

After reviewing the preview:

```powershell
& $deployPython -m deploy.lifecycle pause --execute
& $deployPython -m deploy.lifecycle status
```

The pause operation:

1. Acquires a generation-fenced lease and records a recovery snapshot in the private checkpoint bucket.
2. Suspends automatic reconciliation for the root, platform and food Argo Applications, waiting for any in-progress reconciliation to finish.
3. Marks analytics submission as paused and disables node-pool autoscaling.
4. Stops the web entry point and application workloads; their SQL ledgers, outboxes and leases remain durable.
5. Stops the exact Flink job with a canonical GCS savepoint, without draining pending event-time windows. It verifies completed savepoint metadata and job termination before stopping its VM.
6. Scales ZooKeeper to zero with its persistent volumes retained, then removes the GKE node VMs.
7. Stops Cloud SQL and verifies the expected paused states.

Normal pause never runs Terraform destroy, deletes a cluster/database/bucket/topic, deletes a PVC, revokes credentials or clears Kafka offsets. The public application is unavailable while paused.

The private records are:

- `gs://<project>-<prefix>-checkpoints/lifecycle/journal.json`
- `gs://<project>-<prefix>-checkpoints/lifecycle/analytics.json`
- `gs://<project>-<prefix>-checkpoints/lifecycle/kafka-policy.json`
- Savepoints under `gs://<project>-<prefix>-checkpoints/flink/savepoints/<pause-id>/`

These records contain resource identities and recovery metadata, not credentials. Do not publish them or delete them to force a fresh start. The GCS generation checks prevent two lifecycle operators from overwriting each other.

### Resume before a rehearsal or presentation

Start well before the demonstration and leave time for readiness checks:

```powershell
& $deployPython -m deploy.lifecycle resume --dry-run
& $deployPython -m deploy.lifecycle resume --execute
& $deployPython -m deploy.lifecycle check
```

Resume verifies that the recorded resources were not recreated and that the release/profile still match. It starts SQL and the analytics VM, restores Flink from the recorded savepoint using a new stable run ID, restores application nodes, brings back ZooKeeper quorum and then starts application replicas. HPA-controlled APIs start at their configured minimum rather than their temporary load-test replica counts.

Autoscaling and the previous GitOps policies are restored after application readiness. GitOps hooks consult the shared analytics control record and reuse the resumed run. The operation verifies Healthy/Synced Applications and a RUNNING analytics job before reporting success.

**A lifecycle readiness PASS is not an end-to-end business or load-test PASS.** Place and complete a fresh synthetic order after resuming, check receipt-confirmed notifications and actual analytics output, and retain the required cloud evidence.

### Retention and pause duration

The topic policy now retains events for 21 days instead of seven. The helper requires the successful Kafka-apply receipt to match the intended cluster, broker and rendered retention policy. It sets a scheduling deadline one day inside that window and reports it in UTC through `status`.

With the default policy, resume within 20 days of pausing, and earlier if existing unprocessed events were already old. This deadline is a scheduling guard, not a measurement of every consumer's lag. Confirm the backlog is clear before pausing. Resume refuses an expired margin rather than silently assuming source events still exist. A missing receipt requires applying the reviewed Kafka stage; a missing savepoint, changed policy or expired window requires operator review.

Kafka retention is separate from GCP or Confluent trial expiry. Both accounts must remain usable through the pause and presentation. See [Confluent topic retention configuration](https://docs.confluent.io/cloud/current/topics/manage.html).

### Interrupted operations

Re-running the same command resumes recorded progress. A lost response after savepoint completion is reconciled from the deterministic GCS savepoint prefix, and a lost submission response reuses the same analytics run ID.

If another operator holds a live lease, the command stops. If the prior process died, first confirm it is no longer running. Only after its 30-minute lease has expired may an operator explicitly reclaim it:

```powershell
& $deployPython -m deploy.lifecycle pause --execute --recover
# Or, for an interrupted resume:
& $deployPython -m deploy.lifecycle resume --execute --recover
```

If the exact analytics job created during an unfinished resume is confirmed terminal, inspect its failure first. After correcting the cause without changing the recorded release, explicitly retry that restore:

```powershell
& $deployPython -m deploy.lifecycle resume --execute --retry-analytics
```

That option uses the same savepoint and a new runtime ID only after confirming the prior run ended. It does not silently start from empty state. Missing savepoint metadata, a changed release or a recreated resource requires operator review.

One deployment owner must coordinate Terraform and lifecycle operations; do not launch a Terraform apply concurrently with a pause/resume. Normal Terraform stages refuse to operate when a pause or lifecycle operation is already recorded as unfinished. Use the reviewed stage helper; direct Terraform commands can bypass that workflow guard. The state-bootstrap root remains a separate protected infrastructure operation.

### What still costs money while paused

Stopping compute is not a zero-bill guarantee. Retained database storage, disks, backups, buckets, registry artifacts, networking and an uncovered GKE control-plane fee can continue to be billed. Confluent billing is independent; this helper stops the application's Kafka clients but does not delete or alter the Kafka cluster.

Use Basic Kafka for the demonstration, keep data retention deliberate, check actual GCP/Confluent usage and credit expiry, and avoid extra managed connectors or processing products that the application does not use. Trial credits can cover eligible usage only while they remain valid. The cost estimates discussed during planning are not a provider quote or a proven operating cost.

### Recorded acceptance and presentation checks

Actual IAM/IAP access, infrastructure provisioning, DNS/certificate readiness, managed function receipts, the complete order flow, restored Flink state and both HPAs have been checked on this cloud instance. Keep the measured load envelope and the failed 60-VU latency result visible. Before the presentation, resume early, run lifecycle readiness and complete a fresh order; account/trial availability and future provider startup time still need to be checked on the day.

The original source-only baseline remains intact. An additional source-only snapshot of the budget/lifecycle implementation, captured before its publication, is kept locally at `.local-build/recovery/budget-lifecycle-source.zip` with its SHA-256 manifest. Later documentation edits are not retroactively included in that frozen snapshot. It contains source/configuration only, without images, credentials, Terraform state or runtime data.

## 15. Dedicated Kafka and full cloud log evidence

For a new deployment, Terraform can provision the Confluent Basic cluster, its two service identities, scoped application ACLs, and the four topics. The GCP stages still use their existing Terraform roots. The traffic from this deployment stays in its own Kafka cluster.

1. Fill `deploy/config.team.json` from the example, using the new deployment's GCP project and state bucket. Leave `kafka_cluster_id`, `kafka_rest_endpoint`, and `kafka_bootstrap` as empty strings.
2. Provide a Confluent Cloud management credential privately through the Confluent Terraform provider's `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET` environment variables. Use a management credential with permissions to create the environment, cluster, service accounts and role bindings. The provisioning stage creates separate cluster-scoped runtime and topic-administration keys.
3. Run the reviewed Terraform stages in order. Use the same terminal for the management credential and Confluent stage.

```powershell
python -m deploy.managed_kafka --prepare --cloud AWS --region us-east-2
python -m deploy.cloud_stage state
# Inspect the saved state-bucket plan privately, then apply it.
python -m deploy.cloud_stage state --apply
python -m deploy.cloud_stage confluent
# Inspect the saved Confluent plan privately, then apply it.
python -m deploy.cloud_stage confluent --apply
python -m deploy.managed_kafka
python -m deploy.configure
```

The binding command reads the sensitive Terraform output in memory and creates the two ignored Terraform input files. The command prompts for the operator passphrase. Protect the state bucket, saved plans and local input files because they contain credentials. Keep the original setup in sections 6 and 7 for the existing shared cluster. The dedicated Kafka root is for a fresh deployment, with separate state.

Continue at section 7 with the Cloud and Kafka stages and use the regular release and GitOps steps. For a fresh Dataproc cluster, set `enable_dataproc_job_logging` to `true` in the private configuration before rendering to collect job driver and YARN container logs. This is a Dataproc creation-time setting. For an existing cluster, review the plan for replacement and follow the Flink savepoint restore procedure before scheduling that change.

Cloud Notification delivery runs as the independently deployed Pub/Sub-triggered Cloud Function. It owns the GCS receipt. The GKE notification components manage event routing, subscriptions and inbox/attempt history. Cloud startup requires the function transport, and the order graph only records delivery after the receipt is verified.

After a complete synthetic order and a fresh Flink window, run the cloud log check with the current Dataproc job ID from the release readiness output. Use the start time of this verification run so earlier logs cannot supply the evidence.

```powershell
python -m deploy.cloud_log_check --project YOUR_PROJECT_ID --prefix YOUR_PREFIX --flink-job YOUR_DATAPROC_JOB_ID --since YOUR_ISO_8601_START_TIME
```

The checker requires one recent structured request log from each of the eight services, one delivery receipt event from the Cloud Function, and one window-emission marker from the specific Flink job's YARN container log. It prints source status only. A missing source or query error makes the result incomplete and returns a nonzero exit code. The Flink marker shows a processed window. Verify committed output on `restaurant_analytics_results` as a separate streaming check. The existing Grafana dashboard supplies RPS, error rates, p95 latency and Kubernetes health. The Cloud Terraform root also creates a managed-service dashboard, log-based metrics and alerts.
