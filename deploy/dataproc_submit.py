"""GitOps Sync hook: submit one stable Restaurant Flink job using Workload Identity."""
import hashlib
import json
import os
import re
import sys
import time
from urllib.parse import quote
import google.auth
from google.auth.transport.requests import AuthorizedSession
TERMINAL={"DONE","ERROR","CANCELLED","ATTEMPT_FAILURE"}
def spec(env):
    project,region,cluster=(env[k] for k in ("PROJECT_ID","REGION","DATAPROC_CLUSTER"))
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]",project) or not re.fullmatch(r"[a-z]+-[a-z]+[0-9]",region):raise ValueError("Invalid managed job destination.")
    settings={"APP_ENV":"cloud","KAFKA_BOOTSTRAP_SERVERS":env["KAFKA_BOOTSTRAP_SERVERS"],"KAFKA_SECRET_VERSION":env["KAFKA_SECRET_VERSION"],"ANALYTICS_CHECKPOINT_URI":env["ANALYTICS_CHECKPOINT_URI"],"ANALYTICS_WINDOW_SECONDS":"60"}
    jar=env["ANALYTICS_JAR_URI"]
    if not jar.startswith("gs://") or not settings["ANALYTICS_CHECKPOINT_URI"].startswith("gs://"):raise ValueError("Managed jobs require protected Cloud Storage artifacts/checkpoints.")
    digest=hashlib.sha256(json.dumps([settings,jar,env["ANALYTICS_RELEASE"]],sort_keys=True).encode()).hexdigest()[:20]
    job={"reference":{"projectId":project,"jobId":"food-analytics-"+digest},"placement":{"clusterName":cluster},"labels":{"food_component":"restaurant-analytics"},"flinkJob":{"mainClass":"org.fooddelivery.analytics.RestaurantAnalytics","jarFileUris":[jar],"args":["--settings-json="+json.dumps(settings,separators=(",",":"))],"properties":{"parallelism.default":"1","execution.checkpointing.interval":"10s"}}}
    if env.get("ANALYTICS_SAVEPOINT"):job["flinkJob"]["savepointUri"]=env["ANALYTICS_SAVEPOINT"]
    return project,region,job
def submit(session,env):
    project,region,job=spec(env);base=f"https://dataproc.googleapis.com/v1/projects/{project}/regions/{region}/jobs"
    identity=job["reference"]["jobId"]
    response=session.get(base+"/"+identity,timeout=15)
    if response.status_code==200:
        state=response.json().get("status",{}).get("state")
        if state in TERMINAL:raise RuntimeError("The release job is terminal. Review it and use a new analytics release with a recovery savepoint.")
        return identity,"existing"
    if response.status_code!=404:raise RuntimeError("Managed job lookup failed.")
    existing=session.get(base,params={"filter":"labels.food_component=restaurant-analytics","pageSize":100},timeout=15)
    if existing.status_code!=200:raise RuntimeError("Managed job enumeration failed.")
    if existing.json().get("nextPageToken"):raise RuntimeError("Job history needs explicit operator review before a new release.")
    if any(row.get("status",{}).get("state") not in TERMINAL for row in existing.json().get("jobs",[])):
        raise RuntimeError("Another analytics release is active. Savepoint and stop it before changing the release.")
    response=session.post(base+":submit",json={"job":job},timeout=30)
    if response.status_code not in {200,201,409}:raise RuntimeError("Managed Flink submission was not confirmed.")
    return identity,"submitted"
def main():
    try:
        credential,_=google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        identity,outcome=submit(AuthorizedSession(credential),os.environ)
        # Do not declare the hook healthy until Dataproc confirms RUNNING.
        session=AuthorizedSession(credential);project,region,_=spec(os.environ)
        for _ in range(90):
            result=session.get(f"https://dataproc.googleapis.com/v1/projects/{project}/regions/{region}/jobs/{identity}",timeout=15)
            if result.status_code==200:
                state=result.json().get("status",{}).get("state")
                if state=="RUNNING":
                    print(json.dumps({"service":"restaurant-analytics-launcher","event":"managed_job_running","outcome":outcome}),flush=True);return
                if state in TERMINAL:raise RuntimeError("The managed job entered a terminal state before readiness.")
            time.sleep(5)
        raise RuntimeError("Managed Flink readiness timed out.")
    except Exception:
        print("Managed analytics deployment needs review. Inspect Dataproc job status through the authorized console; no credentials or provider response were printed.",file=sys.stderr)
        raise SystemExit(1)
if __name__=="__main__":main()
