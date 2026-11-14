"""Repeatable local verification; cloud acceptance is deliberately separate."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from deploy.configure import ROOT
COMPOSE=["docker","compose","-p","food-assignment","--env-file",".env.local.example","--env-file",".env.assignment.example","-f","compose.yaml","-f","compose.assignment.yaml"]
def run(command):
    env=dict(os.environ)
    helm_home=ROOT/".local-build/tools/helm"
    helm_home.mkdir(parents=True,exist_ok=True)
    env.update(HELM_CONFIG_HOME=str(helm_home/"config"),HELM_CACHE_HOME=str(helm_home/"cache"),HELM_DATA_HOME=str(helm_home/"data"))
    subprocess.run(command,cwd=ROOT,check=True,env=env)
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("phase",choices=["static","unit","smoke","flink","telemetry"]);a=p.parse_args()
    if a.phase=="static":
        helm=shutil.which("helm") or str(ROOT/".local-build/tools/helm.exe")
        run(["python","-m","deploy.configure","--example"])
        run([helm,"repo","add","food-external","https://charts.external-secrets.io"])
        run([helm,"repo","add","food-prometheus","https://prometheus-community.github.io/helm-charts"])
        run([helm,"dependency","build","deploy/charts/platform"])
        for chart in ["food","platform"]:run([helm,"lint","deploy/charts/"+chart])
        for root in ["state","cloud","kafka","controller","gitops"]:
            run(["terraform","-chdir=deploy/terraform/"+root,"init","-backend=false","-input=false"])
            run(["terraform","-chdir=deploy/terraform/"+root,"validate"])
    elif a.phase=="unit":
        command=["docker","run","--rm","--network","none","--mount",f"type=bind,source={ROOT/'tests'},target=/app/tests,readonly","--mount",f"type=bind,source={ROOT/'deploy/config.example.json'},target=/app/deploy/config.example.json,readonly","--mount",f"type=bind,source={ROOT/'compose.yaml'},target=/app/compose.yaml,readonly","cloudproject1-app:handoff","python","-m","unittest","discover","-s","tests","-p","test_*.py","-q"]
        run(command)
        for script in ["test_web_ui_poll.mjs","test_web_ui_graph.mjs","test_web_ui_charts.mjs"]:
            path=ROOT/"tests"/script
            if path.exists():run(["node",str(path)])
        run(["docker","run","--rm","--network","none","--entrypoint","python","--mount",f"type=bind,source={ROOT/'functions/notification_transport/tests'},target=/function/tests,readonly","food-notification-function:local","-m","unittest","discover","-s","tests","-q"])
    elif a.phase=="smoke":
        run(COMPOSE+["run","--rm","--no-deps","web-smoke-test"])
    elif a.phase=="telemetry":run(["python","-m","deploy.observability_check"])
    else:
        name=uuid.uuid4().hex[:12];settings={"ANALYTICS_INPUT_TOPIC":"analytics_contract_input_"+name,"ANALYTICS_RESULTS_TOPIC":"analytics_contract_results_"+name,"ANALYTICS_GROUP":"analytics-contract-"+name,"ANALYTICS_TRANSACTION_PREFIX":"analytics-contract-"+name+"-"}
        overlay={"services":{service:{"environment":settings} for service in ["flink-contract","flink-contract-worker","analytics-smoke-test"]}}
        folder=ROOT/".local-build/evidence";folder.mkdir(parents=True,exist_ok=True)
        path=folder/"compose-flink-run.json";path.write_text(json.dumps(overlay,indent=2))
        command=COMPOSE+["-f",str(path)]
        run(command+["run","--rm","--no-deps","analytics-smoke-test","python","-m","local.analytics_smoke_test","init"])
        try:
            run(command+["up","-d","--no-deps","--force-recreate","flink-contract","flink-contract-worker"])
            run(command+["run","--rm","--no-deps","analytics-smoke-test","python","-m","local.analytics_smoke_test","prepare"])
            run(command+["restart","flink-contract-worker"])
            run(command+["run","--rm","--no-deps","analytics-smoke-test","python","-m","local.analytics_smoke_test","finish"])
        finally:
            run(command+["stop","flink-contract-worker","flink-contract"])
if __name__=="__main__":main()
