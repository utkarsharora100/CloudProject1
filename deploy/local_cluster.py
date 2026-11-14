"""Prepare a sanitized, disposable local GitOps fixture; never changes the dev branch."""
import json
from pathlib import Path
import shutil
import subprocess
ROOT=Path(__file__).resolve().parents[1]
COMPOSE=["docker","compose","-p","food-assignment","--env-file",".env.local.example","--env-file",".env.assignment.example","-f","compose.yaml","-f","compose.assignment.yaml"]
def run(command,**kwargs):return subprocess.run(command,cwd=ROOT,check=True,**kwargs)
def main():
    fixture=ROOT/".local-build/gitops-fixture"
    fixture.mkdir(parents=True,exist_ok=True)
    if (fixture/".git").exists():raise SystemExit("Disposable fixture already exists. Keep it for evidence; create a new test directory deliberately for another run.")
    shutil.copytree(ROOT/"deploy/charts/food",fixture/"chart",ignore=shutil.ignore_patterns("charts","*.tgz"))
    # Remove cloud-only resources from the local fixture, never from the cloud chart.
    values=json.loads((fixture/"chart/values.yaml").read_text())
    values.update(profile="local",image="cloudproject1-app:handoff",imagePullPolicy="Never",namespace="food")
    import sys
    if "--full" not in sys.argv:
        values["workloads"]={name:item for name,item in values["workloads"].items() if name in {"customer-service","order-service","restaurant-api","restaurant-service"}}
    values["ingress"]["enabled"]=False;values["networkPolicy"]["enabled"]=False
    values["metrics"]["enabled"]=False;values["secrets"]["enabled"]=False
    values["managedFlink"]["enabled"]=False;values["zookeeper"]["enabled"]=False
    values["serviceAccounts"]={key:"" for key in values["serviceAccounts"]}
    config=json.loads(subprocess.check_output(COMPOSE+["config","--format","json"],cwd=ROOT,encoding="utf-8"))
    local=config["services"]["order-service"]["environment"]
    expected={}
    for line in (ROOT/".env.local.example").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key,value=line.split("=",1);expected[key]=value.strip().strip('"')
    if local["DB_PASSWORD"]!=expected["LOCAL_DB_PASSWORD"]:raise RuntimeError("Refusing to copy a host credential override into the disposable fixture.")
    from deploy.export_handoff import known_private_values
    if any(value in local["DB_PASSWORD"].encode() for value in known_private_values()):raise RuntimeError("Refusing a known exposed credential in the local fixture.")
    values["env"].update(APP_ENV="local",PROJECT_ID="demo-food-delivery",DB_PRIVATE_IP="mysql",DB_USER=local["DB_USER"],DB_NAME=local["DB_NAME"],DB_PORT="3306",KAFKA_BOOTSTRAP_SERVERS="kafka:9092",KAFKA_SECURITY_PROTOCOL="PLAINTEXT",FIRESTORE_EMULATOR_HOST="firestore:8080",PUBSUB_EMULATOR_HOST="pubsub:8085",STORAGE_EMULATOR_HOST="http://gcs:4443",NOTIFICATION_RECEIPTS_BUCKET="food-notification-receipts",NOTIFICATION_DISPATCH_TOPIC="notification-dispatch")
    values["env"].pop("CLOUD_DEPLOYMENT_ACK",None)
    (fixture/"chart/values.yaml").write_text(json.dumps(values,indent=2))
    run(["docker","run","-d","--name","food-kind-backends","--network","kind","-e","APP_ENV=local","--mount",f"type=bind,source={ROOT/'deploy/local_proxy.py'},target=/local_proxy.py,readonly","cloudproject1-app:handoff","python","/local_proxy.py"],stdout=subprocess.DEVNULL)
    run(["docker","network","connect","food-assignment_default","food-kind-backends"])
    info=json.loads(subprocess.check_output(["docker","inspect","food-kind-backends"],encoding="utf-8"))[0]
    proxy_ip=info["NetworkSettings"]["Networks"]["kind"]["IPAddress"]
    external=[]
    for name,port in [("mysql",3306),("kafka",9092),("firestore",8080),("gcs",4443),("pubsub",8085),("zookeeper-1",2181),("zookeeper-2",2181),("zookeeper-3",2181)]:
        cid=subprocess.check_output(COMPOSE+["ps","-q",name],cwd=ROOT,encoding="utf-8").strip()
        info=json.loads(subprocess.check_output(["docker","inspect",cid],encoding="utf-8"))[0]
        ip=proxy_ip
        target_port=22180+int(name.rsplit("-",1)[1]) if name.startswith("zookeeper-") else port
        external.extend([{"apiVersion":"v1","kind":"Service","metadata":{"name":name},"spec":{"ports":[{"name":"backend","port":port,"targetPort":port}]}},{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":name+"-docker","labels":{"kubernetes.io/service-name":name}},"addressType":"IPv4","ports":[{"name":"backend","protocol":"TCP","port":target_port}],"endpoints":[{"addresses":[ip],"conditions":{"ready":True}}]}])
    external.append({"apiVersion":"v1","kind":"Secret","metadata":{"name":"food-db"},"type":"Opaque","stringData":{"DB_PASSWORD":local["DB_PASSWORD"]}})
    for index,item in enumerate(external):
        (fixture/f"chart/templates/local-backend-{index}.yaml").write_text(json.dumps(item,indent=2))
    import urllib.request,hashlib
    raw=urllib.request.urlopen("https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.8.0/components.yaml",timeout=30).read()
    assert hashlib.sha256(raw).hexdigest()=="ff64d1a13b9ac3b0635f0dd985815fb44c23eed4706c04e5db1daadf6bc0a83b"
    metrics=raw.decode().replace("        - --cert-dir=/tmp","        - --kubelet-insecure-tls\n        - --cert-dir=/tmp")
    (fixture/"chart/templates/local-metrics.yaml").write_text(metrics)
    # Only public chart code + clearly local dummy DB values go into this NEW disposable repository.
    run(["git","init","--initial-branch=fixture",str(fixture)],stdout=subprocess.DEVNULL)
    for args in [["config","user.name","Local GitOps Verification"],["config","user.email","fixture@example.test"],["add","chart"],["commit","-m","Disposable sanitized GitOps fixture"]]:
        run(["git","-C",str(fixture),*args],stdout=subprocess.DEVNULL)
    server=ROOT/".local-build/gitops-server";server.mkdir(exist_ok=True)
    (server/"Dockerfile").write_text("FROM alpine:3.22\nRUN apk add --no-cache git git-daemon\nRUN git config --system --add safe.directory /repos/gitops-fixture\nEXPOSE 9418\nENTRYPOINT [\"git\",\"daemon\",\"--reuseaddr\",\"--base-path=/repos\",\"--export-all\",\"--listen=0.0.0.0\",\"--verbose\",\"/repos\"]\n")
    run(["docker","build","-t","food-local-gitops:fixture",str(server)],stdout=subprocess.DEVNULL)
    run(["docker","run","-d","--name","food-local-gitops","--network","kind","--mount",f"type=bind,source={fixture},target=/repos/gitops-fixture,readonly","food-local-gitops:fixture"],stdout=subprocess.DEVNULL)
    run([str(ROOT/".local-build/tools/kind.exe"),"load","docker-image","cloudproject1-app:handoff","--name","food-assignment"])
    target=ROOT/".local-build/kind/gitops";target.mkdir(parents=True,exist_ok=True)
    tf=json.loads((ROOT/"deploy/terraform/gitops/main.tf.json").read_text())
    tf["terraform"].pop("backend",None)
    root=tf["resource"]["kubernetes_manifest"]["root"]["manifest"]
    root["metadata"]["name"]="food-local"
    root["spec"]["source"].pop("directory",None)
    root["spec"]["source"]["path"]="chart"
    root["spec"]["destination"]["namespace"]="food"
    (target/"main.tf.json").write_text(json.dumps(tf,indent=2))
    (target/"terraform.tfvars.json").write_text(json.dumps({"kubeconfig_path":str(ROOT/".local-build/kind/kubeconfig"),"kube_context":"kind-food-assignment","repository":"git://food-local-gitops/gitops-fixture","revision":"fixture"}))
    if "--bootstrap" in sys.argv:
        controller=ROOT/".local-build/kind/controller";controller.mkdir(parents=True,exist_ok=True)
        config=json.loads((ROOT/"deploy/terraform/controller/main.tf.json").read_text());config["terraform"].pop("backend",None)
        helm_values=json.loads(config["resource"]["helm_release"]["argocd"]["values"][0])
        helm_values["configs"]["cm"]["resource.exclusions"]=""
        config["resource"]["helm_release"]["argocd"]["values"]=[json.dumps(helm_values)]
        (controller/"main.tf.json").write_text(json.dumps(config,indent=2))
        (controller/"terraform.tfvars.json").write_text(json.dumps({"kubeconfig_path":str(ROOT/".local-build/kind/kubeconfig"),"kube_context":"kind-food-assignment"}))
        for stage in ("controller","gitops"):
            run(["terraform","-chdir=.local-build/kind/"+stage,"init","-input=false"])
            run(["terraform","-chdir=.local-build/kind/"+stage,"apply","-input=false","-auto-approve"])
    print("Sanitized local fixture is ready. Current development repository and remote were untouched.")
if __name__=="__main__":main()
