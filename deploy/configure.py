"""Render non-secret cloud inputs from one reviewed team configuration; never deploys."""
import argparse
import ipaddress
import json
from pathlib import Path
import re
from urllib.parse import urlparse
import zipfile
ROOT=Path(__file__).resolve().parents[1]
FIELDS={"project_id","region","zone","prefix","public_host","git_repository","git_revision","image_tag","kafka_bootstrap","kafka_cluster_id","kafka_rest_endpoint","dns_zone","admin_cidr","create_firestore","state_bucket","operator_user"}
def validate(v,allow_example=False):
    if not isinstance(v,dict) or set(v)!=FIELDS:raise ValueError("Use exactly the non-secret fields in config.example.json.")
    if any(type(value) is not str for key,value in v.items() if key!="create_firestore") or type(v["create_firestore"]) is not bool:raise ValueError("Invalid configuration types.")
    checks={"project_id":r"[a-z][a-z0-9-]{4,28}[a-z0-9]","prefix":r"[a-z][a-z0-9-]{2,14}","region":r"[a-z]+-[a-z]+[0-9]","zone":r"[a-z]+-[a-z]+[0-9]-[a-z]","public_host":r"[a-z0-9][a-z0-9.-]+\.[a-z]{2,}","image_tag":r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}","operator_user":r"[A-Za-z0-9_-]{3,40}","state_bucket":r"[a-z0-9][a-z0-9.-]{3,61}[a-z0-9]"}
    for key,pattern in checks.items():
        if not re.fullmatch(pattern,v[key]):raise ValueError("Invalid non-secret field: "+key)
    if v["image_tag"].lower() in {"latest","dev","local"}:raise ValueError("Use an immutable release image tag.")
    if not v["zone"].startswith(v["region"]+"-"):raise ValueError("Zone must belong to the configured region.")
    network=ipaddress.ip_network(v["admin_cidr"],strict=True)
    if network.prefixlen<24:raise ValueError("Restrict the control-plane administrator network to /24 or narrower.")
    for key in ("git_repository","kafka_rest_endpoint"):
        parsed=urlparse(v[key])
        if parsed.scheme!="https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:raise ValueError("Use credential-free HTTPS for "+key)
    if not re.fullmatch(r"[A-Za-z0-9.-]+:9092",v["kafka_bootstrap"]):raise ValueError("Kafka must use a host:9092 endpoint.")
    if not re.fullmatch(r"lkc-[A-Za-z0-9]+",v["kafka_cluster_id"]):raise ValueError("Invalid Kafka cluster ID.")
    if not re.fullmatch(r"[A-Za-z0-9._/-]+",v["git_revision"]) or ".." in v["git_revision"]:raise ValueError("Invalid Git revision.")
    if v["dns_zone"] and not re.fullmatch(r"[a-z][a-z0-9-]{0,62}",v["dns_zone"]):raise ValueError("Invalid existing Cloud DNS zone name.")
    if not allow_example and any(marker in json.dumps(v).lower() for marker in ("replace","example.edu","203.0.113.")):raise ValueError("Replace all example project/domain/Kafka/control-plane values before rendering team configuration.")
    return v

def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+"\n",encoding="utf-8")

def render(v,target="team",outputs=None):
    project,prefix,region=v["project_id"],v["prefix"],v["region"]
    base=ROOT/"deploy/environments"/target
    values=json.loads((ROOT/"deploy/charts/food/values.yaml").read_text())
    values.update(projectId=project,clusterName=prefix+"-gke",clusterLocation=v["zone"],image=f"{region}-docker.pkg.dev/{project}/{prefix}-app/app:{v['image_tag']}",host=v["public_host"],publicOrigin="https://"+v["public_host"],cloudSqlConnection=f"{project}:{region}:{prefix}-sql")
    values["env"].update(PROJECT_ID=project,KAFKA_BOOTSTRAP_SERVERS=v["kafka_bootstrap"],NOTIFICATION_RECEIPTS_BUCKET=f"{project}-{prefix}-receipts",NOTIFICATION_DISPATCH_TOPIC=prefix+"-notification-dispatch")
    values["serviceAccounts"]={k:f"{prefix}-{g}@{project}.iam.gserviceaccount.com" for k,g in {"sql":"sql","firestore":"firestore","combined":"combined","notification":"notify","eso":"secrets","launcher":"launcher"}.items()}
    values["managedFlink"]["env"].update(REGION=region,DATAPROC_CLUSTER=prefix+"-flink",KAFKA_SECRET_VERSION=f"projects/{project}/secrets/{prefix}-kafka/versions/1",ANALYTICS_CHECKPOINT_URI=f"gs://{project}-{prefix}-checkpoints/flink",ANALYTICS_JAR_URI=f"gs://{project}-{prefix}-artifacts/restaurant-analytics-{v['image_tag']}.jar",ANALYTICS_RELEASE=v["image_tag"])
    values["ingress"]["ipName"]=prefix+"-web-ip"
    values["secrets"].update(db=prefix+"-db-password",kafka=prefix+"-kafka",web=prefix+"-web-access")
    if outputs:
        versions=outputs["secret_versions"]["value"]
        values["secrets"]["versions"]={key:str(versions[name]) for key,name in {"db":"db-password","kafka":"kafka","web":"web-access"}.items()}
        values["managedFlink"]["env"]["KAFKA_SECRET_VERSION"]=outputs["kafka_secret_version"]["value"]
    write(base/"food-values.json",values)
    apps=[]
    for name,path,ns,wave,extra in [("food-platform","deploy/charts/platform","platform","0",{}),("food","deploy/charts/food","food","10",{"helm":{"valueFiles":["../../environments/"+target+"/food-values.json"]}})]:
        apps.append({"apiVersion":"argoproj.io/v1alpha1","kind":"Application","metadata":{"name":name,"namespace":"argocd","annotations":{"argocd.argoproj.io/sync-wave":wave},"finalizers":["resources-finalizer.argocd.argoproj.io"]},"spec":{"project":"default","source":{"repoURL":v["git_repository"],"targetRevision":v["git_revision"],"path":path,**extra},"destination":{"server":"https://kubernetes.default.svc","namespace":ns},"syncPolicy":{"automated":{"prune":True,"selfHeal":True},"syncOptions":["CreateNamespace=true","ServerSideApply=true"]}}})
    # JSON List is valid Kubernetes/YAML and contains only deployable child Applications.
    write(base/"applications.yaml",{"apiVersion":"v1","kind":"List","items":apps})
    ignored=ROOT/".local-build/config"/target;ignored.mkdir(parents=True,exist_ok=True)
    cloud={k:v[k] for k in ("project_id","region","zone","prefix","kafka_bootstrap","public_host","dns_zone","admin_cidr","create_firestore","operator_user")}
    write(ignored/"cloud.tfvars.json",cloud)
    write(ignored/"state.tfvars.json",{k:v[k] for k in ("project_id","region","state_bucket")})
    write(ignored/"kafka.tfvars.json",{"cluster_id":v["kafka_cluster_id"],"rest_endpoint":v["kafka_rest_endpoint"]})
    write(ignored/"gitops.tfvars.json",{"repository":v["git_repository"],"revision":v["git_revision"]})
    for root in ("cloud","controller","gitops","kafka"):
        (ignored/(root+"-backend.hcl")).write_text('bucket = "'+v["state_bucket"]+'"\nprefix = "food/'+root+'"\n',encoding="utf-8")
    artifact=ROOT/".local-build/artifacts/notification-function.zip";artifact.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(artifact,"w",zipfile.ZIP_DEFLATED) as archive:
        for name in ("main.py","requirements.txt"):archive.write(ROOT/"functions/notification_transport"/name,name)
    return base
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",default="deploy/config.team.json");parser.add_argument("--example",action="store_true");parser.add_argument("--cloud-outputs")
    args=parser.parse_args()
    try:
        config=ROOT/("deploy/config.example.json" if args.example else args.config)
        v=validate(json.loads(config.read_text(encoding="utf-8")),allow_example=args.example)
        path=render(v,"example" if args.example else "team",json.loads(Path(args.cloud_outputs).read_text()) if args.cloud_outputs else None)
    except (OSError,ValueError,KeyError):raise SystemExit("Configuration rejected. Check the example field names, non-secret values and README; no values were printed.")
    print("Rendered non-secret chart/GitOps inputs and ignored Terraform inputs: "+str(path))
    print("No infrastructure was provisioned. Secret values must use deploy.secret_inputs.")
if __name__=="__main__":main()
