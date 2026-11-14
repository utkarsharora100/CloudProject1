"""Explicit local/cloud profiles. Endpoints and access are configuration, not source edits."""
import os
import re
from urllib.parse import urlparse

LOCAL_ZK={"zookeeper-1:2181","zookeeper-2:2181","zookeeper-3:2181","localhost:2181","localhost:2182","localhost:2183","127.0.0.1:2181","127.0.0.1:2182","127.0.0.1:2183"}
INTERNAL=re.compile(r"^[a-z][a-z0-9-]*(?:\.[a-z0-9-]+)*$")
def cloud(environ=None):
    return (os.environ if environ is None else environ).get("APP_ENV")=="cloud"
def require_profile(mode=None,environ=None):
    env=os.environ if environ is None else environ
    mode=mode or env.get("APP_ENV","production")
    if mode=="local":return mode
    if mode!="cloud" or env.get("CLOUD_DEPLOYMENT_ACK")!="assignment-private-network-v1":
        raise RuntimeError("Services are local-demo only unless the reviewed private cloud deployment profile is configured.")
    project=env.get("PROJECT_ID","")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]",project) or project.startswith("demo-"):
        raise RuntimeError("Cloud deployment requires an explicit real project ID.")
    if env.get("FIRESTORE_EMULATOR_HOST") or env.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise RuntimeError("Cloud workloads require Workload Identity, without emulators or credential files.")
    return mode
def service_endpoint(value,allowed,*,grpc=False,environ=None):
    env=os.environ if environ is None else environ
    if env.get("APP_ENV")!="cloud":
        if value not in allowed:raise RuntimeError("Local service destination is outside the isolated stack.")
        return value
    require_profile(environ=env)
    parsed=urlparse("//"+value if grpc else value)
    if (not grpc and parsed.scheme!="http") or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment:
        raise RuntimeError("Cloud service destinations must be internal service endpoints.")
    try:port=parsed.port
    except ValueError:raise RuntimeError("Invalid internal service port.") from None
    names=set(filter(None,env.get("INTERNAL_SERVICE_HOSTS","customer-service,restaurant-service,restaurant-api,order-service,payment-service,kitchen-api,delivery-service,notification-api,task-api,restaurant-analytics-api").split(",")))
    if parsed.hostname not in names or not port or not INTERNAL.fullmatch(parsed.hostname):
        raise RuntimeError("Destination is outside the configured private service allowlist.")
    return value
def firestore_client(environ=None):
    env=os.environ if environ is None else environ
    from google.cloud import firestore
    if env.get("APP_ENV")=="cloud":
        require_profile(environ=env)
        return firestore.Client(project=env["PROJECT_ID"])
    from google.auth.credentials import AnonymousCredentials
    if env.get("APP_ENV")!="local" or env.get("PROJECT_ID")!="demo-food-delivery" or env.get("FIRESTORE_EMULATOR_HOST") not in {"firestore:8080","localhost:8080","127.0.0.1:8080"}:
        raise RuntimeError("Local data access requires the isolated Firestore emulator.")
    return firestore.Client(project="demo-food-delivery",credentials=AnonymousCredentials())
def zk_hosts(environ=None):
    env=os.environ if environ is None else environ
    hosts=env.get("ZK_HOSTS","")
    if env.get("APP_ENV")=="cloud":
        require_profile(environ=env)
        expected=set(filter(None,env.get("ZK_ALLOWED_HOSTS","").split(",")))
        if len(expected)<3 or not hosts or not set(hosts.split(","))<=expected:
            raise RuntimeError("Cloud coordination requires three explicitly configured private ensemble members.")
    elif env.get("APP_ENV")!="local" or not hosts or not set(hosts.split(","))<=LOCAL_ZK:
        raise RuntimeError("Scheduler coordination requires the isolated local ZooKeeper ensemble.")
    return hosts
