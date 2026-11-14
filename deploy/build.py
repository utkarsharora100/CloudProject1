"""Build restricted contexts and extract only public deployment artifacts; publish only on explicit flag."""
import argparse
import hashlib
from pathlib import Path
import subprocess
from deploy.commands import resolve
import uuid
from deploy.configure import ROOT,validate
import json
def run(args):subprocess.run(resolve(args),cwd=ROOT,check=True)
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--release",action="store_true");parser.add_argument("--publish",action="store_true");args=parser.parse_args()
    if args.publish and not args.release:parser.error("--publish requires --release")
    image="cloudproject1-app:handoff";config=None
    if args.release:
        config=validate(json.loads((ROOT/"deploy/config.team.json").read_text()))
        image=f"{config['region']}-docker.pkg.dev/{config['project_id']}/{config['prefix']}-app/app:{config['image_tag']}"
    run(["docker","build","-f","Dockerfile.local","-t",image,"."])
    run(["docker","build","-t","food-notification-function:local","functions/notification_transport"])
    run(["docker","build","-f","restaurant_service/analytics/Dockerfile","-t","food-restaurant-analytics:local","restaurant_service/analytics"])
    output=ROOT/".local-build/artifacts";output.mkdir(parents=True,exist_ok=True)
    name="food-artifact-"+uuid.uuid4().hex[:10]
    try:
        run(["docker","create","--name",name,"food-restaurant-analytics:local"])
        target=output/("restaurant-analytics-"+(config["image_tag"] if config else "local")+".jar")
        run(["docker","cp",name+":/opt/flink/usrlib/restaurant-analytics.jar",str(target)])
    finally:run(["docker","rm","-f",name])
    (target.with_suffix(".jar.sha256")).write_text(hashlib.sha256(target.read_bytes()).hexdigest()+"  "+target.name+"\n")
    if args.publish:
        run(["gcloud","auth","configure-docker",config["region"]+"-docker.pkg.dev","--quiet"])
        run(["docker","push",image])
        run(["gcloud","storage","cp",str(target),f"gs://{config['project_id']}-{config['prefix']}-artifacts/{target.name}","--project",config["project_id"]])
    print("Builds and public artifact checksum are complete. "+("Release artifacts were published." if args.publish else "No registry or cloud object was written."))
if __name__=="__main__":main()
