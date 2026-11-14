"""Explicit cloud-team Terraform stages. No stage runs without a selected command."""
import argparse
import json
import os
from pathlib import Path
import subprocess
from deploy.commands import resolve
from deploy.configure import ROOT,validate
def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("stage",choices=["state","cloud","kafka","kubeconfig","controller","gitops"])
    p.add_argument("--apply",action="store_true",help="Apply the reviewed saved plan; otherwise only plan.")
    a=p.parse_args()
    config=validate(json.loads((ROOT/"deploy/config.team.json").read_text()))
    inputs=ROOT/".local-build/config/team";folder=ROOT/"deploy/terraform"/a.stage
    env=dict(os.environ)
    kube=ROOT/".local-build/cloud/kubeconfig";kube.parent.mkdir(parents=True,exist_ok=True)
    context=f"gke_{config['project_id']}_{config['zone']}_{config['prefix']}-gke"
    def run(command,**kwargs):subprocess.run(resolve(command),cwd=ROOT,env=env,check=True,**kwargs)
    if a.stage=="kubeconfig":
        if a.apply:p.error("kubeconfig does not use --apply.")
        env["KUBECONFIG"]=str(kube)
        run(["gcloud","container","clusters","get-credentials",config["prefix"]+"-gke","--zone",config["zone"],"--project",config["project_id"]])
        print("Cloud kubeconfig stored only in the ignored workspace directory.");return
    command=["terraform","-chdir="+str(folder)]
    init=["init","-input=false"]
    if a.stage!="state":init+=["-backend-config="+str(inputs/(a.stage+"-backend.hcl"))]
    run(command+init)
    variables=inputs/(a.stage+".tfvars.json")
    if a.stage in {"controller","gitops"}:
        value={"kubeconfig_path":str(kube),"kube_context":context}
        if a.stage=="gitops":value.update(repository=config["git_repository"],revision=config["git_revision"])
        variables.write_text(json.dumps(value,indent=2)+"\n")
    if a.stage in {"cloud","kafka"} and not (folder/"secrets.auto.tfvars.json").exists():raise SystemExit("Create the ignored private input with deploy.secret_inputs before planning this stage.")
    plan=inputs/(a.stage+".tfplan")
    run(command+["plan","-input=false","-var-file="+str(variables),"-out="+str(plan)])
    print("Plan saved in the ignored configuration directory; plans and state can contain secrets.")
    if a.apply:
        run(command+["apply","-input=false",str(plan)])
        if a.stage=="cloud":
            result=subprocess.run(command+["output","-json"],cwd=ROOT,env=env,check=True,capture_output=True,encoding="utf-8")
            # This root defines only non-secret resource identifiers and secret-version references.
            (inputs/"cloud-outputs.json").write_text(result.stdout,encoding="utf-8")
        print("Selected cloud stage applied. Follow the next ordered runbook step.")
    else:print("No infrastructure was applied. Review the plan before choosing --apply.")
if __name__=="__main__":main()
