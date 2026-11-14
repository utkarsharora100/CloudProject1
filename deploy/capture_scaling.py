"""Capture actual HPA decisions and completed k6 outcomes; never manufactures scaling success."""
import argparse
import json
from pathlib import Path
import subprocess
import time
from datetime import datetime,timezone
from deploy.configure import ROOT
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--kubeconfig",required=True);p.add_argument("--context",required=True);p.add_argument("--job",required=True);p.add_argument("--scope",choices=["local","gke"],default="local");p.add_argument("--timeout",type=int,default=900);a=p.parse_args()
    k=["kubectl","--kubeconfig",a.kubeconfig,"--context",a.context,"--request-timeout=10s"]
    def get(args):
        r=subprocess.run(k+args+["-o","json"],cwd=ROOT,capture_output=True,encoding="utf-8")
        return json.loads(r.stdout) if r.returncode==0 else None
    if a.scope=="gke":
        nodes=get(["get","nodes"])
        if not nodes or not all(v["spec"].get("providerID","").startswith("gce://") for v in nodes["items"]):raise SystemExit("GKE evidence requires actual GCE-backed cluster nodes.")
    initial={};samples=[];last=None;deadline=time.monotonic()+a.timeout
    while time.monotonic()<deadline:
        hpas=get(["get","hpa","-n","food"]);job=get(["get","job",a.job,"-n","food"])
        if hpas:
            values={}
            for item in hpas["items"]:
                name=item["metadata"]["name"]
                if name not in {"customer-service","order-service"}:continue
                assert {r["resource"]["name"] for r in item["spec"]["metrics"]}=={"cpu","memory"}
                count=item["status"].get("currentReplicas",0)
                if count:initial.setdefault(name,count)
                values[name]={"current":count,"desired":item["status"].get("desiredReplicas"),"metrics":item["status"].get("currentMetrics",[]),"conditions":item["status"].get("conditions",[])}
            samples.append({"at":datetime.now(timezone.utc).isoformat(),"hpas":values})
            current={key:row["current"] for key,row in values.items()}
            if current!=last:print("Actual replicas: "+json.dumps(current),flush=True);last=current
        if job and (job.get("status",{}).get("succeeded") or job.get("status",{}).get("failed")):break
        time.sleep(10)
    else:raise SystemExit("Scaling capture timed out before k6 completion.")
    result=subprocess.run(k+["logs","-n","food","job/"+a.job],cwd=ROOT,capture_output=True,encoding="utf-8")
    summaries=[line.split("K6_SUMMARY ",1)[1] for line in result.stdout.splitlines() if "K6_SUMMARY " in line]
    summary=json.loads(summaries[-1]) if summaries else None
    maximum={name:max(sample["hpas"].get(name,{}).get("current",0) for sample in samples) for name in initial}
    scaled=all(maximum[name]>initial[name] for name in ("customer-service","order-service"))
    timing_valid=bool(summary and summary.get("metrics",{}).get("http_req_duration",{}).get("min",-1)>=0)
    functional_pass=bool(job["status"].get("succeeded") and summary and scaled)
    report={"result":"PASS" if functional_pass and timing_valid else "FAIL","functional_load_and_scale_pass":functional_pass,"latency_sample_bounds_valid":timing_valid,"scope":a.scope,"initial_replicas":initial,"maximum_replicas":maximum,"both_scaled_out":scaled,"k6":summary,"samples":samples}
    folder=ROOT/".local-build/evidence";folder.mkdir(parents=True,exist_ok=True);(folder/("scaling-"+a.scope+".json")).write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps({key:value for key,value in report.items() if key!="samples"}))
    if report["result"]!="PASS":raise SystemExit("Evidence did not confirm scale-out, successful traffic and valid latency bounds together. Review the saved measurements; negative request durations are invalid timing evidence.")
if __name__=="__main__":main()
