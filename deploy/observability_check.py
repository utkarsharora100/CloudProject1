"""Record real local telemetry evidence without exporting log contents or contact fields."""
import json
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import urlopen
ROOT=Path(__file__).resolve().parents[1]
def get(url):return json.load(urlopen(url,timeout=8))
def main():
    targets=get("http://127.0.0.1:19090/api/v1/targets")["data"]["activeTargets"]
    up=[x["labels"]["instance"] for x in targets if x["health"]=="up"]
    assert len(targets)>=12 and len(up)==len(targets),"A configured metrics target is unavailable."
    counts={}
    for service,event in [("notification-function","transport_receipt"),("restaurant-analytics","flink_log"),("customer","http_request"),("order","http_request"),("payment","http_request"),("kitchen","http_request"),("delivery","http_request"),("notification","http_request"),("restaurant","http_request"),("scheduler","http_request")]:
        query='{service="'+service+'"} |= "'+event+'"'
        data=get("http://127.0.0.1:13100/loki/api/v1/query_range?"+urlencode({"query":query,"limit":2,"start":str(int((time.time()-7200)*1e9))}))
        counts[service]=sum(len(row["values"]) for row in data["data"]["result"])
        assert counts[service]>0,"Expected service logs were not ingested."
    dashboard=get("http://127.0.0.1:13000/api/dashboards/uid/supper-system")
    assert dashboard["dashboard"]["panels"],"Grafana dashboard was not provisioned."
    output={"result":"PASS","metrics_targets_up":up,"sanitized_service_log_records_observed":counts,"grafana_dashboard_provisioned":True,"scope":"Local Compose telemetry. Kubernetes panels require a cluster exporter."}
    path=ROOT/".local-build/evidence/observability.json";path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(output,indent=2)+"\n")
    print(json.dumps(output))
if __name__=="__main__":main()
