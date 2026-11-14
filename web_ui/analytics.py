"""Measured, workspace-scoped aggregates; no addresses, contacts or lease proofs."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone

STAGES={"PAID":"Payment confirmed","KITCHEN_ACCEPTED":"Kitchen accepted","PREPARING":"Preparing",
    "READY":"Ready","COURIER_ASSIGNED":"Courier assigned","PICKED_UP":"Picked up","DELIVERED":"Delivered"}
def parse(value):
    return datetime.fromisoformat(value.replace("Z","+00:00")) if value else None

def seconds(start,end):
    if not start or not end:return None
    value=(parse(end)-parse(start)).total_seconds()
    return round(value,3) if value>=0 else None

def history_metrics(events):
    times={}
    for event in events:
        times.setdefault(event["to_state"],event["occurred_at"])
    return {"preparation_seconds":seconds(times.get("PREPARING"),times.get("READY")),
        "delivery_seconds":seconds(times.get("PICKED_UP"),times.get("DELIVERED")),
        "assignment_wait_seconds":seconds(times.get("READY"),times.get("COURIER_ASSIGNED")),
        "total_seconds":seconds(times.get("PENDING_PAYMENT"),times.get("DELIVERED")),"stage_times":times}

def aggregate(orders,histories,*,hours=24,now=None):
    now=now or datetime.now(timezone.utc);start=now-timedelta(hours=hours)
    scoped=[row for row in orders if start<=parse(row["created_at"])<=now]
    counts=Counter(row["status"] for row in scoped)
    buckets=[]
    for index in range(hours+1):
        point=start.replace(minute=0,second=0,microsecond=0)+timedelta(hours=index)
        buckets.append({"start":point.isoformat(),"label":point.strftime("%H:%M"),"count":0})
    bucket_map={parse(row["start"]).replace(minute=0,second=0,microsecond=0):row for row in buckets}
    series=[];prep=[];delivery=[];incomplete=0
    for row in scoped:
        stamp=parse(row["created_at"]).replace(minute=0,second=0,microsecond=0)
        if stamp in bucket_map:bucket_map[stamp]["count"]+=1
        events=histories.get(row["order_id"])
        metrics=history_metrics(events) if events is not None else None
        if metrics:
            if metrics["preparation_seconds"] is not None:prep.append(metrics["preparation_seconds"])
            if metrics["delivery_seconds"] is not None:delivery.append(metrics["delivery_seconds"])
            series.append({"order_id":row["order_id"],"created_at":row["created_at"],
                "preparation_seconds":metrics["preparation_seconds"],"delivery_seconds":metrics["delivery_seconds"]})
        else:incomplete+=1
    return {"scope":"Current demo workspace","timezone":"UTC","hours":hours,"as_of":now.isoformat(),
        "order_count":len(scoped),"delivered_count":counts["DELIVERED"],"stage_counts":dict(counts),
        "preparation":{"mean_seconds":round(sum(prep)/len(prep),2) if prep else None,"sample_count":len(prep)},
        "delivery":{"mean_seconds":round(sum(delivery)/len(delivery),2) if delivery else None,"sample_count":len(delivery)},
        "history_unavailable_count":incomplete,"arrivals":buckets,"durations":series,
        "orders":[{key:row[key] for key in ("order_id","status","total_minor","currency","created_at","restaurant_id")} for row in scoped]}

def collect(client,orders,*,hours=24):
    def fetch(row):
        try:return row["order_id"],client.call("order","GET","/orders/"+row["order_id"]+"/events")["events"]
        except Exception:return row["order_id"],None
    with ThreadPoolExecutor(max_workers=6) as pool:histories=dict(pool.map(fetch,orders))
    return aggregate(orders,histories,hours=hours)
