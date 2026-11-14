"""Browser-facing HTTP smoke: real services, explicit actors, no SQL/cloud credentials."""
import json
import re
import time
import uuid
from http.cookiejar import CookieJar
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor,Request,build_opener

BASE="http://web-ui:3000"
class Web:
    def __init__(self):
        self.opener=build_opener(HTTPCookieProcessor(CookieJar()))
        status,html=self.request("GET","/",html=True)
        assert status==200,"Web entry page unavailable"
        self.csrf=re.search(r'name="csrf-token" content="([^"]+)"',html)[1]
    def request(self,method,path,body=None,*,html=False,headers=None):
        values={"Accept":"text/html" if html else "application/json"}
        if body is not None:values.update({"Content-Type":"application/json","X-CSRF-Token":self.csrf})
        values.update(headers or {})
        req=Request(BASE+path,method=method,headers=values,data=json.dumps(body).encode() if body is not None else None)
        try:response=self.opener.open(req,timeout=35)
        except HTTPError as error:response=error
        with response:
            raw=response.read().decode()
            return response.status,raw if html else json.loads(raw)
    def get(self,path):
        status,data=self.request("GET",path)
        assert status==200,"GET "+path+" returned "+str(status)
        return data
    def post(self,path,body=None,*,op=None,expected=(200,201,202)):
        status,data=self.request("POST",path,{**(body or {}),"operation_id":op or str(uuid.uuid4())})
        assert status in expected,"POST "+path+" returned "+str(status)
        return data
def wait(fn,*,seconds=150,heartbeat=None):
    until=time.monotonic()+seconds;beat=0
    while time.monotonic()<until:
        if heartbeat and time.monotonic()-beat>8:heartbeat();beat=time.monotonic()
        result=fn()
        if result:return result
        time.sleep(.7)
    raise AssertionError("Timed out waiting for a confirmed web-facing outcome")
def exercise():
    web=Web()
    pages={"/":"discover","/checkout":"checkout","/orders":"orders","/account":"account",
        "/notifications":"notifications","/kitchen":"kitchen","/kitchen/menu":"menu-editor",
        "/courier":"courier","/operations":"operations","/operations/fleet":"fleet",
        "/operations/services":"services"}
    for path,page in pages.items():
        status,html=web.request("GET",path,html=True)
        assert status==200 and 'data-page="'+page+'"' in html,"Page routing mismatch"
    status,_=web.request("POST","/api/workspaces",{"operation_id":str(uuid.uuid4())},headers={"Origin":"https://untrusted.example"})
    assert status==400,"Cross-site write accepted"
    ctx=web.post("/api/workspaces");w=ctx["workspace"];rid=w["restaurant_id"]
    assert len(w["couriers"])==2 and w["status"]=="READY"
    fleet=web.get("/api/fleet");assert fleet["capacity"]["counts"]["OFFLINE"]==2
    profile=web.get("/api/profile")
    saved=web.post("/api/profile",{"name":"Web smoke diner","email":profile["customer"]["email"]})
    assert web.get("/api/profile")["customer"]["name"]=="Web smoke diner"
    menu=web.get("/api/kitchen/menu");bowl=next(i for i in menu["items"] if i["item_id"]=="paneer-bowl")
    paused={k:bowl[k] for k in ("name","description","price_minor","available")};paused["available"]=False
    web.post("/api/kitchen/menu",{"action":"put","version":menu["version"],"item_id":bowl["item_id"],"item":paused})
    status,_=web.request("POST","/api/cart",{"restaurant_id":rid,"item_id":bowl["item_id"],"quantity":1})
    assert status==409,"Unavailable dish added to cart"
    menu=web.get("/api/kitchen/menu");paused["available"]=True
    web.post("/api/kitchen/menu",{"action":"put","version":menu["version"],"item_id":bowl["item_id"],"item":paused})
    web.post("/api/cart",{"restaurant_id":rid,"item_id":"paneer-bowl","quantity":1})
    web.post("/api/cart/clear");assert not web.get("/api/cart")["items"]
    for item in ("paneer-bowl","lemon-tea"):web.post("/api/cart",{"restaurant_id":rid,"item_id":item,"quantity":1})
    cart=web.get("/api/cart");assert cart["preview_total_minor"]==33800
    body={"address_id":next(a["id"] for a in profile["addresses"] if a["is_default"]),"notification_scenario":"response_loss_once"}
    operation=str(uuid.uuid4());first=web.post("/api/checkout",body,op=operation)
    replay=web.post("/api/checkout",body,op=operation)
    assert first==replay and first["notification_subscription"]=="CONFIRMED"
    order=first["order"];oid=order["order_id"];path="/api/orders/"+oid
    assert order["total_minor"]==33800 and not web.get("/api/cart")["items"]
    print("Separate pages, CSRF, real profile/menu/cart and stable checkout replay: PASS",flush=True)
    def detail():return web.get(path)
    def order_state(state):
        value=detail();return value if value["order"]["status"]==state else None
    web.post(path+"/payment",{"action":"pay","expected_version":order["version"],"scenario":"decline_then_success","refund_scenario":"success"})
    failed=wait(lambda:order_state("PAYMENT_FAILED"))
    assert failed["payment"]["payment"]["state"]=="DECLINED"
    web.post(path+"/payment",{"action":"retry","expected_version":failed["order"]["version"]})
    wait(lambda:order_state("PAID"))
    def ticket(state):
        rows=web.get("/api/kitchen")["tickets"]
        return next((r for r in rows if r["order_id"]==oid and r["state"]==state and not any(c["state"]=="PENDING" for c in r["commands"])),None)
    current=wait(lambda:ticket("WAITING"))
    for action,state in (("accept","ACCEPTED"),("start_preparation","PREPARING"),("ready","READY")):
        web.post("/api/kitchen/"+oid+"/actions",{"action":action,"expected_version":current["order_version"]})
        current=wait(lambda:ticket(state))
    web.post("/api/kitchen/"+oid+"/dispatch",{"expected_version":current["order_version"]})
    def graph():return web.get("/api/operations/orders/"+oid)
    def waiting_for_courier():
        value=graph()
        node=next((n for n in (value["graph"] or {}).get("nodes",[]) if n["key"]=="assign"),None)
        return value if node and node["reason_code"]=="courier_unavailable" else None
    blocked=wait(waiting_for_courier)
    nodes={n["key"]:n for n in blocked["graph"]["nodes"]}
    assert nodes["pickup"]["prerequisites_complete"]==1 and len(nodes["pickup"]["prerequisites"])==2
    assert nodes["assign"]["attempt_count"]==0 and nodes["assign"]["state"]=="READY"
    print("Explicit payment decline/retry, kitchen stages and no-courier AND-join: PASS",flush=True)
    cid=ctx["courier_id"]
    def actor():return web.get("/api/courier")
    def heartbeat():web.post("/api/couriers/"+cid+"/heartbeat")
    try:
        courier=actor()["courier"]
        web.post("/api/couriers/"+cid+"/presence",{"expected_version":courier["version"],"online":True,"accepting_work":True})
        def assigned(state):
            value=actor()
            return value if value["delivery"] and value["delivery"]["order_id"]==oid and value["delivery"]["state"]==state and not any(c["state"]=="PENDING" for c in value["delivery"]["commands"]) else None
        current=wait(lambda:assigned("ASSIGNED"),heartbeat=heartbeat)
        aid=current["delivery"]["assignment"]["assignment_id"]
        for action,state in (("pickup","PICKED_UP"),("deliver","DELIVERED")):
            d=current["delivery"]
            payload={"action":action,"order_id":oid,"expected_version":d["order_version"],"assignment_id":aid}
            key=str(uuid.uuid4());response=web.post("/api/courier/actions",payload,op=key)
            # Successful domain submission is retained even if the command is still queued.
            assert response
            if state=="PICKED_UP":current=wait(lambda:assigned(state),heartbeat=heartbeat)
            else:wait(lambda:order_state("DELIVERED"),heartbeat=heartbeat)
        assert not actor()["courier"]["occupied"]
    finally:
        courier=actor()["courier"]
        web.post("/api/couriers/"+cid+"/presence",{"expected_version":courier["version"],"online":False,"accepting_work":False})
    metrics=web.get("/api/operations?hours=24")
    assert metrics["order_count"]==metrics["delivered_count"]==1
    assert metrics["preparation"]["sample_count"]==metrics["delivery"]["sample_count"]==1
    assert sum(b["count"] for b in metrics["arrivals"])==1
    assert all(key not in json.dumps(metrics) for key in ("delivery_snapshot","email","phone","lease_token"))
    events=detail()["history"]
    def notes():
        values=web.get("/api/notifications")["notifications"]
        return values if len(values)==len(events) and all(n["state"]=="SENT" for n in values) else None
    messages=wait(notes,seconds=300)
    assert all(n["attempt_count"]==1 and n["recipient_type"]=="customer" for n in messages)
    courier_notes=wait(lambda: value if len(value:=web.get("/api/notifications?recipient=courier")["notifications"])==3 and all(n["state"]=="SENT" for n in value) else None,seconds=300)
    assert all(n["recipient_type"]=="courier" for n in courier_notes)
    selected=messages[0];key=str(uuid.uuid4())
    read_body={"action":"read","expected_version":selected["version"],"recipient":"customer"}
    read=web.post("/api/notifications/"+selected["notification_id"]+"/actions",read_body,op=key)
    assert web.post("/api/notifications/"+selected["notification_id"]+"/actions",read_body,op=key)==read
    assert read["read_at"] and not read["unread"]
    def proven():
        value=graph();nodes={n["key"]:n for n in value["graph"]["nodes"]}
        return value if all(nodes[k]["state"]=="SUCCEEDED" and nodes[k]["completion_source"]=="notification_receipt" for k in ("notify_paid","notify_ready","notify_delivered")) else None
    proof=wait(proven)
    assert all(key not in json.dumps(proof) for key in ("lease_token","owner_session","delivery_snapshot","email","phone"))
    assert next(n for n in proof["graph"]["nodes"] if n["key"]=="pickup")["prerequisites_complete"]==2
    assert proof["metrics"]["preparation_seconds"]==metrics["durations"][0]["preparation_seconds"]
    assert proof["metrics"]["delivery_seconds"]==metrics["durations"][0]["delivery_seconds"]
    print("Exact courier handoffs, measured charts and receipt-confirmed recipient streams: PASS",flush=True)
    system=web.get("/api/system")
    assert all(s["status"]=="healthy" for s in system["services"]) and system["scheduler"]["leader_available"]
    print("Scoped graph proofs, versioned inbox replay and service readiness: PASS",flush=True)
    return {"web_order_id":oid}
def main():print(json.dumps({"result":"PASS",**exercise()}),flush=True)
if __name__=="__main__":main()
