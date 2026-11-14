from common.deployment import require_profile
from common.web_access import CloudAccess
"""Local multi-page browser layer; domain writes always use the owning service API."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import hmac
import json
import os
from pathlib import Path
import re
import uuid
from flask import Flask,g,jsonify,render_template,request,redirect
from werkzeug.exceptions import BadRequest,Conflict,HTTPException,NotFound,ServiceUnavailable
from web_ui.client import Client,ServiceError,SERVICES
from web_ui.state import State
from web_ui.demo import create_workspace
from web_ui.analytics import collect,history_metrics

UUID=re.compile(r"^[a-fA-F0-9-]{36}$")
IDENTIFIER=re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
PAGES={
 "/":("discover","Order something good","customer"),"/orders":("orders","Your orders","customer"),
 "/checkout":("checkout","Make it yours","customer"),"/account":("account","Your little corner","customer"),
 "/notifications":("notifications","The latest, for you","customer"),
 "/kitchen":("kitchen","A well-run kitchen","kitchen"),"/kitchen/menu":("menu-editor","On the menu","kitchen"),
 "/courier":("courier","Good things, on the move","courier"),
 "/operations":("operations","The bigger picture","operations"),
 "/operations/stream":("stream","Kitchen pulse","operations"),
 "/operations/fleet":("fleet","Room for the next order","operations"),
 "/operations/services":("services","Behind every good delivery","operations")}

def uid(value):
    try:return str(uuid.UUID(value))
    except (ValueError,TypeError,AttributeError):raise BadRequest("A valid record ID is required.") from None

def slug(value):
    if not isinstance(value,str) or not IDENTIFIER.fullmatch(value):raise BadRequest("A valid restaurant or menu ID is required.")
    return value

def only(body,allowed,required=()):
    if not isinstance(body,dict) or set(body)-set(allowed) or not set(required)<=set(body):
        raise BadRequest("Unsupported or missing fields.")
    return body

def redact(value):
    private={"lease_token","owner_session","worker_session","delivery_snapshot","address_id","email","phone"}
    if isinstance(value,dict):return {k:redact(v) for k,v in value.items() if k not in private}
    if isinstance(value,list):return [redact(v) for v in value]
    return value

def create_app(config=None,*,client=None):
    app=Flask(__name__)
    app.config.update(APP_ENV=os.environ.get("APP_ENV","production"),MAX_CONTENT_LENGTH=32768,
        STATE_PATH=os.environ.get("WEB_UI_DATA",str(Path(".local-build")/"web-ui.sqlite3")))
    if config:app.config.update(config)
    if not app.testing:require_profile(app.config["APP_ENV"])
    cloud_access=CloudAccess() if app.config["APP_ENV"]=="cloud" else None
    if cloud_access:
        from werkzeug.middleware.proxy_fix import ProxyFix
        app.wsgi_app=ProxyFix(app.wsgi_app,x_proto=1)
    state=State(app.config["STATE_PATH"]);api=client or Client()
    app.extensions.update(web_state=state,web_client=api)

    @app.before_request
    def session():
        if cloud_access:
            denied=cloud_access.authorize()
            if denied is not None:return denied
        if request.path=="/health":return
        if not cloud_access and request.host.split(":")[0].lower() not in {"localhost","127.0.0.1","web-ui"}:
            raise BadRequest("Use the local interface address.")
        if request.endpoint=="static" or request.path=="/health":return
        row,g.new_session=state.session(request.cookies.get("supper_session"))
        g.sid,g.csrf=row["id"],row["csrf"];g.context=json.loads(row["data"])
        if request.method not in {"GET","HEAD","OPTIONS"}:
            supplied=request.headers.get("X-CSRF-Token","")
            if not hmac.compare_digest(supplied,g.csrf):raise BadRequest("Reload this page before submitting the form.")
            if request.headers.get("Sec-Fetch-Site")=="cross-site":raise BadRequest("Cross-site actions are not permitted.")
            origin=request.headers.get("Origin")
            if origin and origin!=request.host_url.rstrip("/"):raise BadRequest("The form must come from this local interface.")

    @app.after_request
    def headers(response):
        response.headers.update({"Cache-Control":"no-store","X-Content-Type-Options":"nosniff","Referrer-Policy":"same-origin",
            "Content-Security-Policy":"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
            "Permissions-Policy":"camera=(), microphone=(), geolocation=()"})
        if getattr(g,"new_session",False):
            response.set_cookie("supper_session",g.sid,httponly=True,samesite="Strict",max_age=30*86400,secure=bool(cloud_access))
        return response

    @app.errorhandler(ServiceError)
    def service_error(error):
        return jsonify({"error":error.message,"uncertain":error.status>=500}),error.status

    @app.errorhandler(HTTPException)
    def http_error(error):
        if request.path.startswith("/api/"):return jsonify({"error":error.description}),error.code
        return render_template("error.html",title="A little pause",message=error.description,csrf=getattr(g,"csrf","")),error.code

    @app.errorhandler(Exception)
    def unexpected(error):
        app.logger.error("Web interface operation could not be confirmed.")
        return jsonify({"error":"This operation could not be confirmed. Refresh the saved records before trying again.","uncertain":True}),503

    def workspace(required=True):
        value=state.workspace(g.context.get("workspace_id"))
        if required and (not value or value.get("status")!="READY"):raise Conflict("Start or select a completed demo workspace first.")
        return value

    def public_context():
        value=workspace(False)
        public={k:value[k] for k in ("id","name","customer_id","restaurant_id","couriers","zone","run_id","status","created_at") if k in value} if value else None
        return {"workspace":public,"cart":g.context.get("cart",{}),
            "kitchen_restaurant_id":g.context.get("kitchen_restaurant_id") or (value or {}).get("restaurant_id"),
            "courier_id":g.context.get("courier_id") or next(iter((value or {}).get("couriers",[])),None),
            "pending_checkout":state.pending(g.sid,"checkout"),"simulation":True}

    def save_context():
        state.save(g.sid,g.context)

    def scope_order(value,*,kind="customer"):
        value=uid(value);w=workspace()
        row=api.call("order","GET","/orders/"+value)["order"]
        if kind=="customer":
            permitted=row["customer_id"]==w["customer_id"]
        elif kind=="kitchen":
            permitted=row["restaurant_id"]==public_context()["kitchen_restaurant_id"]
        else:permitted=value in w.get("orders",[]) and row["customer_id"]==w["customer_id"]
        if not permitted:raise NotFound("This order is outside the selected workspace.")
        return row

    def courier_id(value=None):
        w=workspace();value=uid(value or public_context()["courier_id"])
        if value not in w["couriers"]:raise NotFound("Courier is outside this demo fleet.")
        return value

    def execute(kind,body,service,method,path,payload,*,idempotent=True,version=None):
        operation_id=uid(body.get("operation_id"))
        spec={"kind":kind,"service":service,"method":method,"path":path,"body":payload,"version":version}
        prior=state.begin(g.sid,operation_id,spec)
        if prior["response"] is not None:return prior["response"]
        if not idempotent and not prior.get("is_new",False):
            raise ServiceError(503,"This submission is unconfirmed. Refresh the saved records before making another request.")
        try:
            result=api.call(service,method,path,payload,key="web:"+operation_id if idempotent else None,version=version)
        except ServiceError as error:
            if error.status<500:state.reject(g.sid,operation_id)
            raise
        state.finish(g.sid,operation_id,result)
        return result

    @app.get("/health")
    def health():return jsonify({"service":"web-ui","status":"healthy","simulation":True})

    def page(path):
        name,title,role=PAGES[path]
        if path=="/notifications" and request.args.get("recipient")=="courier":role="courier"
        return render_template("page.html",page=name,title=title,role=role,csrf=g.csrf,record_id="")
    for path in PAGES:
        app.add_url_rule(path,"page_"+PAGES[path][0],lambda path=path:page(path))

    @app.get("/restaurants/<value>")
    def menu_page(value):
        return render_template("page.html",page="menu",title="Something worth waiting for",role="customer",csrf=g.csrf,record_id=slug(value))

    @app.get("/orders/<value>")
    def tracking_page(value):
        return render_template("page.html",page="tracking",title="From kitchen to doorstep",role="customer",csrf=g.csrf,record_id=uid(value))

    @app.get("/operations/orders/<value>")
    def graph_page(value):
        return render_template("page.html",page="execution",title="Every step has a reason",role="operations",csrf=g.csrf,record_id=uid(value))

    @app.get("/api/context")
    def context_read():return jsonify(public_context())

    @app.get("/api/workspaces")
    def workspaces():
        return jsonify({"workspaces":[{k:row[k] for k in ("id","name","status","created_at")} for row in state.list_workspaces()]})

    @app.post("/api/workspaces")
    def workspace_create():
        body=only(request.get_json(),{"operation_id"},{"operation_id"})
        if state.pending(g.sid,"checkout"):raise Conflict("Resolve the pending checkout before creating a workspace.")
        op=uid(body["operation_id"]);prior=state.begin(g.sid,op,{"kind":"workspace"})
        if prior["response"] is not None:value=state.workspace(prior["response"]["id"])
        elif not prior.get("is_new",False):raise Conflict("Workspace creation is unconfirmed. Refresh the workspace list before creating another.")
        else:
            value=create_workspace(api,state);state.finish(g.sid,op,{"id":value["id"]})
        g.context={"workspace_id":value["id"],"cart":{}}
        g.context=state.patch_context(g.sid,g.context,replace=True)
        return jsonify(public_context()),201

    @app.post("/api/context")
    def context_select():
        body=only(request.get_json(),{"operation_id","workspace_id","courier_id","restaurant_id"})
        if state.pending(g.sid,"checkout"):raise Conflict("Resolve the pending checkout before changing workspaces.")
        if "workspace_id" in body:
            value=state.workspace(uid(body["workspace_id"]))
            if not value or value["status"]!="READY":raise NotFound("Completed workspace not found.")
            g.context={"workspace_id":value["id"],"cart":{}}
        if "courier_id" in body:g.context["courier_id"]=courier_id(body["courier_id"])
        if "restaurant_id" in body:
            value=slug(body["restaurant_id"]);api.call("restaurant","GET","/restaurants/"+value)
            g.context["kitchen_restaurant_id"]=value
        fields=g.context if "workspace_id" in body else {k:g.context[k] for k in ("courier_id","kitchen_restaurant_id") if k in g.context}
        g.context=state.patch_context(g.sid,fields,replace="workspace_id" in body)
        return jsonify(public_context())

    @app.get("/api/restaurants")
    def restaurants():
        query={"limit":20,"after":request.args.get("after")}
        return jsonify(api.call("restaurant","GET","/restaurants",query=query))

    @app.get("/api/restaurants/<value>/menu")
    def restaurant_menu(value):return jsonify(api.call("restaurant","GET","/restaurants/"+slug(value)+"/menu"))

    @app.get("/api/cart")
    def cart_read():
        cart=g.context.get("cart",{});menu=None;items=[];total=0
        if cart.get("restaurant_id"):
            menu=api.call("restaurant","GET","/restaurants/"+slug(cart["restaurant_id"])+"/menu")
            lookup={row["item_id"]:row for row in menu["items"]}
            for item_id,quantity in cart.get("items",{}).items():
                item=lookup.get(item_id)
                if item:
                    items.append({**item,"quantity":quantity,"line_total_minor":item["price_minor"]*quantity})
                    total+=item["price_minor"]*quantity
                else:items.append({"item_id":item_id,"name":"Unavailable item","quantity":quantity,"available":False,"price_minor":0,"line_total_minor":0})
        return jsonify({"restaurant":{k:menu[k] for k in ("restaurant_id","name","is_open","version")} if menu else None,
            "items":items,"preview_total_minor":total,"currency":"INR","pending_checkout":state.pending(g.sid,"checkout")})

    @app.post("/api/cart")
    def cart_write():
        workspace()
        if state.pending(g.sid,"checkout"):raise Conflict("Resolve the pending checkout before changing the cart.")
        body=only(request.get_json(),{"operation_id","restaurant_id","item_id","quantity"},{"restaurant_id","item_id","quantity"})
        rid,item=slug(body["restaurant_id"]),slug(body["item_id"]);quantity=body["quantity"]
        if type(quantity) is not int or not 0<=quantity<=20:raise BadRequest("Quantity must be between zero and twenty.")
        menu=api.call("restaurant","GET","/restaurants/"+rid+"/menu")
        entry=next((row for row in menu["items"] if row["item_id"]==item),None)
        if quantity and (not menu["is_open"] or not entry or not entry["available"]):raise Conflict("This item is currently unavailable.")
        g.context=state.cart(g.sid,g.context["workspace_id"],rid,item,quantity)
        return jsonify({"cart":g.context["cart"]})

    @app.post("/api/cart/clear")
    def clear_cart():
        w=workspace();only(request.get_json(),{"operation_id"})
        g.context=state.clear_cart(g.sid,w["id"])
        return jsonify({"cart":g.context["cart"]})

    @app.post("/api/checkout")
    def checkout():
        w=workspace();body=only(request.get_json(),{"operation_id","address_id","notification_scenario"},{"operation_id","address_id"})
        operation_id=uid(body["operation_id"])
        scenario=body.get("notification_scenario","success")
        if scenario not in {"success","fail_once","always_fail","response_loss_once"}:
            raise BadRequest("Choose a supported notification simulation.")
        prior=state.checkout(g.sid,operation_id,w,body["address_id"],scenario)
        if prior["response"] is not None:
            result=prior["response"]
        else:
            try:
                result=api.call("order","POST","/orders",prior["data"]["body"],key="web:"+operation_id)
            except ServiceError as error:
                if error.status<500:state.reject(g.sid,operation_id)
                raise
            g.context=state.finish_checkout(g.sid,operation_id,result,w["id"])

        try:
            api.call("notification","POST","/notifications/orders/"+result["order_id"]+"/subscription",
                {"customer_id":w["customer_id"],"scenario":scenario,"courier_updates":True},
                key="subscription:"+result["order_id"])
            subscription="CONFIRMED"
        except ServiceError:subscription="UNCONFIRMED"
        return jsonify({"order":result,"notification_subscription":subscription}),201

    @app.get("/api/profile")
    def profile():
        w=workspace()
        return jsonify({**api.call("customer","GET",f"/customers/{w['customer_id']}"),
            **api.call("customer","GET",f"/customers/{w['customer_id']}/addresses")})

    @app.post("/api/profile")
    def profile_edit():
        w=workspace();body=only(request.get_json(),{"operation_id","name","email"},{"operation_id","name","email"})
        return jsonify(execute("profile",body,"customer","PATCH",f"/customers/{w['customer_id']}",
            {k:body[k] for k in ("name","email")},idempotent=False))

    @app.post("/api/addresses")
    def addresses():
        w=workspace();body=request.get_json()
        only(body,{"operation_id","action","address_id","address"},{"operation_id","action"})
        action=body["action"];path=f"/customers/{w['customer_id']}/addresses"
        if action not in {"add","edit","delete","default"}:raise BadRequest("Unknown address action.")
        if action!="add":
            value=body.get("address_id")
            if type(value) is not int or value<1:raise BadRequest("A saved address is required.")
            path+="/"+str(value)
        method="POST" if action=="add" else ("DELETE" if action=="delete" else "PATCH")
        payload=None if action=="delete" else ({"is_default":True} if action=="default" else body.get("address"))
        result=execute("address:"+action,body,"customer",method,path,payload,idempotent=False)
        return jsonify(result)

    @app.get("/api/orders")
    def orders():
        w=workspace()
        return jsonify(api.call("order","GET","/orders",query={"customer_id":w["customer_id"],"limit":50,"after":request.args.get("after")}))

    @app.get("/api/orders/<value>")
    def order_detail(value):
        row=scope_order(value)
        services=[("payment","/payments/orders/"+row["order_id"]),("kitchen","/kitchen/orders/"+row["order_id"]),
                  ("delivery","/delivery/orders/"+row["order_id"])]
        result={"order":row,"history":api.call("order","GET","/orders/"+row["order_id"]+"/events")["events"]}
        for name,path in services:
            try:result[name]=api.optional(name,path)
            except ServiceError:result[name]={"unavailable":True}
        try:result["notifications"]=api.optional("notification","/notifications",query={"customer_id":row["customer_id"],"order_id":row["order_id"],"limit":50})
        except ServiceError:result["notifications"]={"unavailable":True}
        return jsonify(result)

    @app.post("/api/orders/<value>/subscription")
    def subscription_retry(value):
        row=scope_order(value);body=only(request.get_json(),{"operation_id"},{"operation_id"})
        return jsonify(execute("subscription",body,"notification","POST","/notifications/orders/"+row["order_id"]+"/subscription",
            {"customer_id":row["customer_id"],"scenario":"success","courier_updates":True}))

    @app.post("/api/orders/<value>/payment")
    def payment_action(value):
        row=scope_order(value);body=only(request.get_json(),{"operation_id","action","expected_version","scenario","refund_scenario"},
            {"operation_id","action","expected_version"})
        action=body["action"]
        if action not in {"pay","retry","cancel"}:raise BadRequest("Unknown payment action.")
        payload={"expected_version":body["expected_version"]}
        if action=="pay":payload.update(scenario=body.get("scenario","success"),refund_scenario=body.get("refund_scenario","success"))
        path="/payments/orders/"+row["order_id"]+("/"+action if action!="pay" else "")
        return jsonify(execute("payment:"+action,body,"payment","POST",path,payload)),202

    @app.get("/api/kitchen")
    def kitchen():
        workspace();rid=public_context()["kitchen_restaurant_id"]
        result=api.call("kitchen","GET","/kitchen/restaurants/"+slug(rid)+"/orders",query={"limit":50,"after":request.args.get("after")})
        def detail(row):
            try:return api.call("kitchen","GET","/kitchen/orders/"+row["order_id"])["ticket"]
            except ServiceError:return {**row,"details_unavailable":True,"items":[],"commands":[]}
        with ThreadPoolExecutor(max_workers=6) as pool:result["tickets"]=list(pool.map(detail,result["tickets"]))
        return jsonify(result)

    @app.post("/api/kitchen/<value>/actions")
    def kitchen_action(value):
        row=scope_order(value,kind="kitchen")
        body=only(request.get_json(),{"operation_id","action","expected_version","reason"},{"operation_id","action","expected_version"})
        payload={k:body[k] for k in ("action","expected_version","reason") if k in body}
        return jsonify(execute("kitchen:"+body["action"],body,"kitchen","POST","/kitchen/orders/"+row["order_id"]+"/actions",payload)),202

    @app.get("/api/kitchen/menu")
    def kitchen_menu():
        workspace();return jsonify(api.call("restaurant","GET","/restaurants/"+slug(public_context()["kitchen_restaurant_id"])+"/menu"))

    @app.post("/api/kitchen/menu")
    def kitchen_menu_edit():
        workspace();rid=slug(public_context()["kitchen_restaurant_id"]);body=request.get_json()
        only(body,{"operation_id","action","version","item_id","item","profile"},{"operation_id","action","version"})
        if body["action"]=="profile":method,path,payload="PATCH","/restaurants/"+rid,body.get("profile")
        elif body["action"] in {"put","delete"}:
            method="PUT" if body["action"]=="put" else "DELETE"
            path="/restaurants/"+rid+"/menu/"+slug(body.get("item_id"))
            payload=body.get("item") if method=="PUT" else None
        else:raise BadRequest("Unknown menu action.")
        return jsonify(execute("menu:"+body["action"],body,"restaurant",method,path,payload,idempotent=False,version=body["version"]))

    @app.post("/api/kitchen/<value>/dispatch")
    def dispatch(value):
        row=scope_order(value,kind="kitchen");w=workspace()
        body=only(request.get_json(),{"operation_id","expected_version"},{"operation_id","expected_version"})
        return jsonify(execute("dispatch",body,"delivery","POST","/delivery/orders/"+row["order_id"],
            {"expected_version":body["expected_version"],"zone":w["zone"]})),202

    @app.get("/api/fleet")
    def fleet():
        w=workspace()
        return jsonify({"capacity":api.call("delivery","GET","/delivery/capacity",query={"zone":w["zone"]}),
            **api.call("delivery","GET","/couriers",query={"zone":w["zone"],"limit":50}),
            **api.call("delivery","GET","/delivery/orders",query={"zone":w["zone"],"limit":50})})

    @app.get("/api/courier")
    def courier():
        value=courier_id();courier=api.call("delivery","GET","/couriers/"+value)["courier"]
        delivery=order=None
        if courier["active_order_id"]:
            delivery=api.call("delivery","GET","/delivery/orders/"+courier["active_order_id"])["delivery"]
            if delivery["assignment"]["courier_id"]!=value:raise Conflict("Assignment changed. Refresh before continuing.")
            full=api.call("order","GET","/orders/"+courier["active_order_id"])["order"]
            order={k:full[k] for k in ("order_id","restaurant_snapshot","items","delivery_snapshot")}
        return jsonify({"courier":courier,"delivery":delivery,"order":order})

    @app.post("/api/couriers/<value>/presence")
    def presence(value):
        value=courier_id(value);body=only(request.get_json(),{"operation_id","expected_version","online","accepting_work"},
            {"operation_id","expected_version","online","accepting_work"})
        return jsonify(execute("presence",body,"delivery","POST","/couriers/"+value+"/presence",
            {k:body[k] for k in ("expected_version","online","accepting_work")}))

    @app.post("/api/couriers/<value>/heartbeat")
    def heartbeat(value):
        value=courier_id(value);body=only(request.get_json(),{"operation_id"},{"operation_id"})
        return jsonify(execute("heartbeat",body,"delivery","POST","/couriers/"+value+"/heartbeat",{}))

    @app.post("/api/courier/actions")
    def courier_action():
        value=courier_id();body=only(request.get_json(),{"operation_id","order_id","action","expected_version","assignment_id"},
            {"operation_id","order_id","action","expected_version","assignment_id"})
        current=api.call("delivery","GET","/couriers/"+value)["courier"]
        if current["active_order_id"]!=body["order_id"] or current["active_assignment_id"]!=body["assignment_id"]:
            raise Conflict("This courier no longer owns that assignment.")
        return jsonify(execute("handoff:"+body["action"],body,"delivery","POST","/delivery/orders/"+uid(body["order_id"])+"/actions",
            {"action":body["action"],"expected_version":body["expected_version"],"courier_id":value,"assignment_id":uid(body["assignment_id"])})),202

    def notification_recipient():
        w=workspace()
        if request.args.get("recipient")=="courier":return "courier",courier_id()
        return "customer",str(w["customer_id"])

    @app.get("/api/notifications")
    def notifications():
        kind,value=notification_recipient()
        return jsonify(api.call("notification","GET","/notifications",query={kind+"_id":value,"limit":50,"after":request.args.get("after")}))

    @app.post("/api/notifications/<value>/actions")
    def notification_action(value):
        w=workspace();body=only(request.get_json(),{"operation_id","action","expected_version","recipient","scenario"},
            {"operation_id","action","expected_version"})
        kind,recipient=("courier",courier_id()) if body.get("recipient")=="courier" else ("customer",w["customer_id"])
        if body["action"] not in {"read","retry"}:raise BadRequest("Unknown notification action.")
        payload={"recipient_type":kind,"recipient_id":recipient,"expected_version":body["expected_version"]}
        if body["action"]=="retry":payload["scenario"]=body.get("scenario","success")
        return jsonify(execute("notification:"+body["action"],body,"notification","POST","/notifications/"+uid(value)+"/"+body["action"],payload))

    def workspace_orders():
        w=workspace();selected=[];after=None
        for _ in range(4):
            result=api.call("order","GET","/orders",query={"customer_id":w["customer_id"],"limit":50,"after":after})
            selected.extend(row for row in result["orders"] if row["order_id"] in w.get("orders",[]))
            after=result["next_after"]
            if not after:break
        return selected,bool(after)

    @app.get("/api/operations/stream")
    def stream_results():
        w=workspace()
        if request.args:raise BadRequest("The stream view uses the selected demo kitchen.")
        return jsonify(api.call("analytics","GET","/analytics/windows",query={"restaurant_id":w["restaurant_id"],"limit":60}))

    @app.get("/api/operations")
    def operations():
        hours=request.args.get("hours","24")
        if hours not in {"6","12","24","48"}:raise BadRequest("Choose a supported chart time window.")
        rows,truncated=workspace_orders();result=collect(api,rows,hours=int(hours));result["truncated"]=truncated
        return jsonify(result)

    @app.get("/api/operations/orders/<value>")
    def execution(value):
        row=scope_order(value,kind="operations");oid=row["order_id"]
        histories=api.call("order","GET","/orders/"+oid+"/events")["events"]
        workflows=api.call("task","GET","/workflows",query={"order_id":oid})["workflows"]
        managed=next((item for item in workflows if item["managed"]),None)
        graph=api.call("task","GET","/workflows/"+managed["workflow_id"]+"/graph") if managed else None
        notes=api.call("notification","GET","/notifications",query={"customer_id":row["customer_id"],"order_id":oid,"limit":50})
        return jsonify(redact({"order":row,"history":histories,"metrics":history_metrics(histories),
            "graph":graph,"notifications":notes["notifications"],"workflows":workflows,"observed_at":datetime.now(timezone.utc).isoformat()}))

    @app.get("/api/system")
    def system():
        def probe(pair):
            name,base=pair
            try:
                data=api.call(name,"GET","/health")
                return {"name":name,"status":"healthy" if data.get("status")=="healthy" else "unavailable","zookeeper_connected":data.get("zookeeper_connected"),
                    "leader_available":data.get("leader_available")}
            except Exception:return {"name":name,"status":"unavailable"}
        with ThreadPoolExecutor(max_workers=8) as pool:services=list(pool.map(probe,SERVICES.items()))
        try:
            workers=api.call("task","GET","/workers")["workers"];scheduler=api.call("task","GET","/scheduler")
        except ServiceError:workers=[];scheduler={"leader_available":False,"zookeeper_connected":False}
        return jsonify(redact({"services":services,"workers":workers,"scheduler":scheduler,"observed_at":datetime.now(timezone.utc).isoformat()}))

    return app

def main():
    create_app().run(host="0.0.0.0",port=3000,debug=False,use_reloader=False,threaded=True)

if __name__=="__main__":main()
