"""Local web scoping, immutable checkout replay, privacy, CSRF and measured analytics."""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import re
import tempfile
import unittest
import uuid
from web_ui.app import create_app
from web_ui.client import Client,ServiceError
from web_ui.analytics import aggregate,history_metrics

class Backend:
    def __init__(self):
        self.calls=[];self.orders={};self.keys={};self.lost=False;self.patch_lost=False
        self.menu={"restaurant_id":"paper","name":"Paper kitchen","currency":"INR","is_open":True,"version":1,
            "estimated_prep_time_mins":12,"items":[{"item_id":"bowl","name":"Demo bowl","description":"Fictional","price_minor":12999,"available":True}]}
    def call(self,service,method,path,body=None,*,key=None,version=None,query=None):
        self.calls.append((service,method,path,body,key,version,query))
        if path=="/health":return {"status":"healthy"}
        if service=="restaurant":
            if path.endswith("/menu"):return self.menu
            if path=="/restaurants":return {"restaurants":[self.menu],"next_page_token":None}
            return {"restaurant":self.menu}
        if service=="customer":
            if method=="PATCH":
                if self.patch_lost:raise ServiceError(503,"Unconfirmed synthetic edit.")
                return {"customer":{"id":7,**body}}
            if path.endswith("/addresses"):return {"addresses":[{"id":9,"customer_id":7,"label":"Demo","line1":"Synthetic private address","city":"Demo City","country_code":"IN","postal_code":"000000","is_default":True}]}
            return {"customer":{"id":7,"name":"Demo","email":"private@example.test","phone":"private-contact"}}
        if service=="order":
            if method=="POST":
                if key in self.keys:return self.keys[key]
                value=str(uuid.uuid4());now=datetime.now(timezone.utc).isoformat()
                result={"order_id":value,"customer_id":body["customer_id"],"restaurant_id":body["restaurant_id"],"version":1,
                    "status":"PENDING_PAYMENT","total_minor":12999,"currency":"INR","created_at":now,"updated_at":now}
                self.keys[key]=result
                self.orders[value]={**result,"address_id":9,"delivery_snapshot":{"line1":"Synthetic private address","phone":"private-contact"},
                    "restaurant_snapshot":{"name":"Paper kitchen","estimated_prep_time_mins":12},"items":[]}
                if self.lost:self.lost=False;raise ServiceError(503,"Lost synthetic response.")
                return result
            if path=="/orders":return {"orders":[{k:v for k,v in row.items() if k not in {"delivery_snapshot","items","address_id","restaurant_snapshot"}} for row in self.orders.values()],"next_after":None}
            parts=path.split("/");row=self.orders.get(parts[2])
            if not row:raise ServiceError(404,"Missing order.")
            if path.endswith("/events"):return {"events":[{"event_id":str(uuid.uuid4()),"to_state":"PENDING_PAYMENT","occurred_at":row["created_at"]}]}
            return {"order":row}
        if service=="notification":
            if method=="GET":return {"notifications":[],"recipient_unread_count":0,"next_after":None}
            return {"simulation":True}
        if service in {"payment","kitchen","delivery"}:
            if method=="GET":raise ServiceError(404,"Not materialized.")
            return {"accepted":True}
        if service=="task":
            if path=="/workflows":return {"workflows":[]}
            if path=="/workers":return {"workers":[]}
            if path=="/scheduler":return {"leader_available":False,"zookeeper_connected":False}
        raise ServiceError(404,"Unknown synthetic route.")
    def optional(self,service,path,*,query=None):
        try:return self.call(service,"GET",path,query=query)
        except ServiceError as error:
            if error.status==404:return None
            raise

class WebTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.backend=Backend()
        self.path=str(Path(self.directory.name)/"web.sqlite3")
        self.app=create_app({"TESTING":True,"APP_ENV":"local","STATE_PATH":self.path},client=self.backend)
        self.client=self.app.test_client()
        html=self.client.get("/").get_data(as_text=True)
        self.csrf=re.search(r'name="csrf-token" content="([^"]+)"',html)[1]
        self.state=self.app.extensions["web_state"]
        self.w={"id":str(uuid.uuid4()),"name":"Synthetic workspace","status":"READY","customer_id":7,
            "restaurant_id":"paper","zone":"zone","run_id":str(uuid.uuid4()),"couriers":[str(uuid.uuid4())],
            "created_at":datetime.now(timezone.utc).isoformat(),"orders":[]}
        self.state.save_workspace(self.w)
        self.post("/api/context",{"workspace_id":self.w["id"]})
    def tearDown(self):self.directory.cleanup()
    def post(self,path,body):
        return self.client.post(path,json=body,headers={"X-CSRF-Token":self.csrf})
    def cart(self,item="bowl",qty=1):
        return self.post("/api/cart",{"restaurant_id":"paper","item_id":item,"quantity":qty})
    def checkout(self,op=None,scenario="success"):
        return self.post("/api/checkout",{"operation_id":op or str(uuid.uuid4()),"address_id":9,"notification_scenario":scenario})
    def add_order(self):
        self.cart();response=self.checkout()
        self.assertEqual(response.status_code,201,response.get_data(as_text=True))
        return response.get_json()["order"]["order_id"]

    def test_distinct_pages_have_their_own_content_and_shared_navigation(self):
        pages={"/":"discover","/checkout":"checkout","/account":"account","/kitchen":"kitchen",
            "/kitchen/menu":"menu-editor","/courier":"courier","/operations":"operations",
            "/operations/fleet":"fleet","/operations/services":"services","/notifications":"notifications"}
        for path,page in pages.items():
            with self.subTest(path=path):
                response=self.client.get(path)
                self.assertEqual(response.status_code,200)
                self.assertIn('data-page="'+page+'"',response.get_data(as_text=True))
                self.assertIn("Content-Security-Policy",response.headers)
        home=self.client.get("/").get_data(as_text=True)
        self.assertNotIn('id="task-graph"',home)
        self.assertNotIn('id="kitchen-board"',home)

    def test_session_cookie_is_opaque_httponly_and_same_site(self):
        response=self.app.test_client().get("/")
        cookie=response.headers["Set-Cookie"]
        self.assertIn("HttpOnly",cookie);self.assertIn("SameSite=Strict",cookie)
        self.assertNotIn("private@example.test",cookie);self.assertNotIn("customer_id",cookie)

    def test_csrf_and_cross_site_mutations_rejected_before_backend_work(self):
        before=len(self.backend.calls)
        self.assertEqual(self.client.post("/api/cart",json={}).status_code,400)
        self.assertEqual(self.client.post("/api/cart",json={},headers={"X-CSRF-Token":self.csrf,"Origin":"https://invalid.example"}).status_code,400)
        self.assertEqual(len(self.backend.calls),before)

    def test_arbitrary_proxy_and_private_worker_routes_absent(self):
        for path in ("/api/proxy?url=https://example.test","/internal/tasks/"+str(uuid.uuid4())+"/process","/api/tasks/complete"):
            self.assertEqual(self.client.get(path).status_code,404)

    def test_host_rebinding_is_rejected(self):
        self.assertEqual(self.client.get("/api/context",headers={"Host":"attacker.example"}).status_code,400)

    def test_cart_uses_integer_menu_prices_and_cannot_mix_kitchens(self):
        self.assertEqual(self.cart(qty=2).status_code,200)
        result=self.client.get("/api/cart").get_json()
        self.assertEqual(result["preview_total_minor"],25998)
        self.assertEqual(self.post("/api/cart",{"restaurant_id":"other","item_id":"bowl","quantity":1}).status_code,409)

    def test_unavailable_items_and_invalid_quantities_rejected(self):
        for qty in (-1,21,True,1.5):
            self.assertEqual(self.cart(qty=qty).status_code,400)
        self.backend.menu["items"][0]["available"]=False
        self.assertEqual(self.cart().status_code,409)

    def test_checkout_replay_after_cart_clear_returns_same_order(self):
        self.cart();op=str(uuid.uuid4())
        first=self.checkout(op);second=self.checkout(op)
        self.assertEqual(first.status_code,201);self.assertEqual(second.status_code,201)
        self.assertEqual(first.get_json()["order"],second.get_json()["order"])
        self.assertEqual(len(self.backend.orders),1)
        self.assertEqual(self.client.get("/api/cart").get_json()["items"],[])

    def test_lost_checkout_response_retains_body_and_recovers_same_key(self):
        self.cart();op=str(uuid.uuid4());self.backend.lost=True
        self.assertEqual(self.checkout(op).status_code,503)
        pending=self.client.get("/api/cart").get_json()["pending_checkout"]
        self.assertEqual(pending["operation_id"],op)
        self.assertEqual(self.cart(qty=2).status_code,409)
        self.assertEqual(self.checkout(str(uuid.uuid4())).status_code,409)
        second=self.checkout(op)
        self.assertEqual(second.status_code,201);self.assertEqual(len(self.backend.orders),1)
        calls=[c for c in self.backend.calls if c[0]=="order" and c[1]=="POST"]
        self.assertEqual(calls[0][3:5],calls[1][3:5])

    def test_pending_checkout_survives_web_app_recreation(self):
        self.cart();op=str(uuid.uuid4());self.backend.lost=True;self.checkout(op)
        recreated=create_app({"TESTING":True,"APP_ENV":"local","STATE_PATH":self.path},client=self.backend)
        self.assertIsNotNone(recreated.extensions["web_state"].operation(self.client.get_cookie("supper_session").value,op))

    def test_pending_checkout_blocks_workspace_switch_and_new_setup(self):
        self.cart();self.backend.lost=True;self.checkout()
        self.assertEqual(self.post("/api/context",{"workspace_id":self.w["id"]}).status_code,409)
        self.assertEqual(self.post("/api/workspaces",{"operation_id":str(uuid.uuid4())}).status_code,409)

    def test_checkout_rejects_client_total_and_changed_scenario(self):
        self.cart();op=str(uuid.uuid4())
        self.assertEqual(self.post("/api/checkout",{"operation_id":op,"address_id":9,"total_minor":1}).status_code,400)
        self.assertEqual(self.checkout(op).status_code,201)
        self.assertEqual(self.checkout(op,"always_fail").status_code,409)

    def test_customer_order_scope_cannot_read_another_customer(self):
        oid=self.add_order();self.backend.orders[oid]["customer_id"]=8
        self.assertEqual(self.client.get("/api/orders/"+oid).status_code,404)

    def test_operations_require_both_workspace_membership_and_customer(self):
        oid=self.add_order();self.backend.orders[oid]["customer_id"]=8
        self.assertEqual(self.client.get("/api/operations/orders/"+oid).status_code,404)

    def test_analytics_and_graph_responses_remove_contact_and_delivery_data(self):
        oid=self.add_order()
        for path in ("/api/operations","/api/operations/orders/"+oid):
            response=self.client.get(path)
            self.assertEqual(response.status_code,200)
            raw=response.get_data(as_text=True)
            for private in ("Synthetic private address","private-contact","private@example.test","lease_token","delivery_snapshot"):
                self.assertNotIn(private,raw)

    def test_customer_profile_keeps_necessary_details_in_its_own_page(self):
        response=self.client.get("/api/profile")
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.get_json()["customer"]["email"],"private@example.test")

    def test_non_idempotent_edits_are_not_blindly_retried_after_unknown_response(self):
        self.backend.patch_lost=True
        body={"operation_id":str(uuid.uuid4()),"name":"Synthetic","email":"synthetic@example.test"}
        self.assertEqual(self.post("/api/profile",body).status_code,503)
        self.assertEqual(self.post("/api/profile",body).status_code,503)
        self.assertEqual(sum(c[0]=="customer" and c[1]=="PATCH" for c in self.backend.calls),1)

    def test_menu_mutations_preserve_if_match_version(self):
        response=self.post("/api/kitchen/menu",{"operation_id":str(uuid.uuid4()),"action":"profile","version":4,
            "profile":{"is_open":False}})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.backend.calls[-1][5],4)

    def test_courier_presence_cannot_target_another_fleet(self):
        response=self.post("/api/couriers/"+str(uuid.uuid4())+"/presence",
            {"operation_id":str(uuid.uuid4()),"expected_version":1,"online":True,"accepting_work":True})
        self.assertEqual(response.status_code,404)

    def test_service_errors_and_unknown_errors_do_not_leak_exception_details(self):
        original=self.backend.call
        self.backend.call=lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError("synthetic-private-connection"))
        response=self.client.get("/api/profile")
        self.assertEqual(response.status_code,503)
        self.assertNotIn("synthetic-private-connection",response.get_data(as_text=True))
        self.backend.call=original

    def test_service_health_is_not_order_completion(self):
        response=self.client.get("/api/system").get_json()
        self.assertEqual(len(response["services"]),8)
        self.assertNotIn("orders_completed",response)

    def test_notifications_courier_page_uses_courier_navigation(self):
        html=self.client.get("/notifications?recipient=courier").get_data(as_text=True)
        self.assertIn('data-role="courier"',html)

    def test_clear_cart_is_repeatable_and_retains_saved_orders(self):
        oid=self.add_order();self.cart(qty=2)
        self.assertEqual(self.post("/api/cart/clear",{}).status_code,200)
        self.assertEqual(self.post("/api/cart/clear",{}).status_code,200)
        self.assertEqual(self.client.get("/api/cart").get_json()["items"],[])
        self.assertEqual(self.client.get("/api/orders/"+oid).status_code,200)

    def test_uncertain_checkout_cannot_be_discarded_by_clear_cart(self):
        self.cart();self.backend.lost=True;op=str(uuid.uuid4());self.checkout(op)
        self.assertEqual(self.post("/api/cart/clear",{}).status_code,409)
        self.assertEqual(self.checkout(op).status_code,201)
        self.assertEqual(len(self.backend.orders),1)

    def test_concurrent_cart_additions_preserve_both_items(self):
        from concurrent.futures import ThreadPoolExecutor
        self.backend.menu["items"].append({"item_id":"tea","name":"Tea","description":"Synthetic","price_minor":501,"available":True})
        cookie=self.client.get_cookie("supper_session").value
        def add(item):
            browser=self.app.test_client();browser.set_cookie("supper_session",cookie)
            return browser.post("/api/cart",json={"restaurant_id":"paper","item_id":item,"quantity":1},headers={"X-CSRF-Token":self.csrf}).status_code
        with ThreadPoolExecutor(max_workers=2) as pool:self.assertEqual(list(pool.map(add,["bowl","tea"])),[200,200])
        value=self.client.get("/api/cart").get_json()
        self.assertEqual({i["item_id"] for i in value["items"]},{"bowl","tea"})
        self.assertEqual(value["preview_total_minor"],13500)

    def test_nonlocal_start_fails_closed_before_serving_demo_roles(self):
        with self.assertRaises(RuntimeError):create_app({"APP_ENV":"production","STATE_PATH":self.path})

class AnalyticsTests(unittest.TestCase):
    def setUp(self):self.now=datetime(2026,11,13,12,25,tzinfo=timezone.utc)
    def row(self,minutes=0,status="DELIVERED"):
        return {"order_id":str(uuid.uuid4()),"created_at":(self.now-timedelta(minutes=minutes)).isoformat(),"status":status,
            "total_minor":12999,"currency":"INR","restaurant_id":"demo"}
    def events(self):
        return [{"to_state":state,"occurred_at":(self.now+timedelta(seconds=delta)).isoformat()} for state,delta in
            (("PENDING_PAYMENT",-120),("PREPARING",-100),("READY",-40),("PICKED_UP",-30),("DELIVERED",-10))]
    def test_completed_durations_use_actual_stage_pairs(self):
        result=history_metrics(self.events())
        self.assertEqual((result["preparation_seconds"],result["delivery_seconds"]),(60,20))
    def test_incomplete_durations_are_missing_not_zero(self):
        row=self.row(status="PREPARING")
        result=aggregate([row],{row["order_id"]:self.events()[:2]},now=self.now)
        self.assertIsNone(result["preparation"]["mean_seconds"])
        self.assertEqual(result["preparation"]["sample_count"],0)
    def test_arrival_buckets_include_both_partial_edge_hours(self):
        rows=[self.row(minutes=0),self.row(minutes=24*60-1)]
        result=aggregate(rows,{},now=self.now)
        self.assertEqual(sum(bucket["count"] for bucket in result["arrivals"]),2)
        self.assertEqual(result["order_count"],2)
    def test_unavailable_history_excluded_from_means_and_reported(self):
        row=self.row();result=aggregate([row],{row["order_id"]:None},now=self.now)
        self.assertEqual(result["history_unavailable_count"],1)
        self.assertIsNone(result["delivery"]["mean_seconds"])
    def test_outside_window_orders_excluded(self):
        row=self.row(minutes=25*60)
        self.assertEqual(aggregate([row],{},now=self.now)["order_count"],0)
    def test_repeated_state_event_does_not_double_duration_or_order_counts(self):
        row=self.row();events=self.events()
        result=aggregate([row],{row["order_id"]:events+[events[-1]]},now=self.now)
        self.assertEqual(result["order_count"],1);self.assertEqual(result["delivery"]["sample_count"],1)

if __name__=="__main__":unittest.main()
