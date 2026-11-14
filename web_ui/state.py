"""Server-side demo sessions, carts and durable HTTP operation identities."""
from contextlib import contextmanager
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import time
import uuid
from werkzeug.exceptions import Conflict

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()

class State:
    def __init__(self,path):
        self.path=str(path);Path(path).parent.mkdir(parents=True,exist_ok=True)
        with self.connection() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, csrf TEXT NOT NULL, data TEXT NOT NULL, updated REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations(session_id TEXT NOT NULL, id TEXT NOT NULL, signature TEXT NOT NULL,
              data TEXT NOT NULL, response TEXT, status TEXT NOT NULL, PRIMARY KEY(session_id,id));
            """)
    @contextmanager
    def connection(self):
        c=sqlite3.connect(self.path,timeout=10);c.row_factory=sqlite3.Row
        try:
            yield c;c.commit()
        except BaseException:
            c.rollback();raise
        finally:c.close()
    def session(self,value):
        with self.connection() as c:
            row=c.execute("SELECT * FROM sessions WHERE id=? AND updated>?",(value,time.time()-30*86400)).fetchone() if value else None
            if row:return dict(row),False
            value,csrf=secrets.token_urlsafe(32),secrets.token_urlsafe(32)
            c.execute("INSERT INTO sessions VALUES(?,?,?,?)",(value,csrf,"{}",time.time()))
            return {"id":value,"csrf":csrf,"data":"{}"},True
    def save(self,sid,data):
        with self.connection() as c:c.execute("UPDATE sessions SET data=?,updated=? WHERE id=?",(json.dumps(data),time.time(),sid))
    def workspace(self,value):
        with self.connection() as c:
            row=c.execute("SELECT data FROM workspaces WHERE id=?",(value,)).fetchone()
        return json.loads(row["data"]) if row else None
    def save_workspace(self,data):
        with self.connection() as c:
            c.execute("INSERT INTO workspaces VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",(data["id"],json.dumps(data)))
    def list_workspaces(self):
        with self.connection() as c:rows=c.execute("SELECT data FROM workspaces ORDER BY rowid DESC LIMIT 20").fetchall()
        return [json.loads(row["data"]) for row in rows]
    def begin(self,sid,operation_id,payload):
        signature=digest(payload)
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            row=c.execute("SELECT * FROM operations WHERE session_id=? AND id=?",(sid,operation_id)).fetchone()
            if row:
                if row["signature"]!=signature:raise Conflict("This action changed. Reload before submitting a new action.")
                result=dict(row)
                result["is_new"]=False
                result["data"]=json.loads(result["data"])
                result["response"]=json.loads(result["response"]) if result["response"] else None
                return result
            c.execute("INSERT INTO operations VALUES(?,?,?,?,?,?)",(sid,operation_id,signature,json.dumps(payload),None,"PENDING"))
        return {"data":payload,"response":None,"status":"PENDING","is_new":True}
    def finish(self,sid,operation_id,response):
        with self.connection() as c:c.execute("UPDATE operations SET response=?,status='CONFIRMED' WHERE session_id=? AND id=?",
            (json.dumps(response),sid,operation_id))
    def pending(self,sid,kind):
        with self.connection() as c:rows=c.execute("SELECT id,data FROM operations WHERE session_id=? AND status='PENDING' ORDER BY rowid DESC",(sid,)).fetchall()
        for row in rows:
            data=json.loads(row["data"])
            if data.get("kind")==kind:return {"operation_id":row["id"],**data}
        return None
    def reject(self,sid,operation_id):
        with self.connection() as c:c.execute("UPDATE operations SET status='REJECTED' WHERE session_id=? AND id=?",(sid,operation_id))

    def operation(self,sid,value):
        with self.connection() as c:row=c.execute("SELECT * FROM operations WHERE session_id=? AND id=?",(sid,value)).fetchone()
        if not row:return None
        return {**dict(row),"data":json.loads(row["data"]),"response":json.loads(row["response"]) if row["response"] else None}

    def patch_context(self,sid,fields,*,replace=False):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            old=json.loads(c.execute("SELECT data FROM sessions WHERE id=?",(sid,)).fetchone()["data"])
            if replace and self._pending_checkout(c,sid):
                raise Conflict("Resolve the pending checkout before changing workspaces.")
            data=dict(fields) if replace else {**old,**fields}
            c.execute("UPDATE sessions SET data=?,updated=? WHERE id=?",(json.dumps(data),time.time(),sid))
            return data

    def _pending_checkout(self,c,sid):
        for row in c.execute("SELECT data FROM operations WHERE session_id=? AND status='PENDING'",(sid,)):
            if json.loads(row["data"]).get("kind")=="checkout":return True
        return False

    def cart(self,sid,workspace_id,rid,item,quantity):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            data=json.loads(c.execute("SELECT data FROM sessions WHERE id=?",(sid,)).fetchone()["data"])
            if data.get("workspace_id")!=workspace_id:raise Conflict("The selected workspace changed. Reload before updating the cart.")
            if self._pending_checkout(c,sid):raise Conflict("Resolve the pending checkout before changing the cart.")
            cart=data.get("cart",{})
            if cart.get("restaurant_id")!=rid and cart.get("items"):raise Conflict("Finish this cart before ordering from another kitchen.")
            items=cart.get("items",{}).copy() if cart.get("restaurant_id")==rid else {}
            if quantity:items[item]=quantity
            else:items.pop(item,None)
            data["cart"]={"restaurant_id":rid,"items":items} if items else {}
            c.execute("UPDATE sessions SET data=?,updated=? WHERE id=?",(json.dumps(data),time.time(),sid))
            return data

    def checkout(self,sid,value,workspace,address,scenario):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            row=c.execute("SELECT * FROM operations WHERE session_id=? AND id=?",(sid,value)).fetchone()
            if row:
                spec=json.loads(row["data"])
                if spec.get("kind")!="checkout" or spec.get("workspace_id")!=workspace["id"] or spec["body"]["address_id"]!=address or spec["notification_scenario"]!=scenario:
                    raise Conflict("This checkout key was already used for a different request.")
                return {"data":spec,"response":json.loads(row["response"]) if row["response"] else None}
            if self._pending_checkout(c,sid):raise Conflict("Retry the existing checkout instead of creating a second order.")
            data=json.loads(c.execute("SELECT data FROM sessions WHERE id=?",(sid,)).fetchone()["data"])
            if data.get("workspace_id")!=workspace["id"]:raise Conflict("The workspace changed. Reload before checkout.")
            cart=data.get("cart",{})
            if not cart.get("items"):raise Conflict("Your cart is empty.")
            payload={"customer_id":workspace["customer_id"],"address_id":address,"restaurant_id":cart["restaurant_id"],
                "items":[{"item_id":item,"quantity":qty} for item,qty in sorted(cart["items"].items())]}
            spec={"kind":"checkout","body":payload,"workspace_id":workspace["id"],"notification_scenario":scenario}
            c.execute("INSERT INTO operations VALUES(?,?,?,?,?,?)",(sid,value,digest(spec),json.dumps(spec),None,"PENDING"))
            return {"data":spec,"response":None}

    def finish_checkout(self,sid,value,response,workspace_id):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("UPDATE operations SET response=?,status='CONFIRMED' WHERE session_id=? AND id=?",(json.dumps(response),sid,value))
            data=json.loads(c.execute("SELECT data FROM sessions WHERE id=?",(sid,)).fetchone()["data"])
            if data.get("workspace_id")==workspace_id:data["cart"]={}
            c.execute("UPDATE sessions SET data=?,updated=? WHERE id=?",(json.dumps(data),time.time(),sid))
            workspace=json.loads(c.execute("SELECT data FROM workspaces WHERE id=?",(workspace_id,)).fetchone()["data"])
            if response["order_id"] not in workspace["orders"]:workspace["orders"].append(response["order_id"])
            c.execute("UPDATE workspaces SET data=? WHERE id=?",(json.dumps(workspace),workspace_id))
            return data

    def clear_cart(self,sid,workspace_id):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            data=json.loads(c.execute("SELECT data FROM sessions WHERE id=?",(sid,)).fetchone()["data"])
            if data.get("workspace_id")!=workspace_id:raise Conflict("The workspace changed. Reload before changing the cart.")
            if self._pending_checkout(c,sid):raise Conflict("Resolve the pending checkout before changing the cart.")
            data["cart"]={}
            c.execute("UPDATE sessions SET data=?,updated=? WHERE id=?",(json.dumps(data),time.time(),sid))
            return data
