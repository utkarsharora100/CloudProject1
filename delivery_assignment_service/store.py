"""Atomic physical courier allocation and explicit handoff commands."""
from copy import deepcopy
from datetime import timedelta
from sqlalchemy import select, func
from werkzeug.exceptions import Conflict, NotFound
from order_service.models import Order,OrderEvent,db,utc_now,timestamp
from order_service.domain import EVENT_TYPES,request_hash,validate_transition
from order_service.events import append_event
from kitchen_workflow.models import Ticket
from payment_service.models import Payment
from task_scheduler.fencing import lock_task,check_live
from delivery_assignment_service.models import Fleet,Zone,Courier,Job,Assignment,ActiveSlot,Command,Receipt,Event
from delivery_assignment_service import policy
from delivery_assignment_service.domain import PRESENCE_SECONDS

def stamp(value):
    return timestamp(value) if value is not None else None

class DeliveryStore:
    def __init__(self,coordination,*,clock=None):
        self.coordination=coordination
        self.clock=clock or (lambda:db.session.execute(select(func.utc_timestamp(6))).scalar_one()
                            if db.engine.dialect.name=="mysql" else utc_now())

    def fleet(self):
        return db.session.execute(select(Fleet).where(Fleet.id==1).with_for_update()).scalar_one()

    def order(self,value,*,lock=False):
        query=select(Order).where(Order.id==value)
        if lock:query=query.with_for_update()
        row=db.session.execute(query).scalar_one_or_none()
        if row is None:raise NotFound("Order not found.")
        return row

    def zone(self,value):
        row=db.session.get(Zone,value)
        if row is None:raise NotFound("Simulation zone not found.")
        return row

    def courier(self,value,*,lock=False):
        query=select(Courier).where(Courier.id==value)
        if lock:query=query.with_for_update()
        row=db.session.execute(query).scalar_one_or_none()
        if row is None:raise NotFound("Courier not found.")
        return row

    def job(self,value):
        row=db.session.get(Job,value)
        if row is None:raise NotFound("Dispatch request not found.")
        return row

    def assignment(self,value):
        return db.session.execute(select(Assignment).where(Assignment.order_id==value)).scalar_one_or_none()

    def emit(self,zone,kind,*,order=None,courier=None,assignment=None,data=None):
        now=self.clock()
        db.session.add(Event(zone=zone,event_type=kind,order_id=order,courier_id=courier,
            assignment_id=assignment,data=data or {},occurred_at=now,available_at=now,attempts=0))

    def courier_data(self,courier,*,now=None):
        now=now or self.clock()
        slot=db.session.get(ActiveSlot,courier.id)
        assignment=db.session.get(Assignment,slot.assignment_id) if slot else None
        fresh=policy.fresh(courier,now)
        state=("RECONCILIATION_REQUIRED" if not fresh else
               ("DELIVERING" if assignment.state=="PICKED_UP" else "ASSIGNED")) if assignment else (
               "AVAILABLE" if fresh and courier.accepting_work else "OFFLINE")
        return {"courier_id":courier.id,"name":courier.name,"zone":courier.zone,"version":courier.version,
            "online":courier.online,"accepting_work":courier.accepting_work,"presence_fresh":fresh,
            "last_seen_at":stamp(courier.last_seen_at),
            "presence_valid_until":stamp(courier.last_seen_at+timedelta(seconds=PRESENCE_SECONDS)) if courier.last_seen_at else None,
            "state":state,"eligible":state=="AVAILABLE","occupied":slot is not None,
            "active_assignment_id":assignment.id if assignment else None,
            "active_order_id":slot.order_id if slot else None,"assignment_state":assignment.state if assignment else None,
            "last_assigned_at":stamp(courier.last_assigned_at),"last_delivery_at":stamp(courier.last_delivery_at)}

    def zone_data(self,zone):
        return {"zone":zone.id,"run_id":zone.run_id,"scenario":zone.scenario,
                "created_at":stamp(zone.created_at),"simulation":True,"presence_ttl_seconds":PRESENCE_SECONDS}

    def capacity(self,value):
        zone=self.zone(value);now=self.clock()
        rows=db.session.execute(select(Courier).where(Courier.zone==value)).scalars().all()
        counts={key:0 for key in ("AVAILABLE","ASSIGNED","DELIVERING","OFFLINE","RECONCILIATION_REQUIRED")}
        for row in rows:counts[self.courier_data(row,now=now)["state"]]+=1
        return {**self.zone_data(zone),"counts":counts,"total":len(rows),"as_of":stamp(now)}

    def job_data(self,job,order):
        assignment=self.assignment(job.order_id)
        return {"order_id":job.order_id,"zone":job.zone,"state":job.state,"version":job.version,
            "order_status":order.status,"order_version":order.version,
            "ready_at":stamp(job.ready_at),"requested_at":stamp(job.requested_at),
            "assigned_at":stamp(job.assigned_at),"picked_up_at":stamp(job.picked_up_at),"delivered_at":stamp(job.delivered_at),
            "wait_reason":policy.gate(db.session,job.order_id,"courier_assignment",self.clock()) if job.state=="WAITING" else None,
            "dispatch_wait_seconds":max(0,((job.assigned_at or self.clock())-job.requested_at).total_seconds()),
            "ready_wait_seconds":max(0,((job.assigned_at or self.clock())-job.ready_at).total_seconds()),
            "delivery_seconds":(job.delivered_at-job.picked_up_at).total_seconds() if job.delivered_at and job.picked_up_at else None,
            "assignment":{"assignment_id":assignment.id,"courier_id":assignment.courier_id,"state":assignment.state,
                "assigned_at":stamp(assignment.assigned_at),"picked_up_at":stamp(assignment.picked_up_at),
                "delivered_at":stamp(assignment.delivered_at),"task_id":assignment.task_id,"generation":assignment.generation} if assignment else None}

    def detail(self,value):
        job=self.job(value);order=self.order(value)
        commands=db.session.execute(select(Command).where(Command.order_id==value).order_by(Command.expected_version,Command.id)).scalars().all()
        return {"delivery":{**self.job_data(job,order),"commands":[{"command_id":c.id,"action":c.action,"state":c.state,
            "courier_id":c.courier_id,"assignment_id":c.assignment_id,"created_at":stamp(c.created_at),
            "applied_at":stamp(c.applied_at),"task_id":c.task_id,"generation":c.generation} for c in commands]}}

    def public_action(self,action,value,data,key):
        db.session.rollback();self.fleet()
        scope=action+(":"+value if value else "")
        digest=request_hash(data)
        previous=db.session.execute(select(Receipt).where(Receipt.scope==scope,Receipt.key_digest==key).with_for_update()).scalar_one_or_none()
        if previous:
            if previous.request_hash!=digest:raise Conflict("This delivery request key was used with a different request.")
            body,code=deepcopy(previous.response),previous.status_code
            db.session.rollback();return body,code,True
        now=self.clock()
        if action=="zone":
            if db.session.get(Zone,data["zone"]):raise Conflict("This simulation zone already exists.")
            row=Zone(id=data["zone"],scenario=data["scenario"],created_at=now)
            db.session.add(row);db.session.flush()
            self.emit(row.id,"SimulationZoneCreated",data={"run_id":row.run_id,"scenario":row.scenario})
            body,code=self.zone_data(row),201
        elif action=="courier":
            self.zone(data["zone"])
            if db.session.execute(select(func.count()).select_from(Courier).where(Courier.zone==data["zone"])).scalar_one()>=100:
                raise Conflict("The local simulation zone courier limit is reached.")
            row=Courier(name=data["name"],zone=data["zone"],version=1,online=False,accepting_work=False,created_at=now,updated_at=now)
            db.session.add(row);db.session.flush()
            self.emit(row.zone,"CourierCreated",courier=row.id,data={"name":row.name,"online":False,"accepting_work":False})
            body,code={"courier":self.courier_data(row)},201
        elif action in {"presence","heartbeat"}:
            row=self.courier(value,lock=True)
            if action=="presence":
                if row.version!=data["expected_version"]:raise Conflict("Courier version changed; reload before changing presence.")
                row.online,row.accepting_work=data["online"],data["accepting_work"]
                row.version+=1
            elif not row.online:
                raise Conflict("Bring this courier online before refreshing presence.")
            if row.online:row.last_seen_at=now
            row.updated_at=now
            self.emit(row.zone,"CourierPresenceChanged" if action=="presence" else "CourierHeartbeat",courier=row.id,
                      data={"online":row.online,"accepting_work":row.accepting_work})
            body,code={"courier":self.courier_data(row)},200
        elif action=="dispatch":
            order=self.order(value,lock=True)
            if order.version!=data["expected_version"]:raise Conflict("Order version changed; reload before dispatch.")
            if db.session.get(Job,value):raise Conflict("This order already has a dispatch request.")
            self.zone(data["zone"])
            ticket=db.session.get(Ticket,value)
            payment=db.session.execute(select(Payment).where(Payment.order_id==value)).scalar_one_or_none()
            if (order.status!="READY" or ticket is None or not ticket.managed or ticket.state!="READY"
                    or ticket.ready_at is None or payment is None or payment.state!="CAPTURED"):
                raise Conflict("Dispatch requires confirmed payment and Kitchen-managed READY food.")
            proof=db.session.execute(select(OrderEvent).where(OrderEvent.order_id==value,OrderEvent.to_state=="READY")).scalars().first()
            if proof is None:raise Conflict("Readiness history is unavailable.")
            job=Job(order_id=value,zone=data["zone"],state="WAITING",version=1,source_event_id=proof.event_id,
                    ready_at=ticket.ready_at,requested_at=now)
            db.session.add(job);db.session.flush()
            self.emit(job.zone,"DispatchRequested",order=value,data={"state":"WAITING","ready_at":stamp(job.ready_at)})
            body,code={"delivery":self.job_data(job,order)},202
        elif action=="command":
            order=self.order(value,lock=True);job=self.job(value)
            if order.version!=data["expected_version"]:raise Conflict("Order version changed; reload before confirming handoff.")
            assignment=self.assignment(value)
            if assignment is None or assignment.id!=data["assignment_id"] or assignment.courier_id!=data["courier_id"]:
                raise Conflict("This courier or assignment does not own this delivery.")
            courier=self.courier(data["courier_id"],lock=True)
            self.verify_slot(assignment)
            if not policy.fresh(courier,now):raise Conflict("Fresh online courier presence is required.")
            expected=("ASSIGNED","COURIER_ASSIGNED") if data["action"]=="pickup" else ("PICKED_UP","PICKED_UP")
            if (assignment.state,order.status)!=expected:raise Conflict("Pickup must precede delivery and food must be assigned.")
            if db.session.execute(select(Command.id).where(Command.order_id==value,Command.action==data["action"])).first():
                raise Conflict("This handoff action already exists; replay its original key.")
            command=Command(order_id=value,assignment_id=assignment.id,courier_id=courier.id,action=data["action"],
                            expected_version=order.version,state="PENDING",created_at=now)
            db.session.add(command);db.session.flush();job.version+=1
            self.emit(job.zone,"PickupRequested" if command.action=="pickup" else "DeliveryRequested",
                      order=value,courier=courier.id,assignment=assignment.id,data={"command_id":command.id})
            body,code={"delivery":self.job_data(job,order),"command_id":command.id},202
        else:raise ValueError("Unsupported internal delivery action.")
        db.session.add(Receipt(scope=scope,key_digest=key,request_hash=digest,response=body,status_code=code))
        db.session.commit();return body,code,False

    def verify_slot(self,assignment):
        slot=db.session.get(ActiveSlot,assignment.courier_id)
        if slot is None or slot.order_id!=assignment.order_id or slot.assignment_id!=assignment.id:
            raise Conflict("Physical courier capacity requires reconciliation.")
        return slot

    def move(self,order,target):
        validate_transition(order.status,target)
        old=order.status;order.status,order.version,order.updated_at=target,order.version+1,self.clock()
        append_event(order,EVENT_TYPES[target],old)

    def process(self,task_id,proof):
        db.session.rollback()
        task,value=lock_task(db.session,self.coordination,task_id,proof,self.clock,
                            kinds={"courier_assignment","pickup","delivery"})
        self.fleet();order=self.order(value,lock=True);job=self.job(value)
        check_live(task,self.coordination,proof,self.clock())
        assignment=self.assignment(value)
        if task["kind"]=="courier_assignment":
            if assignment is not None:
                db.session.rollback();return {"task_outcome":"succeeded"}
            if job.state!="WAITING" or order.status!="READY":raise Conflict("This order no longer permits assignment.")
            # Current locking reads after the fleet mutex avoid a snapshot
            # taken before waiting for that mutex on MySQL REPEATABLE READ.
            if policy.head(db.session,job.zone,current=True)!=value:
                db.session.rollback();return {"task_outcome":"deferred"}
            chosen=db.session.execute(policy.available(db.session,job.zone,self.clock()).limit(1).with_for_update()).mappings().first()
            if chosen is None:
                db.session.rollback();return {"task_outcome":"deferred"}
            courier=self.courier(chosen["id"],lock=True)
            now=self.clock()
            if not policy.fresh(courier,now) or not courier.accepting_work or db.session.get(ActiveSlot,courier.id):
                db.session.rollback();return {"task_outcome":"deferred"}
            check_live(task,self.coordination,proof,self.clock())
            assignment=Assignment(order_id=value,courier_id=courier.id,state="ASSIGNED",assigned_at=now,
                                  task_id=task_id,generation=proof["generation"])
            db.session.add(assignment);db.session.flush()
            db.session.add(ActiveSlot(courier_id=courier.id,order_id=value,assignment_id=assignment.id))
            job.state,job.assigned_at,job.version="ASSIGNED",now,job.version+1
            courier.last_assigned_at,courier.updated_at,courier.version=now,now,courier.version+1
            self.move(order,"COURIER_ASSIGNED")
            self.emit(job.zone,"CourierAssigned",order=value,courier=courier.id,assignment=assignment.id,
                      data={"state":"ASSIGNED","ready_at":stamp(job.ready_at),"requested_at":stamp(job.requested_at)})
        else:
            action="pickup" if task["kind"]=="pickup" else "deliver"
            if assignment is None:raise Conflict("An actual courier assignment is required.")
            self.verify_slot(assignment) if assignment.state!="DELIVERED" else None
            command=db.session.execute(select(Command).where(Command.order_id==value,Command.action==action,
                Command.state=="PENDING")).scalar_one_or_none()
            if command is None:
                confirmed=(action=="pickup" and assignment.picked_up_at is not None) or (action=="deliver" and assignment.delivered_at is not None)
                db.session.rollback();return {"task_outcome":"succeeded" if confirmed else "deferred"}
            courier=self.courier(assignment.courier_id,lock=True)
            if not policy.fresh(courier,self.clock()):
                db.session.rollback();return {"task_outcome":"deferred"}
            expected=("ASSIGNED","COURIER_ASSIGNED") if action=="pickup" else ("PICKED_UP","PICKED_UP")
            if (assignment.state,order.status)!=expected or command.expected_version!=order.version:
                raise Conflict("This handoff command is no longer valid.")
            if command.courier_id!=assignment.courier_id or command.assignment_id!=assignment.id:
                raise Conflict("Handoff command ownership changed.")
            ticket=db.session.get(Ticket,value)
            if ticket is None or ticket.state!="READY" or ticket.ready_at is None:
                raise Conflict("Pickup requires actual READY food.")
            now=self.clock()
            check_live(task,self.coordination,proof,now)
            if action=="pickup":
                self.move(order,"PICKED_UP")
                job.state,assignment.state="PICKED_UP","PICKED_UP"
                job.picked_up_at=assignment.picked_up_at=now
                kind="OrderPickedUp"
            else:
                self.move(order,"DELIVERED")
                job.state,assignment.state="DELIVERED","DELIVERED"
                job.delivered_at=assignment.delivered_at=now
                db.session.delete(self.verify_slot(assignment))
                courier.last_delivery_at,courier.updated_at,courier.version=now,now,courier.version+1
                kind="OrderDelivered"
            job.version+=1
            command.state,command.applied_at,command.task_id,command.generation="APPLIED",now,task_id,proof["generation"]
            self.emit(job.zone,kind,order=value,courier=courier.id,assignment=assignment.id,data={"state":job.state,"command_id":command.id})
        check_live(task,self.coordination,proof,self.clock())
        db.session.commit();return {"task_outcome":"succeeded"}
