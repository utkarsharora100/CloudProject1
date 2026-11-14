"""Scripted physical-courier demonstration through the same local APIs."""
import argparse
import json
from local.delivery_smoke_test import (new_zone,ready_fixture,courier,dispatch,wait_state,handoff,capacity,presence)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--scenario",choices=("normal","no_courier","competition","presence_loss"),default="normal")
    args=parser.parse_args()
    zone=new_zone(args.scenario)
    ready=ready_fixture();dispatch(ready,zone["zone"])
    result={"simulation":True,"scenario":args.scenario,"zone":zone["zone"],"run_id":zone["run_id"],"order_id":ready["order_id"]}
    if args.scenario=="no_courier":
        result["state"]="WAITING"
    else:
        actor=courier(zone["zone"],"Scripted Courier A")
        assigned=wait_state(ready["order_id"],"ASSIGNED")
        result["courier_id"]=actor["courier_id"]
        if args.scenario=="normal":
            handoff(ready["order_id"],"pickup");handoff(ready["order_id"],"deliver")
            presence(actor["courier_id"],False,False);result["state"]="DELIVERED"
        elif args.scenario=="competition":
            other=ready_fixture();dispatch(other,zone["zone"])
            # Refresh after fixture creation; physical capacity remains occupied.
            presence(actor["courier_id"],True,True)
            result.update(state="ASSIGNED",waiting_order_id=other["order_id"])
        else:
            presence(actor["courier_id"],False,False)
            result.update(state="RECONCILIATION_REQUIRED",assignment_id=assigned["assignment"]["assignment_id"])
    result["capacity"]=capacity(zone["zone"])["counts"]
    print(json.dumps(result),flush=True)

if __name__=="__main__":
    main()
