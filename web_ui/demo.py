"""Explicit fictional starting data; every result is a real domain API record."""
import uuid
from datetime import datetime,timezone
from web_ui.client import ServiceError

MEALS=[
 ("paneer-bowl","Charred paneer bowl","Smoky paneer, herbed rice, crisp greens & a lemon-tahini drizzle.",24900),
 ("pesto-pasta","Garden pesto pasta","Basil pesto, roasted cherry tomatoes, parmesan & fresh pasta.",27900),
 ("mushroom-flatbread","Mushroom flatbread","Roasted mushrooms, mozzarella, thyme & a crisp sourdough base.",22900),
 ("lemon-tea","Lemon iced tea","Slow-brewed tea with fresh lemon. Bright, cool, refreshing.",8900)]

def create_workspace(client,state):
    value={"id":str(uuid.uuid4()),"name":"Paper Lantern Kitchen","status":"SETTING_UP","orders":[],
        "couriers":[],"created_at":datetime.now(timezone.utc).isoformat()}
    state.save_workspace(value)
    try:
        customer=client.call("customer","POST","/customers",{"name":"Demo diner","email":"diner-"+value["id"]+"@example.test"})["customer"]
        value["customer_id"]=customer["id"];state.save_workspace(value)
        address=client.call("customer","POST",f"/customers/{customer['id']}/addresses",{
            "label":"Demo home","line1":"12 Paper Lane (fictional)","city":"Demo City","postal_code":"000000","country_code":"IN"})["address"]
        value["address_id"]=address["id"];state.save_workspace(value)
        restaurant=client.call("restaurant","POST","/restaurants",{"name":value["name"],
            "description":"A fictional kitchen serving bowls, pasta and little comforts. Prepared through the live local workflow.",
            "is_open":True,"estimated_prep_time_mins":12})["restaurant"]
        value["restaurant_id"]=restaurant["restaurant_id"];state.save_workspace(value)
        for item_id,name,description,price in MEALS:
            client.call("restaurant","PUT",f"/restaurants/{value['restaurant_id']}/menu/{item_id}",
                {"name":name,"description":description,"price_minor":price,"available":True})
        zone="paper-"+value["id"].replace("-","")[:16]
        result=client.call("delivery","POST","/delivery/zones",{"zone":zone,"scenario":"manual"},key="zone:"+value["id"])
        value["zone"]=zone;value["run_id"]=result["run_id"];state.save_workspace(value)
        for index in ("A","B"):
            result=client.call("delivery","POST","/couriers",{"name":"Courier "+index,"zone":zone},key="courier:"+value["id"]+index)
            value["couriers"].append(result["courier"]["courier_id"]);state.save_workspace(value)
        value["status"]="READY";state.save_workspace(value);return value
    except Exception:
        value["status"]="INCOMPLETE";state.save_workspace(value)
        raise ServiceError(503,"Demo setup was not fully confirmed. Existing data was retained. Choose a completed workspace or explicitly create a new one.") from None
