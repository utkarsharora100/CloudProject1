"""Safe structured process logging for centralized collection."""
import json
import sys
def event(service,name,**fields):
    allowed={"running","connected","count","outcome","window_start","window_end","duration_seconds","reason","schema_version"}
    record={"service":service,"event":name}
    record.update({k:v for k,v in fields.items() if k in allowed and isinstance(v,(str,int,float,bool,type(None)))})
    sys.stdout.write(json.dumps(record,separators=(",",":"))+"\n");sys.stdout.flush()
