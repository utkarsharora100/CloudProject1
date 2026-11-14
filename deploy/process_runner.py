"""Signal-safe process wrapper; bounded, redacted stdout/file logs."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from logging.handlers import RotatingFileHandler
import logging

def redact(line, private):
    for value in sorted(private,key=len,reverse=True):
        line=line.replace(value,"[REDACTED]")
    line=re.sub(r"([A-Za-z][A-Za-z0-9+.-]*://)[^/@\s]+:[^/@\s]+@",r"\1[REDACTED]@",line)
    return re.sub(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+","[REDACTED_JWT]",line)

def main():
    command=sys.argv[1:]
    if not command:raise SystemExit("A process command is required.")
    directory=os.environ.get("LOG_DIR","")
    if not directory:os.execvp(command[0],command)
    name=os.environ.get("SERVICE_NAME") or os.environ.get("WORKER_NAME") or next((v for v in command if "." in v and not v.startswith("-")),"application")
    name=re.sub(r"[^A-Za-z0-9_.-]","-",name)[:100]
    instance=re.sub(r"[^A-Za-z0-9_.-]","-",os.environ.get("HOSTNAME","instance"))[:80]
    path=Path(directory)/(name+"-"+instance+".jsonl");path.parent.mkdir(parents=True,exist_ok=True)
    handler=RotatingFileHandler(path,maxBytes=10*1024*1024,backupCount=2,encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger=logging.getLogger("file");logger.setLevel(logging.INFO);logger.addHandler(handler);logger.propagate=False
    private=[v for k,v in os.environ.items() if re.search(r"PASSWORD|SECRET|TOKEN|API_KEY|PRIVATE_KEY",k) and len(v)>=6]
    child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda number,frame:child.send_signal(number))
    for line in child.stdout:
        safe=redact(line.rstrip("\n"),private)
        print(safe,flush=True)
        try:record=json.loads(safe)
        except ValueError:record={"service":name,"event":"process_log","message":safe[:3000]}
        if not isinstance(record,dict):record={"service":name,"event":"process_log"}
        record.setdefault("service",name)
        logger.info(json.dumps(record,separators=(",",":")))
    handler.close()
    raise SystemExit(child.wait())
if __name__=="__main__":main()
