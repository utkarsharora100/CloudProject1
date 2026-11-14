import os
from common.deployment import service_endpoint
"""Fixed local service destinations; no arbitrary URL proxy or credential forwarding."""
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SERVICES = {"customer":"http://customer-service:8083","restaurant":"http://restaurant-api:8084",
    "order":"http://order-service:8081","payment":"http://payment-service:8086",
    "kitchen":"http://kitchen-api:8087","delivery":"http://delivery-service:8082",
    "notification":"http://notification-api:8088","task":"http://task-api:8085"}

class ServiceError(Exception):
    def __init__(self, status, message="The service could not confirm this request."):
        self.status, self.message = status, message
        super().__init__(message)

class Client:
    def __init__(self):
        self.services={key:os.environ.get(key.upper()+"_API_URL",value) for key,value in SERVICES.items()}
        self.services["analytics"]=os.environ.get("ANALYTICS_API_URL","http://restaurant-analytics-api:8091")
        for key,value in self.services.items():service_endpoint(value,{SERVICES.get(key,"http://restaurant-analytics-api:8091")})
    def call(self, service, method, path, body=None, *, key=None, version=None, query=None):
        if service not in self.services or not path.startswith("/") or "://" in path or ".." in path or "//" in path:
            raise ServiceError(400,"Unsupported service request.")
        url=self.services[service]+path+("?" + urlencode({k:v for k,v in (query or {}).items() if v is not None}) if query else "")
        headers={"Accept":"application/json"}
        if body is not None:headers["Content-Type"]="application/json"
        if key:headers["Idempotency-Key"]=key
        if version is not None:headers["If-Match"]='"'+str(version)+'"'
        req=Request(url,method=method,headers=headers,data=None if body is None else json.dumps(body).encode())
        try:
            response=urlopen(req,timeout=8)
        except HTTPError as error:
            response=error
        except (URLError,TimeoutError,OSError):
            raise ServiceError(503,"The service is temporarily unavailable. The outcome may be unconfirmed; retry safely.") from None
        with response:
            raw=response.read(2*1024*1024)
            try:data=json.loads(raw) if raw else {}
            except (ValueError,UnicodeError):
                raise ServiceError(503,"The service returned an unconfirmed response.") from None
            if response.status>=400:
                # Service errors are already sanitized by the local backend contracts.
                message=data.get("error","The service could not confirm this request.") if isinstance(data,dict) else "The service could not confirm this request."
                raise ServiceError(response.status,message[:350])
            return data

    def optional(self, service, path, *, query=None):
        try:return self.call(service,"GET",path,query=query)
        except ServiceError as error:
            if error.status==404:return None
            raise
