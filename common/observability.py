"""Low-cardinality metrics and structured logs; never log paths, queries or submitted data."""
import json
import logging
import os
import time
from flask import Response,g,request
from prometheus_client import CollectorRegistry,Counter,Histogram,generate_latest,CONTENT_TYPE_LATEST

def install(app,service):
    if "observability" in app.extensions:return
    registry=CollectorRegistry()
    requests=Counter("food_http_requests_total","Completed HTTP requests",["service","method","route","status"],registry=registry)
    latency=Histogram("food_http_request_duration_seconds","HTTP request duration",["service","method","route"],buckets=(.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10),registry=registry)
    app.extensions["observability"]=registry
    # Werkzeug's access logger includes raw paths and queries. Our own logger uses route templates only.
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    logger=logging.getLogger("food.access")
    if not logger.handlers:
        handler=logging.StreamHandler();handler.setFormatter(logging.Formatter("%(message)s"));logger.addHandler(handler)
    logger.setLevel(logging.INFO);logger.propagate=False
    @app.before_request
    def start():
        g.observed_start=time.monotonic()
    @app.after_request
    def finish(response):
        if request.path in {"/metrics","/health","/ready"}:return response
        route=request.url_rule.rule if request.url_rule else "unmatched"
        method=request.method if request.method in {"GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS"} else "OTHER"
        elapsed=max(0,time.monotonic()-getattr(g,"observed_start",time.monotonic()))
        requests.labels(service,method,route,str(response.status_code)).inc()
        latency.labels(service,method,route).observe(elapsed)
        if not app.testing:
            logger.info(json.dumps({"event":"http_request","service":service,"method":method,"route":route,"status":response.status_code,"duration_seconds":round(elapsed,6)},separators=(",",":")))
        return response
    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(registry),content_type=CONTENT_TYPE_LATEST)
