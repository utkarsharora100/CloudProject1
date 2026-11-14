"""Restaurant Service's private read-only stream aggregate API."""
import os
import re
from flask import Flask,jsonify,request
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from common.deployment import firestore_client,require_profile
from common.observability import install

def create_app(config=None,*,client=None):
    app=Flask(__name__)
    if config:app.config.update(config)
    if not app.testing:require_profile()
    database=client or firestore_client()
    @app.get("/health")
    def health():
        list(database.collection("restaurant_analytics_v1").limit(1).stream(timeout=5))
        return jsonify({"service":"restaurant-analytics","status":"healthy","component_of":"restaurant-service"})
    @app.get("/analytics/windows")
    def windows():
        rid=request.args.get("restaurant_id","");limit=request.args.get("limit","60")
        if set(request.args)-{"restaurant_id","limit"} or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}",rid) or not limit.isascii() or not limit.isdigit() or not 1<=int(limit)<=120:
            return jsonify({"error":"Provide one restaurant_id and a limit between 1 and 120."}),400
        query=database.collection("restaurant_analytics_v1").where(filter=FieldFilter("restaurant_id","==",rid)).order_by("window_start",direction=firestore.Query.DESCENDING).limit(int(limit))
        from restaurant_service.analytics_contract import FIELDS
        records=[{k:v for k,v in row.to_dict().items() if k in FIELDS} for row in query.stream(timeout=5)]
        return jsonify({"restaurant_id":rid,"windows":list(reversed(records)),"source":"flink_kafka_committed_results","window_definition":"Event-time tumbling windows; distinct orders with activity, not creation count.","timezone":"UTC"})
    @app.errorhandler(Exception)
    def failed(error):
        from werkzeug.exceptions import HTTPException
        if isinstance(error,HTTPException):return jsonify({"error":error.description}),error.code
        app.logger.error("Restaurant aggregate data unavailable.")
        return jsonify({"error":"Stream results are temporarily unavailable."}),503
    install(app,"restaurant-analytics")
    return app
def main():create_app().run(host="0.0.0.0",port=8091,debug=False)
if __name__=="__main__":main()
