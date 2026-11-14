"""Compatibility module entry point for the durable local Delivery API."""
from delivery_assignment_service.api import create_app

if __name__=="__main__":
    create_app().run(host="0.0.0.0",port=8082,debug=False,use_reloader=False)
