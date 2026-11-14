"""Production WSGI targets; business initialization remains a separate ordered job."""
import importlib
import os
from common.deployment import require_profile
from common.observability import install
FACTORIES={"customer":"customer_service.customer_service","order":"order_service.event_producer","restaurant-api":"restaurant_service.http_api","task-api":"task_scheduler.api","payment":"payment_service.api","kitchen":"kitchen_workflow.api","delivery":"delivery_assignment_service.api","notification":"notification_service.api","web":"web_ui.app","restaurant-analytics":"restaurant_service.analytics_api"}
def create():
    require_profile()
    service=os.environ["SERVICE_NAME"]
    module=importlib.import_module(FACTORIES[service])
    app=module.create_app();install(app,service)
    return app
