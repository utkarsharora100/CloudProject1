"""Exercise real local services with synthetic identities and explicit demo actors."""
import json
from local.customer_smoke_test import exercise_customer_service
from local.environment import require_local_emulator
from local.order_smoke_test import exercise_order_service
from local.restaurant_smoke_test import exercise_restaurant_service
from local.scheduler_smoke_test import exercise_scheduler
from local.payment_smoke_test import exercise_payments
from local.kitchen_smoke_test import exercise_kitchen
from local.delivery_smoke_test import exercise_delivery
from local.notification_smoke_test import exercise_notifications


def main():
    project = require_local_emulator()
    restaurant = exercise_restaurant_service()
    customer = exercise_customer_service()
    result = exercise_order_service(customer, restaurant)
    scheduler = exercise_scheduler(result)
    payments = exercise_payments()
    kitchen = exercise_kitchen()
    delivery = exercise_delivery()
    notifications = exercise_notifications()
    print(json.dumps({"result": "PASS", "project": project, "scheduler_workflow_id": scheduler["workflow_id"], **payments, **kitchen, **delivery, **notifications, **result}), flush=True)


if __name__ == "__main__":
    main()
