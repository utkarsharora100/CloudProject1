"""Fixed templates and constrained recipient/scenario inputs."""
from werkzeug.exceptions import BadRequest
from task_scheduler.domain import number, uuid_id

SCENARIOS = {"success", "fail_once", "always_fail", "response_loss_once"}
CUSTOMER_TEMPLATES = {
    "PENDING_PAYMENT": ("Order received", "Your order is awaiting payment."),
    "PAID": ("Payment confirmed", "Your payment was confirmed."),
    "PAYMENT_FAILED": ("Payment unsuccessful", "Your payment did not succeed. You can retry payment."),
    "CANCELLED": ("Order cancelled", "Your order was cancelled."),
    "KITCHEN_ACCEPTED": ("Kitchen accepted", "The kitchen accepted your order."),
    "PREPARING": ("Preparing your order", "The kitchen has started preparing your order."),
    "READY": ("Order ready", "Your order is ready for courier pickup."),
    "COURIER_ASSIGNED": ("Courier assigned", "A courier has been assigned to your order."),
    "PICKED_UP": ("Order on the way", "Your courier has picked up your order."),
    "DELIVERED": ("Order delivered", "Your order has been delivered."),
    "REFUND_PENDING": ("Refund requested", "Your refund is being processed."),
    "REFUNDED": ("Refund confirmed", "Your refund was confirmed.")}
COURIER_STAGES = {"CourierAssigned": "COURIER_ASSIGNED", "OrderPickedUp": "PICKED_UP", "OrderDelivered": "DELIVERED"}
COURIER_TEMPLATES = {
    "COURIER_ASSIGNED": ("New delivery assignment", "You have been assigned an order."),
    "PICKED_UP": ("Pickup confirmed", "Your order pickup was confirmed."),
    "DELIVERED": ("Delivery confirmed", "Your delivery was confirmed.")}
DEFAULT_KEYS = {"PAID": "notify_paid", "READY": "notify_ready", "DELIVERED": "notify_delivered"}

def scenario(value):
    if not isinstance(value, str) or value not in SCENARIOS:
        raise BadRequest("Choose success, fail_once, always_fail or response_loss_once.")
    return value

def recipient(kind, value):
    if kind == "customer":
        if isinstance(value, str) and value.isascii() and value.isdigit():
            value = int(value)
        return kind, str(number(value, "customer_id", 1, 2147483647))
    if kind == "courier":
        return kind, uuid_id(value)
    raise BadRequest("Choose a customer or courier recipient.")

def subscription_request(body):
    if not isinstance(body, dict) or set(body)-{"customer_id", "scenario", "courier_updates"}:
        raise BadRequest("Provide customer_id and optional scenario/courier_updates only.")
    clean = {"customer_id": number(body.get("customer_id"), "customer_id", 1, 2147483647),
             "scenario": scenario(body.get("scenario", "success")),
             "courier_updates": body.get("courier_updates", True)}
    if type(clean["courier_updates"]) is not bool:
        raise BadRequest("courier_updates must be boolean.")
    return clean

def content_digest(message):
    from task_scheduler.domain import digest
    return digest({"id":message.id,"order":message.order_id,"recipient":message.recipient_id,
        "kind":message.recipient_type,"stage":message.stage,"event":message.source_event_id,"channel":message.channel})
