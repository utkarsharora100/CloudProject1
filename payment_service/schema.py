"""Add only payment tables after the Order schema exists."""
from order_service.models import db
from order_service.storage import create_storage_app
from payment_service.models import Payment, PaymentAttempt, PaymentEvent, PaymentRequest, ProviderOperation, Refund

def main():
    app = create_storage_app()
    with app.app_context():
        db.metadata.create_all(bind=db.engine, tables=[m.__table__ for m in
            (Payment, PaymentAttempt, Refund, ProviderOperation, PaymentRequest, PaymentEvent)])
    print("Local simulated payment schema ready; existing order and legacy records preserved.", flush=True)

if __name__ == "__main__":
    main()
