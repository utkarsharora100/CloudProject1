"""Add only kitchen tables; preserve Order, Payment and Firestore records."""
from order_service.models import db
from order_service.storage import create_storage_app
from kitchen_workflow.models import Ticket, Command, RequestReceipt, Event

def main():
    app = create_storage_app()
    with app.app_context():
        db.metadata.create_all(bind=db.engine, tables=[m.__table__ for m in (Ticket, Command, RequestReceipt, Event)])
    print("Local kitchen schema ready; existing records preserved.", flush=True)

if __name__ == "__main__":
    main()
