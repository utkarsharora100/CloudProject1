"""Add only notification-owned tables; retain every existing service record."""
from order_service.storage import create_storage_app
from order_service.models import db
from notification_service.models import MODELS

def main():
    app = create_storage_app()
    with app.app_context():
        db.metadata.create_all(bind=db.engine, tables=[model.__table__ for model in MODELS])
    print("Notification schema ready; existing business records retained.", flush=True)

if __name__ == "__main__":
    main()
