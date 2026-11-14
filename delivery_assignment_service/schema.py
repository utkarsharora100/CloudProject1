"""Create only additive delivery records and seed the resource mutex once."""
from order_service.models import db
from order_service.storage import create_storage_app
from delivery_assignment_service.models import Fleet,Zone,Courier,Job,Assignment,ActiveSlot,Command,Receipt,Event

def initialize():
    if db.session.get(Fleet,1) is None:
        db.session.add(Fleet(id=1));db.session.commit()

def main():
    app=create_storage_app()
    with app.app_context():
        db.metadata.create_all(bind=db.engine,tables=[m.__table__ for m in
            (Fleet,Zone,Courier,Job,Assignment,ActiveSlot,Command,Receipt,Event)])
        initialize()
    print("Local delivery schema ready; previous Orders, Payments, Kitchen and legacy data preserved.",flush=True)

if __name__=="__main__":
    main()
