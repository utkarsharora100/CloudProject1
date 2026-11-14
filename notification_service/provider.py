"""Independent SQL-backed in-app transport, with stable logical delivery IDs."""
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import Conflict
from notification_service.models import TransportReceipt

class SimulatedTransport:
    def __init__(self, engine):
        self.engine = engine

    def deliver(self, notification_id, payload_digest, now):
        table = TransportReceipt.__table__
        with self.engine.begin() as connection:
            existing = connection.execute(select(table).where(table.c.notification_id == notification_id)).mappings().one_or_none()
            if existing:
                if existing["payload_digest"] != payload_digest:
                    raise Conflict("The stable notification transport identity differs.")
                return existing["delivered_at"], False
            try:
                with connection.begin_nested():
                    connection.execute(table.insert().values(notification_id=notification_id,
                        payload_digest=payload_digest, delivered_at=now))
                return now, True
            except IntegrityError:
                existing = connection.execute(select(table).where(table.c.notification_id == notification_id)).mappings().one()
                if existing["payload_digest"] != payload_digest:
                    raise Conflict("The stable notification transport identity differs.")
                return existing["delivered_at"], False
