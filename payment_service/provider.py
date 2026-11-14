"""SQL-backed simulated provider with durable idempotent decisions."""
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import Conflict
from payment_service.models import ProviderOperation

class SimulatedProvider:
    def __init__(self, engine):
        self.engine = engine

    def apply(self, *, operation_id, payment_id, kind, amount_minor, currency, decline=False):
        table = ProviderOperation.__table__
        expected = {"payment_id": payment_id, "kind": kind, "amount_minor": amount_minor,
                    "currency": currency, "outcome": "DECLINED" if decline else "SUCCEEDED"}
        try:
            with self.engine.begin() as connection:
                previous = connection.execute(select(table).where(table.c.id == operation_id)).mappings().one_or_none()
                if previous is not None:
                    if any(previous[key] != value for key, value in expected.items()):
                        raise Conflict("Simulated provider operation identity conflicts.")
                    return dict(previous), False
                connection.execute(table.insert().values(id=operation_id, **expected))
            return {"id": operation_id, **expected}, True
        except IntegrityError:
            with self.engine.connect() as connection:
                previous = connection.execute(select(table).where(table.c.id == operation_id)).mappings().one()
                if any(previous[key] != value for key, value in expected.items()):
                    raise Conflict("Simulated provider operation identity conflicts.") from None
                return dict(previous), False

    def lookup(self, operation_id):
        table = ProviderOperation.__table__
        with self.engine.connect() as connection:
            value = connection.execute(select(table).where(table.c.id == operation_id)).mappings().one_or_none()
            return dict(value) if value else None
