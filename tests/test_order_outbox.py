from datetime import timedelta
import json
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError
from order_fixtures import PAYLOAD, make_app
from order_service.models import Outbox, db, utc_now
from order_service.outbox import (DeliveryUnconfirmed, claim_next, confirmed_publish,
                                  dispatch_once, finish_claim)


class DemoProducer:
    def __init__(self, fail=False, missing_callback=False):
        self.fail = fail
        self.missing_callback = missing_callback
        self.sent = []

    def produce(self, **kwargs):
        self.sent.append(kwargs)

    def flush(self, timeout):
        if not self.missing_callback:
            self.sent[-1]["on_delivery"](object() if self.fail else None, None)
        return 0


class OrderOutboxTests(unittest.TestCase):
    def setUp(self):
        self.app = make_app()
        self.context = self.app.app_context()
        self.context.push()
        self.client = self.app.test_client()
        response = self.client.post("/orders", json=PAYLOAD, headers={"Idempotency-Key": "outbox-create-key"})
        self.assertEqual(response.status_code, 201)
        self.order = response.get_json()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.context.pop()

    def test_event_is_marked_only_after_confirmed_acknowledgement(self):
        producer = DemoProducer()
        self.assertEqual(dispatch_once(producer), "published")
        row = Outbox.query.first()
        self.assertIsNotNone(row.published_at)
        self.assertEqual(row.attempts, 1)
        self.assertIsNone(row.claimed_by)
        self.assertEqual(dispatch_once(producer), "idle")
        self.assertEqual(len(producer.sent), 1)

    def test_failed_publish_persists_backoff_and_retries_the_same_event(self):
        failed = DemoProducer(fail=True)
        self.assertEqual(dispatch_once(failed), "retry")
        row = Outbox.query.first()
        event_id = row.event_id
        self.assertIsNone(row.published_at)
        self.assertEqual(row.last_error_code, "delivery_unconfirmed")
        self.assertGreater(row.available_at, utc_now())
        self.assertIsNone(claim_next())
        row.available_at = utc_now() - timedelta(seconds=1)
        db.session.commit()
        good = DemoProducer()
        self.assertEqual(dispatch_once(good), "published")
        self.assertEqual(json.loads(good.sent[0]["value"])["event_id"], event_id)
        self.assertEqual(Outbox.query.first().attempts, 2)

    def test_missing_ack_callback_is_not_success(self):
        with self.assertRaises(DeliveryUnconfirmed):
            confirmed_publish(DemoProducer(missing_callback=True), "orders", "demo", {"event_id": "demo"})

    def test_publisher_exception_is_redacted(self):
        class Broken:
            def produce(self, **kwargs):
                raise RuntimeError("dummy-private-kafka-detail")
        with self.assertRaises(DeliveryUnconfirmed) as raised:
            confirmed_publish(Broken(), "orders", "demo", {})
        self.assertNotIn("dummy-private", str(raised.exception))

    def test_active_claim_cannot_be_reclaimed(self):
        first = claim_next()
        self.assertIsNotNone(first)
        self.assertIsNone(claim_next())

    def test_expired_claim_recovers_and_fences_the_old_owner(self):
        first = claim_next()
        row = Outbox.query.first()
        row.lease_until = utc_now() - timedelta(seconds=1)
        db.session.commit()
        second = claim_next()
        self.assertEqual(second["event_id"], first["event_id"])
        self.assertNotEqual(second["token"], first["token"])
        self.assertFalse(finish_claim(first, published=True))
        self.assertTrue(finish_claim(second, published=True))

    def test_expired_owner_cannot_mark_success_without_reclaim(self):
        claim = claim_next()
        future = utc_now() + timedelta(seconds=60)
        self.assertFalse(finish_claim(claim, published=True, now=future))
        self.assertIsNone(Outbox.query.first().published_at)

    def test_pending_earlier_event_blocks_only_its_own_order(self):
        response = self.client.post(f"/orders/{self.order['order_id']}/transitions",
            json={"to_state": "PAID", "expected_version": 1}, headers={"Idempotency-Key": "paid-transition-key"})
        self.assertEqual(response.status_code, 200)
        first = claim_next()
        self.assertEqual(first["payload"]["sequence"], 1)
        finish_claim(first, published=False)
        self.assertIsNone(claim_next())
        other = self.client.post("/orders", json=PAYLOAD, headers={"Idempotency-Key": "other-outbox-create"}).get_json()
        next_claim = claim_next()
        self.assertEqual(next_claim["order_id"], other["order_id"])
        finish_claim(next_claim, published=True)
        oldest = db.session.get(Outbox, first["event_id"])
        oldest.available_at = utc_now() - timedelta(seconds=1)
        db.session.commit()
        retry = claim_next()
        finish_claim(retry, published=True)
        second = claim_next()
        self.assertEqual(second["order_id"], self.order["order_id"])
        self.assertEqual(second["payload"]["sequence"], 2)

    def test_ack_then_database_failure_can_republish_with_the_same_event_id(self):
        first = DemoProducer()
        with patch("order_service.outbox.finish_claim", side_effect=SQLAlchemyError("dummy-db-detail")):
            with self.assertRaises(SQLAlchemyError):
                dispatch_once(first)
        row = Outbox.query.first()
        self.assertIsNone(row.published_at)
        row.lease_until = utc_now() - timedelta(seconds=1)
        db.session.commit()
        second = DemoProducer()
        self.assertEqual(dispatch_once(second), "published")
        self.assertEqual(json.loads(first.sent[0]["value"])["event_id"], json.loads(second.sent[0]["value"])["event_id"])


if __name__ == "__main__":
    unittest.main()
