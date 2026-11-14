import json
import unittest
from restaurant_service.analytics_contract import validate,identity,fingerprint
class Results(unittest.TestCase):
    def value(self):
        return {"schema_version":1,"source":"restaurant-flink-event-time-v1","restaurant_id":"paper","window_start":"2026-01-01T00:00:00Z","window_end":"2026-01-01T00:01:00Z","window_seconds":60,"unique_order_count":2,"event_count":3,"stage_counts":{"PAID":2,"READY":1}}
    def test_exact_window_and_counts(self):
        value=validate(json.dumps(self.value()).encode());self.assertEqual(value["unique_order_count"],2)
        self.assertEqual(identity(value),identity(dict(reversed(list(value.items())))))
    def test_unsafe_or_private_fields_rejected(self):
        for patch in ({"phone":"synthetic"},{"restaurant_id":"../bad"},{"window_seconds":True},{"unique_order_count":4},{"stage_counts":{"PAID":99}},{"window_end":"2026-01-01T00:02:00Z"},{"schema_version":True}):
            with self.subTest(patch=patch),self.assertRaises(ValueError):validate(json.dumps({**self.value(),**patch}).encode())
    def test_malformed_records_and_timezone_are_not_accepted(self):
        for raw in (b'{bad',b'[]',b'x'*33000,json.dumps({**self.value(),"window_start":"2026-01-01T00:00:00"}).encode()):
            with self.assertRaises(ValueError):validate(raw)
    def test_changed_result_retains_identity_but_changes_integrity_hash(self):
        value=self.value();other={**value,"unique_order_count":1}
        self.assertEqual(identity(value),identity(other));self.assertNotEqual(fingerprint(value),fingerprint(other))
if __name__=="__main__":unittest.main()
