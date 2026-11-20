"""Optional nanosecond wire fields must preserve v1 compatibility."""
import json
import unittest
from flow_lifecycle import parse_lifecycle

BASE = {
    "schema": "nlbwmon-flow-v1", "router_name": "lab", "connection_id": "test-123",
    "event_type": "DESTROY", "family": 2, "protocol": 6,
    "orig_src": "192.0.2.10", "orig_dst": "198.51.100.20",
    "reply_src": "198.51.100.20", "reply_dst": "192.0.2.10",
    "orig_sport": 12345, "orig_dport": 443, "reply_sport": 443, "reply_dport": 12345,
    "orig_packets": 3, "orig_bytes": 100, "reply_packets": 2, "reply_bytes": 50,
    "started_sec": 1795132800, "finished_sec": 1795132887
}

class NanoTests(unittest.TestCase):
    def parse(self, **kw):
        return parse_lifecycle(json.dumps({**BASE, **kw}))

    def test_legacy_seconds(self):
        flow = self.parse()["flow"]
        self.assertEqual(flow["started_at"], "2026-11-20T00:00:00.000Z")
        self.assertNotIn("duration_ns", flow)

    def test_exact_nanos(self):
        flow = self.parse(started_nsec=123456789, finished_nsec=987654321)["flow"]
        self.assertEqual(flow["started_at"], "2026-11-20T00:00:00.123456789Z")
        self.assertEqual(flow["duration_ns"], 87864197532)

    def test_missing_pair(self):
        self.assertIsNone(self.parse(started_nsec=123))

    def test_invalid_nsec(self):
        self.assertIsNone(self.parse(started_nsec=1000000000, finished_nsec=0))

    def test_negative_duration(self):
        self.assertIsNone(self.parse(started_sec=1795132800, finished_sec=1795132800,
                                     started_nsec=100, finished_nsec=99))

if __name__ == "__main__":
    unittest.main()
