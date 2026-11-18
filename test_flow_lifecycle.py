import json
import unittest

from flow_lifecycle import parse_lifecycle


def event(kind="NEW", protocol=6, family=2):
    obj = {
        "schema": "nlbwmon-flow-v1", "router_name": "lab-router",
        "event_type": kind, "connection_id": "boot-uuid-123-1794600000-42",
        "id": 123, "family": family, "protocol": protocol,
        "orig_src": "192.168.1.5", "orig_dst": "10.0.0.16",
        "reply_src": "10.0.0.16", "reply_dst": "192.168.1.5",
        "orig_sport": 42000, "orig_dport": 5201,
        "reply_sport": 5201, "reply_dport": 42000,
        "orig_packets": 10, "orig_bytes": 1200,
        "reply_packets": 8, "reply_bytes": 900,
        "started_sec": 1794600000,
        "finished_sec": 0 if kind == "NEW" else 1794600300,
    }
    if family == 10:
        obj.update(orig_src="fd00::5", orig_dst="2001:db8::16",
                   reply_src="2001:db8::16", reply_dst="fd00::5")
    return obj


class LifecycleTests(unittest.TestCase):
    def test_new_and_destroy_share_identity(self):
        start = parse_lifecycle(json.dumps(event()))
        end = parse_lifecycle(json.dumps(event("DESTROY")))
        self.assertEqual(start["connection_id"], end["connection_id"])
        self.assertEqual(start["flow"]["status"], "active")
        self.assertNotIn("ended_at", start["flow"])
        self.assertNotIn("duration_seconds", start["flow"])
        self.assertNotIn("bytes", start["source"])
        self.assertEqual(end["flow"]["status"], "closed")
        self.assertEqual(end["source"]["bytes"], 1200)
        self.assertEqual(end["destination"]["bytes"], 900)
        self.assertIn("ended_at", end["flow"])
        self.assertEqual(end["flow"]["duration_seconds"], 300)

    def test_zero_duration_destroy(self):
        obj = event("DESTROY")
        obj["finished_sec"] = obj["started_sec"]
        doc = parse_lifecycle(json.dumps(obj))
        self.assertEqual(doc["flow"]["duration_seconds"], 0)

    def test_destroy_without_new_has_duration(self):
        doc = parse_lifecycle(json.dumps(event("DESTROY")))
        self.assertEqual(doc["flow"]["duration_seconds"], 300)

    def test_icmp_has_no_fake_ports(self):
        for protocol, family in ((1, 2), (58, 10)):
            with self.subTest(protocol=protocol):
                doc = parse_lifecycle(json.dumps(event("DESTROY", protocol, family)))
                self.assertIsNotNone(doc)
                self.assertNotIn("port", doc["source"])
                self.assertNotIn("port", doc["destination"])

    def test_reject_bad_identity_and_timestamps(self):
        for change in ({"connection_id": ""}, {"router_name": "../oops"},
                       {"started_sec": 0}, {"finished_sec": 1},
                       {"orig_bytes": -1}, {"protocol": 99}):
            with self.subTest(change=change):
                obj = event("DESTROY")
                obj.update(change)
                self.assertIsNone(parse_lifecycle(json.dumps(obj)))

    def test_reject_bad_family(self):
        self.assertIsNone(parse_lifecycle(json.dumps(event("NEW", 58, 2))))

    def test_no_intermediate_update(self):
        self.assertIsNone(parse_lifecycle(json.dumps(event("UPDATE"))))


if __name__ == "__main__":
    unittest.main()
