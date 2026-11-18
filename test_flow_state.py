import copy
import unittest

from flow_state import transition, mark_stale


def record(kind, status=None):
    flow = {"status": status or ("active" if kind == "NEW" else "closed"),
            "started_at": "2026-11-14T10:00:00.000Z"}
    if kind == "DESTROY":
        flow["ended_at"] = "2026-11-14T10:05:00.000Z"
    return {"router_name": "lab", "connection_id": "boot-1",
            "event_type": kind, "flow": flow,
            "source": {"ip": "192.0.2.1", **({"bytes": 42} if kind == "DESTROY" else {})}}


class FlowStateTests(unittest.TestCase):
    def test_new_then_destroy(self):
        new, end = record("NEW"), record("DESTROY")
        self.assertEqual(transition(new, end)["flow"]["status"], "closed")
        self.assertEqual(transition(new, end)["source"]["bytes"], 42)
        self.assertNotIn("bytes", new["source"])

    def test_destroy_without_new(self):
        self.assertEqual(transition(None, record("DESTROY"))["flow"]["status"], "closed")

    def test_stale_then_late_destroy(self):
        stale = mark_stale(record("NEW"), 1794660000, 3600)
        self.assertEqual(stale["flow"]["status"], "stale")
        self.assertNotIn("ended_at", stale["flow"])
        self.assertEqual(transition(stale, record("DESTROY"))["flow"]["status"], "closed")

    def test_late_new_never_reopens(self):
        closed = record("DESTROY")
        self.assertEqual(transition(closed, record("NEW")), closed)
        self.assertEqual(transition(record("NEW", "stale"), record("NEW"))["flow"]["status"], "stale")

    def test_duplicate_destroy_no_regression(self):
        closed = record("DESTROY")
        self.assertEqual(transition(closed, record("DESTROY")), closed)

    def test_timeout_boundary_and_no_mutation(self):
        active = record("NEW")
        original = copy.deepcopy(active)
        start = 1794650400  # 2026-11-14 10:00:00 UTC
        self.assertEqual(mark_stale(active, start + 3599, 3600)["flow"]["status"], "active")
        self.assertEqual(mark_stale(active, start + 3600, 3600)["flow"]["status"], "stale")
        self.assertEqual(active, original)

    def test_reject_wrong_identity_and_timeout(self):
        other = record("DESTROY")
        other["connection_id"] = "other"
        with self.assertRaises(ValueError):
            transition(record("NEW"), other)
        with self.assertRaises(ValueError):
            mark_stale(record("NEW"), 0, 0)


if __name__ == "__main__":
    unittest.main()
