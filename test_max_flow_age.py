"""Retention cutoff unit tests; no OpenSearch or network access."""
import unittest
from flow_collector import is_expired


class MaxFlowAgeTests(unittest.TestCase):
    def setUp(self):
        self.doc = {"flow": {"started_at": "2026-11-20T00:00:00.000Z"}}
        self.start = 1795132800

    def test_below_cutoff(self):
        self.assertFalse(is_expired(self.doc, 60, self.start + 59.999))

    def test_at_cutoff(self):
        self.assertTrue(is_expired(self.doc, 60, self.start + 60))

    def test_above_cutoff(self):
        self.assertTrue(is_expired(self.doc, 60, self.start + 61))

    def test_future_start(self):
        self.assertFalse(is_expired(self.doc, 60, self.start - 1))

    def test_invalid_config(self):
        with self.assertRaises(ValueError):
            is_expired(self.doc, 0, self.start)


if __name__ == "__main__":
    unittest.main()
