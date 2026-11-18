"""Tests for the isolated OpenSearch lifecycle request builder."""
import unittest
from flow_opensearch import document_address, update_request, validate_index_prefix


def sample(kind="NEW"):
    doc = {
        "router_name": "lab-router",
        "connection_id": "boot-id:42",
        "event_type": kind,
        "flow": {
            "status": "active" if kind == "NEW" else "closed",
            "started_at": "2026-11-13T23:59:58.000Z",
        },
    }
    if kind == "DESTROY":
        doc["flow"]["ended_at"] = "2026-11-14T00:00:03.000Z"
    return doc


class OpenSearchRequestTests(unittest.TestCase):
    def test_new_and_destroy_share_identity_across_midnight(self):
        self.assertEqual(document_address(sample("NEW")), document_address(sample("DESTROY")))
        self.assertEqual(document_address(sample())[0], "conntrack-flows-2026.11.13")

    def test_different_routers_have_different_ids(self):
        other = sample()
        other["router_name"] = "other-router"
        self.assertNotEqual(document_address(sample())[1], document_address(other)[1])

    def test_request_is_scripted_upsert(self):
        index, identifier, body = update_request(sample("DESTROY"))
        self.assertEqual(index, "conntrack-flows-2026.11.13")
        self.assertEqual(len(identifier), 64)
        self.assertTrue(body["scripted_upsert"])
        self.assertEqual(body["upsert"], {})
        self.assertEqual(body["script"]["params"]["doc"]["flow"]["status"], "closed")
        self.assertIn("ctx.op = 'none'", body["script"]["source"])

    def test_custom_prefix_keeps_id_and_day(self):
        default_index, identifier = document_address(sample())
        test_index, test_identifier = document_address(sample(), "conntrack-flows-test")
        self.assertEqual(test_index, "conntrack-flows-test-2026.11.13")
        self.assertEqual(test_identifier, identifier)
        self.assertEqual(update_request(sample(), "conntrack-flows-test")[0], test_index)

    def test_invalid_prefix_rejected(self):
        for value in ("", "../prod", "CONNTRACK", "-test", "test*", "a" * 64):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_index_prefix(value)

    def test_reject_invalid_event(self):
        bad = sample()
        bad["event_type"] = "UPDATE"
        with self.assertRaises(ValueError):
            update_request(bad)

    def test_reject_mismatched_status(self):
        bad = sample()
        bad["flow"]["status"] = "closed"
        with self.assertRaises(ValueError):
            update_request(bad)


if __name__ == "__main__":
    unittest.main()
