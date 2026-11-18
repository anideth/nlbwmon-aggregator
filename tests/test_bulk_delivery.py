import json
import unittest
from unittest.mock import Mock

from flow_writer import send_bulk_updates


def doc(event):
    return {
        "event_type": event,
        "router_name": "lab",
        "connection_id": "conn",
        "flow": {"started_at": "2026-11-16T20:00:00Z",
                 "status": "active" if event == "NEW" else "closed"},
    }


class BulkTests(unittest.TestCase):
    def test_ordered_ndjson_and_partial_failure(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read = Mock(return_value=json.dumps({"items": [
            {"update": {"status": 200, "result": "created"}},
            {"update": {"status": 429, "error": {"type": "rejected"}}},
        ]}).encode())
        opener = Mock()
        opener.open.return_value = response
        self.assertEqual(send_bulk_updates("https://example.test:9200",
                         [doc("NEW"), doc("DESTROY")], opener=opener), [True, False])
        request = opener.open.call_args.args[0]
        self.assertEqual(request.full_url, "https://example.test:9200/_bulk")
        lines = request.data.decode().splitlines()
        self.assertEqual(len(lines), 4)
        self.assertEqual(json.loads(lines[1])["script"]["params"]["doc"]["event_type"], "NEW")
        self.assertEqual(json.loads(lines[3])["script"]["params"]["doc"]["event_type"], "DESTROY")

    def test_malformed_response_retains_batch(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read = Mock(return_value=b'{"items":[]}')
        opener = Mock()
        opener.open.return_value = response
        with self.assertRaises(ValueError):
            send_bulk_updates("https://example.test", [doc("NEW")], opener=opener)


if __name__ == "__main__":
    unittest.main()
