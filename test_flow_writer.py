"""Offline tests for lifecycle writer TLS and authentication."""
import base64
import io
import json
import ssl
import tempfile
import unittest
from unittest.mock import patch

from flow_writer import make_opener, read_secret, send_update, send_dns_update, send_bulk_updates


class FakeResponse:
    def __init__(self, result):
        self.payload = io.BytesIO(json.dumps({"result": result}).encode())

    def __enter__(self):
        return self.payload

    def __exit__(self, *args):
        self.payload.close()


class FakeOpener:
    def __init__(self, result="created"):
        self.result = result
        self.request = None

    def open(self, request, timeout=None):
        self.request = request
        return FakeResponse(self.result)


class WriterTests(unittest.TestCase):
    def test_dns_patch_only_contains_reverse_dns(self):
        opener = FakeOpener("updated")
        doc = {"router_name": "lab", "connection_id": "id",
               "flow": {"started_at": "2026-11-14T00:00:00Z"}}
        result = send_dns_update("https://localhost:9200", doc,
                                 {"destination": "example.test"}, opener=opener)
        self.assertEqual(result, "updated")
        body = json.loads(opener.request.data)
        self.assertEqual(body["script"]["params"]["names"], {"destination": "example.test"})
        self.assertNotIn("upsert", body)
        self.assertIn("/_update/", opener.request.full_url)

    def test_dns_patch_rejects_unexpected_fields(self):
        with self.assertRaises(ValueError):
            send_dns_update("https://localhost:9200", {}, {"flow": "bad"})


    def test_http_rejects_tls_options(self):
        with self.assertRaises(ValueError):
            make_opener("http://localhost:9200", insecure=True)

    def test_https_default_verifies_certificates(self):
        with patch("flow_writer.ssl.create_default_context", return_value=ssl.create_default_context()) as create:
            make_opener("https://localhost:9200", ca_file="/tmp/ca.pem")
            create.assert_called_once_with(cafile="/tmp/ca.pem")

    def test_https_insecure_requires_explicit_opt_in(self):
        with patch("flow_writer.ssl._create_unverified_context", return_value=ssl.create_default_context()) as create:
            make_opener("https://localhost:9200", insecure=True)
            create.assert_called_once()

    def test_client_key_requires_certificate(self):
        with self.assertRaises(ValueError):
            make_opener("https://localhost:9200", client_key="/tmp/key.pem")

    def test_password_file(self):
        with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8") as f:
            f.write("secret\n")
            f.flush()
            self.assertEqual(read_secret(f.name), "secret")

    def test_basic_auth_request(self):
        opener = FakeOpener()
        doc = {"router_name": "lab", "connection_id": "id", "event_type": "NEW",
               "flow": {"status": "active", "started_at": "2026-11-14T00:00:00Z"}}
        self.assertEqual(send_update("https://localhost:9200", doc, "user", "secret", opener=opener), "created")
        self.assertEqual(opener.request.get_header("Authorization"),
                         "Basic " + base64.b64encode(b"user:secret").decode())

    def test_api_key_request(self):
        opener = FakeOpener("noop")
        doc = {"router_name": "lab", "connection_id": "id", "event_type": "NEW",
               "flow": {"status": "active", "started_at": "2026-11-14T00:00:00Z"}}
        self.assertEqual(send_update("https://localhost:9200", doc, opener=opener, api_key="token"), "noop")
        self.assertEqual(opener.request.get_header("Authorization"), "ApiKey token")



class BulkResponse:
    def __init__(self, payload):
        self.payload = io.BytesIO(json.dumps(payload).encode())

    def __enter__(self):
        return self.payload

    def __exit__(self, *args):
        self.payload.close()


class BulkOpener:
    def __init__(self, payload):
        self.payload = payload
        self.request = None

    def open(self, request, timeout=None):
        self.request = request
        return BulkResponse(self.payload)


class BulkResponseTests(unittest.TestCase):
    def docs(self):
        return [
            {"router_name": "lab", "connection_id": str(i), "event_type": "NEW",
             "flow": {"status": "active", "started_at": "2026-11-14T00:00:00Z"}}
            for i in range(2)
        ]

    def send(self, items):
        opener = BulkOpener({"errors": True, "items": items})
        result = send_bulk_updates("https://localhost:9200", self.docs(),
                                   opener=opener, index_prefix="conntrack-flows-tests")
        self.assertTrue(opener.request.full_url.endswith("/_bulk"))
        self.assertEqual(len(opener.request.data.decode().splitlines()), 4)
        return result

    def test_all_successful_items(self):
        self.assertEqual(self.send([
            {"update": {"status": 201, "result": "created"}},
            {"update": {"status": 200, "result": "noop"}},
        ]), [True, True])

    def test_partial_rate_limit_failure(self):
        self.assertEqual(self.send([
            {"update": {"status": 200, "result": "updated"}},
            {"update": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}},
        ]), [True, False])

    def test_mapping_error_item(self):
        self.assertEqual(self.send([
            {"update": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
            {"update": {"status": 200, "result": "updated"}},
        ]), [False, True])

    def test_rejects_wrong_item_count(self):
        opener = BulkOpener({"items": [{"update": {"status": 200, "result": "updated"}}]})
        with self.assertRaisesRegex(ValueError, "invalid bulk response length"):
            send_bulk_updates("https://localhost:9200", self.docs(), opener=opener)

    def test_rejects_malformed_item(self):
        opener = BulkOpener({"items": [{"update": {"status": 200, "result": "updated"}}, {"index": {}}]})
        with self.assertRaisesRegex(ValueError, "invalid bulk update response"):
            send_bulk_updates("https://localhost:9200", self.docs(), opener=opener)

    def test_http_200_without_known_result_is_failure(self):
        self.assertEqual(self.send([
            {"update": {"status": 200}},
            {"update": {"status": 200, "result": "updated"}},
        ]), [False, True])

if __name__ == "__main__":
    unittest.main()
