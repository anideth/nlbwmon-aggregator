"""Integration-style tests for collector SQLite queue and TCP ingestion (no OpenSearch needed)."""
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flow_collector import Handler, Server, connect_db, worker


def event(connection="test-1", kind="NEW"):
    return {
        "schema": "nlbwmon-flow-v1", "event_type": kind,
        "router_name": "test-router", "connection_id": connection,
        "family": 2, "protocol": 6, "orig_src": "192.0.2.10",
        "orig_dst": "198.51.100.20", "reply_src": "198.51.100.20",
        "reply_dst": "192.0.2.10", "started_sec": 1794600000,
        "finished_sec": 0 if kind == "NEW" else 1794600060,
        "orig_sport": 12345, "orig_dport": 443, "reply_sport": 443,
        "reply_dport": 12345, "orig_packets": 2, "orig_bytes": 200,
        "reply_packets": 3, "reply_bytes": 300
    }


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, "events.db")
        self.db = connect_db(self.path)
        self.addCleanup(self.db.close)

    def count(self):
        return self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def insert(self, name="test-1"):
        from flow_lifecycle import parse_lifecycle
        doc = parse_lifecycle(json.dumps(event(name)))
        self.db.execute(
            "INSERT INTO events(router_name,connection_id,event_type,payload,received_at) VALUES(?,?,?,?,?)",
            (doc["router_name"], doc["connection_id"], doc["event_type"], json.dumps(doc), int(time.time())))

    def server(self, max_events=100):
        srv = Server(("127.0.0.1", 0), Handler)
        srv.db_path = self.path
        srv.max_events = max_events
        srv.write_lock = threading.Lock()
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (srv.shutdown(), srv.server_close(), thread.join(timeout=2)))
        return srv

    def send(self, srv, record):
        with socket.create_connection(srv.server_address, timeout=2) as sock:
            sock.sendall((json.dumps(record) + "\n").encode())
            sock.shutdown(socket.SHUT_WR)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if self.count():
                break
            time.sleep(0.01)

    def test_tcp_event_persisted_across_db_reopen(self):
        srv = self.server()
        self.send(srv, event())
        self.assertEqual(self.count(), 1)
        with connect_db(self.path) as reopened:
            row = reopened.execute("SELECT payload FROM events").fetchone()
        self.assertEqual(json.loads(row[0])["flow"]["status"], "active")

    def test_queue_full_preserves_old_event(self):
        srv = self.server(max_events=1)
        self.send(srv, event("first"))
        self.send(srv, event("second"))
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.db.execute("SELECT connection_id FROM events").fetchone()[0], "first")

    def test_invalid_event_does_not_enter_queue(self):
        srv = self.server()
        self.send(srv, {"schema": "invalid"})
        self.assertEqual(self.count(), 0)

    def run_worker(self, sender, wait=0.2, dns=False, stop_after=None):
        args = SimpleNamespace(db=self.path, url="https://localhost:9200",
                               ca_file=None, client_cert=None, client_key=None,
                               insecure=False, password_file=None, api_key_file=None,
                               timeout=0.1, retry_delay=0.02, dns=dns,
                               bulk_size=100, max_dns_jobs=100000, index_prefix="conntrack-flows-test", max_flow_age=2592000)
        stop = threading.Event()
        with patch("flow_collector.make_opener", return_value=object()), patch(
                "flow_collector.send_bulk_updates", side_effect=sender):
            thread = threading.Thread(target=worker, args=(stop, args), daemon=True)
            thread.start()
            if stop_after is not None:
                self.assertTrue(stop_after.wait(timeout=2), "worker did not reach expected bulk call")
            else:
                time.sleep(wait)
            stop.set()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_ptr_cache_enriches_bulk_without_creating_connection_jobs(self):
        self.insert("first")
        self.insert("second")
        self.db.execute("INSERT INTO dns_cache(ip,hostname,expires_at) VALUES(?,?,?)",
                        ("198.51.100.20", "remote.example.test", time.time() + 3600))
        seen = []
        def sender(url, docs, *args, **kwargs):
            seen.extend(docs)
            return [True] * len(docs)
        with patch("flow_collector.DNSResolver") as resolver:
            resolver.return_value.cache = {}
            resolver.return_value.lock = threading.Lock()
            self.run_worker(sender, dns=True)
        self.assertEqual(len(seen), 2)
        self.assertTrue(all(d["destination"]["reverse_dns"] == "remote.example.test" for d in seen))
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0], 0)

    def test_failed_delivery_does_not_create_ptr_jobs(self):
        self.insert()
        with patch("flow_collector.DNSResolver") as resolver:
            resolver.return_value.cache = {}
            resolver.return_value.lock = threading.Lock()
            self.run_worker(OSError("offline"), wait=0.07, dns=True)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0], 0)

    def test_late_ptr_bulk_after_delivery(self):
        self.insert("late-ptr")
        delivered = threading.Event()
        followed = threading.Event()
        instances = []
        class FakeResolver:
            def __init__(self, **kwargs):
                self.cache = {}
                self.lock = threading.Lock()
                instances.append(self)
            def lookup(self, ip):
                return None
        def sender(url, docs, *args, **kwargs):
            self.assertNotIn("reverse_dns", docs[0]["destination"])
            delivered.set()
            return [True] * len(docs)
        def ptr_sender(url, updates, *args, **kwargs):
            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0][1], {"destination": "late.example.test"})
            followed.set()
            return [True]
        args = SimpleNamespace(db=self.path, url="https://localhost:9200",
                               ca_file=None, client_cert=None, client_key=None,
                               insecure=False, password_file=None, api_key_file=None,
                               timeout=0.1, retry_delay=0.02, dns=True,
                               bulk_size=100, index_prefix="conntrack-flows-test",
                               max_flow_age=2592000)
        stop = threading.Event()
        with patch("flow_collector.make_opener", return_value=object()), patch(
                "flow_collector.send_bulk_updates", side_effect=sender), patch(
                "flow_collector.send_bulk_dns_updates", side_effect=ptr_sender), patch(
                "flow_collector.DNSResolver", FakeResolver):
            thread = threading.Thread(target=worker, args=(stop, args), daemon=True)
            thread.start()
            try:
                self.assertTrue(delivered.wait(2))
                with instances[0].lock:
                    instances[0].cache["198.51.100.20"] = ("late.example.test", time.monotonic() + 60)
                self.assertTrue(followed.wait(2))
                self.assertEqual(self.count(), 0)
            finally:
                stop.set()
                thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_failed_delivery_survives_worker_restart(self):
        self.insert()
        self.run_worker(OSError("offline"), wait=0.07)
        self.assertEqual(self.count(), 1)
        self.run_worker(lambda url, docs, *args, **kwargs: [True] * len(docs), wait=0.08)
        self.assertEqual(self.count(), 0)

    def test_bulk_partial_failure_preserves_ordered_suffix(self):
        self.insert("first")
        self.insert("second")
        calls = []
        def sender(url, docs, *args, **kwargs):
            calls.append([doc["connection_id"] for doc in docs])
            self.assertEqual(kwargs["index_prefix"], "conntrack-flows-test")
            stop_after_first_bulk.set()
            return [True, False]
        stop_after_first_bulk = threading.Event()
        self.run_worker(sender, wait=0.07, stop_after=stop_after_first_bulk)
        self.assertEqual(calls[0], ["first", "second"])
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.db.execute("SELECT connection_id FROM events").fetchone()[0], "second")

    def test_bulk_retry_drains_queue(self):
        self.insert("first")
        self.insert("second")
        self.run_worker(lambda *args, **kwargs: [False, True], wait=0.07)
        self.assertEqual(self.count(), 2)
        self.run_worker(lambda url, docs, *args, **kwargs: [True] * len(docs), wait=0.08)
        self.assertEqual(self.count(), 0)


if __name__ == "__main__":
    unittest.main()
