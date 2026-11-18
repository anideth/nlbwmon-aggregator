"""Regression tests for mixed input and exporter lifecycle semantics."""
import ipaddress
import json
import unittest
from unittest.mock import patch

from aggregator import Aggregator, parse_input

NETS = [ipaddress.ip_network("192.0.2.0/24"), ipaddress.ip_network("10.0.0.0/24")]
WAN = {ipaddress.ip_address("203.0.113.10")}

def flow(**changes):
    obj = {
        "schema": "nlbwmon-flow-v1", "router_name": "lab", "event_type": "DESTROY",
        "connection_id": "boot-123-456", "family": 2, "protocol": 6,
        "orig_src": "192.0.2.12", "orig_dst": "198.51.100.20",
        "reply_src": "198.51.100.20", "reply_dst": "203.0.113.10",
        "orig_sport": 54321, "orig_dport": 443,
        "reply_sport": 443, "reply_dport": 54321,
        "orig_packets": 8, "orig_bytes": 200, "reply_packets": 11,
        "reply_bytes": 300, "finished_sec": 1790000060
    }
    obj.update(changes)
    return json.dumps(obj)

class IntegrationRegressionTests(unittest.TestCase):
    def parse(self, line):
        return parse_input(line, NETS, WAN, "legacy")

    def test_new_is_metadata(self):
        self.assertIsNone(self.parse(flow(event_type="NEW")))

    def test_destroy_totals_not_deltas(self):
        event = self.parse(flow())
        self.assertEqual((event["sent_bytes"], event["received_bytes"]), (200, 300))

    def test_bad_family_and_ports(self):
        self.assertIsNone(self.parse(flow(family=10)))
        self.assertIsNone(self.parse(flow(orig_dport=70000)))
        self.assertIsNone(self.parse(flow(reply_sport=-1)))

    def test_duplicate_destroy_not_counted_twice(self):
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100)
        agg.ingest(flow())
        agg.ingest(flow())
        self.assertEqual(sum(g["connections"] for g in agg.groups.values()), 1)
        self.assertEqual(agg.duplicates, 1)

    def test_same_connection_id_different_router(self):
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100)
        agg.ingest(flow())
        agg.ingest(flow(router_name="other"))
        self.assertEqual(len(agg.groups), 2)

    def test_bounded_dedup_eviction(self):
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100, dedup_limit=1)
        agg.ingest(flow(connection_id="a"))
        agg.ingest(flow(connection_id="b"))
        self.assertEqual(len(agg.seen_connections), 1)
        agg.ingest(flow(connection_id="a"))
        self.assertEqual(sum(g["connections"] for g in agg.groups.values()), 3)

    def test_non_unicast_destinations_rejected(self):
        for destination in ("255.255.255.255", "192.0.2.255", "224.0.0.251", "0.0.0.0"):
            with self.subTest(destination=destination):
                self.assertIsNone(self.parse(flow(orig_dst=destination)))

    def test_router_filter_is_opt_in(self):
        router_ips = {"lab": {ipaddress.ip_address("192.0.2.12")}}
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100,
                         router_ips=router_ips)
        agg.ingest(flow())
        self.assertEqual(sum(g["connections"] for g in agg.groups.values()), 1)

    def test_router_filter_is_scoped_by_router_name(self):
        router_ips = {"lab": {ipaddress.ip_address("192.0.2.12")}}
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100,
                         exclude_router_traffic=True, router_ips=router_ips)
        agg.ingest(flow())
        agg.ingest(flow(router_name="other"))
        self.assertEqual(sum(g["connections"] for g in agg.groups.values()), 1)
        self.assertEqual(next(iter(agg.groups))[0], "other")

    def test_router_filter_preserves_other_clients(self):
        router_ips = {"lab": {ipaddress.ip_address("192.0.2.1")}}
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100,
                         exclude_router_traffic=True, router_ips=router_ips)
        agg.ingest(flow())
        self.assertEqual(sum(g["connections"] for g in agg.groups.values()), 1)

    def test_ptr_enrichment(self):
        class Resolver:
            def lookup(self, ip):
                return "client.example" if ip == "192.0.2.12" else None
        agg = Aggregator(NETS, WAN, ("127.0.0.1", 5516), 60, 100, resolver=Resolver())
        agg.ingest(flow())
        sent = []
        class Socket:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def settimeout(self, timeout): pass
            def sendall(self, data): sent.append(data)
        with patch("aggregator.socket.create_connection", return_value=Socket()):
            agg.flush()
        doc = json.loads(sent[0].splitlines()[0])
        self.assertEqual(doc["source"]["reverse_dns"], "client.example")

if __name__ == "__main__":
    unittest.main()
