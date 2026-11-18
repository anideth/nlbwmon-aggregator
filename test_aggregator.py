import ipaddress
import unittest
from aggregator import parse_input, Aggregator

NETS = [ipaddress.ip_network('192.168.1.0/24'), ipaddress.ip_network('10.0.0.0/24')]
WAN = {ipaddress.ip_address('203.0.113.2')}

LEGACY = '[1790000060] [DESTROY] ipv4 2 tcp 6 ESTABLISHED src=192.168.1.128 dst=198.51.100.20 sport=54321 dport=443 packets=8 bytes=160 src=198.51.100.20 dst=203.0.113.2 sport=443 dport=54321 packets=11 bytes=290'
JSONL = '{"schema":"nlbwmon-flow-v1","router_name":"nlbw-lab","event_type":"DESTROY","family":2,"protocol":6,"orig_src":"192.168.1.128","orig_dst":"10.0.0.16","orig_sport":54321,"orig_dport":5201,"reply_src":"10.0.0.16","reply_dst":"10.0.0.42","reply_sport":5201,"reply_dport":54321,"orig_packets":8,"orig_bytes":160,"reply_packets":11,"reply_bytes":290,"finished_sec":1790000060}'

class FormatTests(unittest.TestCase):
    def test_legacy(self):
        e = parse_input(LEGACY, NETS, WAN, 'home-router')
        self.assertEqual(e['router_name'], 'home-router')
        self.assertEqual(e['sent_bytes'], 160)
        self.assertEqual(e['received_bytes'], 290)

    def test_json_snat_lab(self):
        e = parse_input(JSONL, NETS, WAN, 'home-router')
        self.assertEqual(e['router_name'], 'nlbw-lab')
        self.assertEqual(e['remote_ip'], '10.0.0.16')
        self.assertEqual(e['sent_bytes'], 160)
        self.assertEqual(e['received_bytes'], 290)

    def test_new_ignored(self):
        self.assertIsNone(parse_input(JSONL.replace('"DESTROY"', '"NEW"'), NETS, WAN, 'home-router'))

    def test_router_name_validation(self):
        self.assertIsNone(parse_input(JSONL.replace('nlbw-lab', 'bad"router'), NETS, WAN, 'home-router'))

    def test_group_separation(self):
        agg = Aggregator(NETS, WAN, ('127.0.0.1', 5516), 60, 100)
        agg.ingest(LEGACY)
        agg.ingest(JSONL)
        self.assertEqual(len(agg.groups), 2)
        self.assertEqual({k[0] for k in agg.groups}, {'home-router', 'nlbw-lab'})

if __name__ == '__main__':
    unittest.main()
