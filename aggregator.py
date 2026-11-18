#!/usr/bin/env python3
"""Aggregate OpenWrt conntrack DESTROY events and forward JSON lines to Fluent Bit."""
import argparse
import ipaddress
import json
import logging
import re
import signal
import socket
import socketserver
import threading
import time
from collections import defaultdict, OrderedDict
from dns_resolver import DNSResolver
from datetime import datetime, timezone

LOG = logging.getLogger('conntrack-aggregator')
EVENT = re.compile(r'^\[(?P<epoch>\d+(?:\.\d+)?)\]\s+\[DESTROY\]\s+ipv[46]\s+\d+\s+(?P<proto>tcp|udp)\s+\d+\s+(?:(?P<state>[A-Z_]+)\s+)?(?P<rest>.*)$')
FIELDS = re.compile(r'\b(src|dst|sport|dport|packets|bytes)=([^\s]+)')


def parse(line, local_networks, wan_addresses):
    m = EVENT.match(line.strip())
    if not m:
        return None
    matches = list(FIELDS.finditer(m['rest']))
    tuples = []
    current = {}
    for match in matches:
        key, value = match.groups()
        if key == 'src' and 'src' in current:
            tuples.append(current)
            current = {}
        current[key] = value
    if current:
        tuples.append(current)
    if len(tuples) != 2 or any(not {'src','dst','sport','dport'}.issubset(t) for t in tuples):
        return None
    orig, reply = tuples
    try:
        o_src, o_dst = ipaddress.ip_address(orig['src']), ipaddress.ip_address(orig['dst'])
        r_src, r_dst = ipaddress.ip_address(reply['src']), ipaddress.ip_address(reply['dst'])
        def is_local(addr):
            return any(addr in network for network in local_networks)
        # Only forwarded traffic. DNAT: external -> WAN, reply local -> external.
        if o_dst in wan_addresses and is_local(r_src) and not is_local(o_src):
            direction, local, remote = 'inbound', r_src, o_src
            remote_port = int(orig['dport'])  # published service port for inbound grouping
            local_port = int(reply['sport'])
            sent, received = reply, orig
        # Outbound SNAT: local -> external, reply external -> WAN.
        elif is_local(o_src) and not is_local(o_dst) and r_dst in wan_addresses:
            direction, local, remote = 'outbound', o_src, o_dst
            remote_port = int(orig['dport'])
            local_port = int(orig['sport'])
            sent, received = orig, reply
        else:
            return None
        # DNS by service port, not ephemeral remote port in inbound connections.
        if (direction == 'outbound' and remote_port in (53, 853)) or (direction == 'inbound' and local_port in (53, 853)):
            return None
        def metric(t, key):
            return int(t.get(key, 0))
        return {
            'timestamp': float(m['epoch']), 'local_ip': str(local), 'remote_ip': str(remote),
            'remote_port': remote_port, 'local_port': local_port,
            'protocol': m['proto'], 'direction': direction,
            'unreplied': '[UNREPLIED]' in m['rest'],
            'assured': '[ASSURED]' in m['rest'],
            'state': m['state'] or 'UNKNOWN',
            'sent_packets': metric(sent, 'packets'), 'received_packets': metric(received, 'packets'),
            'sent_bytes': metric(sent, 'bytes'), 'received_bytes': metric(received, 'bytes'),
        }
    except (ValueError, KeyError):
        return None



def is_non_unicast_destination(addr, local_networks):
    """Reject broadcast/multicast destinations from WAN-oriented JSONL telemetry."""
    if addr.is_multicast or addr.is_unspecified:
        return True
    if addr.version == 4:
        if addr == ipaddress.IPv4Address('255.255.255.255'):
            return True
        return any(network.version == 4 and network.prefixlen < 31 and
                   addr == network.broadcast_address for network in local_networks)
    return False


def parse_nlbwmon(line, local_networks, wan_addresses):
    """Parse nlbwmon-flow-v1 DESTROY events; NEW is lifecycle metadata only."""
    try:
        obj = json.loads(line)
        if not isinstance(obj, dict) or obj.get('schema') != 'nlbwmon-flow-v1':
            return None
        if obj.get('event_type') != 'DESTROY':
            return None
        router_name = obj.get('router_name', 'unknown')
        if not isinstance(router_name, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', router_name):
            return None
        protocol = {6: 'tcp', 17: 'udp'}.get(int(obj['protocol']))
        if protocol is None:
            return None
        orig_src = ipaddress.ip_address(obj['orig_src'])
        orig_dst = ipaddress.ip_address(obj['orig_dst'])
        reply_src = ipaddress.ip_address(obj['reply_src'])
        reply_dst = ipaddress.ip_address(obj['reply_dst'])
        family = int(obj['family'])
        if family not in (2, 10) or any(addr.version != (4 if family == 2 else 6) for addr in (orig_src, orig_dst, reply_src, reply_dst)):
            return None
        for name in ('orig_sport', 'orig_dport', 'reply_sport', 'reply_dport'):
            if not 0 <= int(obj[name]) <= 65535:
                return None
        if is_non_unicast_destination(orig_dst, local_networks):
            return None
        local = lambda addr: any(addr in network for network in local_networks)
        if orig_dst in wan_addresses and local(reply_src) and not local(orig_src):
            direction, local_ip, remote_ip = 'inbound', reply_src, orig_src
            remote_port, local_port = int(obj['orig_dport']), int(obj['reply_sport'])
            sent, received = 'reply', 'orig'
        elif local(orig_src) and (not local(orig_dst) or reply_dst != orig_src):
            # SNAT also covers lab-to-lab forwarded traffic translated by the router.
            direction, local_ip, remote_ip = 'outbound', orig_src, orig_dst
            remote_port, local_port = int(obj['orig_dport']), int(obj['orig_sport'])
            sent, received = 'orig', 'reply'
        else:
            return None
        if (direction == 'outbound' and remote_port in (53, 853)) or (direction == 'inbound' and local_port in (53, 853)):
            return None
        timestamp = float(obj['finished_sec'])
        if not (0 < timestamp < 253402300800):
            return None
        values = {name: int(obj[name]) for name in ('orig_packets', 'reply_packets', 'orig_bytes', 'reply_bytes')}
        if any(value < 0 for value in values.values()):
            return None
        connection_id = obj.get('connection_id')
        if connection_id is not None and (not isinstance(connection_id, str) or not 1 <= len(connection_id) <= 256):
            return None
        return {
            'connection_id': connection_id,
            'router_name': router_name, 'timestamp': timestamp,
            'local_ip': str(local_ip), 'remote_ip': str(remote_ip),
            'remote_port': remote_port, 'local_port': local_port,
            'protocol': protocol, 'direction': direction,
            'unreplied': False, 'assured': False, 'state': 'UNKNOWN',
            'sent_packets': values[sent + '_packets'], 'received_packets': values[received + '_packets'],
            'sent_bytes': values[sent + '_bytes'], 'received_bytes': values[received + '_bytes'],
        }
    except (ValueError, TypeError, KeyError, OverflowError):
        return None


def parse_input(line, local_networks, wan_addresses, legacy_router_name):
    if line.lstrip().startswith('{'):
        return parse_nlbwmon(line, local_networks, wan_addresses)
    event = parse(line, local_networks, wan_addresses)
    if event is not None:
        event['router_name'] = legacy_router_name
    return event


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class Aggregator:
    def __init__(self, networks, wan, target, interval, max_groups, legacy_router_name='home-router', resolver=None, dedup_limit=100000, exclude_router_traffic=False, router_ips=None):
        self.legacy_router_name = legacy_router_name
        self.exclude_router_traffic = exclude_router_traffic
        self.router_ips = router_ips if router_ips is not None else {}
        self.resolver = resolver
        self.dedup_limit = dedup_limit
        self.seen_connections = OrderedDict()
        self.duplicates = 0
        self.networks = networks
        self.wan = wan
        self.target = target
        self.interval = interval
        self.max_groups = max_groups
        self.lock = threading.Lock()
        self.groups = {}
        self.dropped = 0
        self.parsed = 0

    def ingest(self, line):
        event = parse_input(line, self.networks, self.wan, self.legacy_router_name)
        if not event:
            return
        if self.exclude_router_traffic and ipaddress.ip_address(event['local_ip']) in self.router_ips.get(event['router_name'], set()):
            return
        key = (event['router_name'], event['local_ip'], event['remote_ip'], event['protocol'], event['remote_port'], event['direction'])
        with self.lock:
            connection_id = event.get('connection_id')
            dedup_key = (event['router_name'], connection_id) if connection_id else None
            if dedup_key is not None and dedup_key in self.seen_connections:
                self.duplicates += 1
                return
            if key not in self.groups:
                if len(self.groups) >= self.max_groups:
                    self.dropped += 1
                    return
                self.groups[key] = {'connections': 0, 'unreplied': 0, 'assured': 0,
                                    'sent_packets': 0, 'received_packets': 0,
                                    'sent_bytes': 0, 'received_bytes': 0,
                                    'first': event['timestamp'], 'last': event['timestamp'],
                                    'states': defaultdict(int)}
            if dedup_key is not None and self.dedup_limit > 0:
                self.seen_connections[dedup_key] = None
                if len(self.seen_connections) > self.dedup_limit:
                    self.seen_connections.popitem(last=False)
            g = self.groups[key]
            g['connections'] += 1
            g['unreplied'] += int(event['unreplied'])
            g['assured'] += int(event['assured'])
            for field in ('sent_packets', 'received_packets', 'sent_bytes', 'received_bytes'):
                g[field] += event[field]
            g['first'] = min(g['first'], event['timestamp'])
            g['last'] = max(g['last'], event['timestamp'])
            g['states'][event['state']] += 1
            self.parsed += 1

    def flush(self):
        with self.lock:
            groups, self.groups = self.groups, {}
            dropped, self.dropped = self.dropped, 0
        if not groups:
            return
        lines = []
        for (router_name, local, remote, protocol, port, direction), g in groups.items():
            doc = {
                '@timestamp': iso(g['last']),
                'router_name': router_name,
                'window_seconds': self.interval,
                'first_seen': iso(g['first']), 'last_seen': iso(g['last']),
                'source': {'ip': local, 'bytes': g['sent_bytes'], 'packets': g['sent_packets']},
                'destination': {'ip': remote, 'port': port, 'bytes': g['received_bytes'], 'packets': g['received_packets']},
                'network': {'transport': protocol, 'direction': direction},
                'connections': g['connections'],
                'unreplied': g['unreplied'], 'assured': g['assured'],
                'conntrack': {'states': dict(g['states'])},
            }
            if self.resolver is not None:
                for field, ip in (('source', local), ('destination', remote)):
                    name = self.resolver.lookup(ip)
                    if name:
                        doc[field]['reverse_dns'] = name
            lines.append(json.dumps(doc, ensure_ascii=False, separators=(',', ':')).encode() + b'\n')
        # In case of failure, retain pending batch in memory for next retry.
        payload = b''.join(lines)
        try:
            with socket.create_connection(self.target, timeout=5) as sock:
                sock.settimeout(10)
                sock.sendall(payload)
            LOG.info('sent %d groups (%d bytes); dropped=%d', len(groups), len(payload), dropped)
        except OSError as exc:
            LOG.error('Fluent Bit send failed (%s); retry next interval', exc)
            with self.lock:
                # Avoid losing aggregate data on transient Fluent Bit outages.
                for key, g in groups.items():
                    if key not in self.groups:
                        self.groups[key] = g
                    else:
                        dst = self.groups[key]
                        for f in ('connections','unreplied','assured','sent_packets','received_packets','sent_bytes','received_bytes'):
                            dst[f] += g[f]
                        dst['first'] = min(dst['first'], g['first'])
                        dst['last'] = max(dst['last'], g['last'])
                        for state, count in g['states'].items():
                            dst['states'][state] += count
                self.dropped += dropped


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        LOG.info('client connected: %s', self.client_address[0])
        while True:
            line = self.rfile.readline(65537)
            if not line:
                break
            if len(line) > 65536:
                LOG.warning('oversized input line ignored')
                if not line.endswith(b'\n'):
                    while True:
                        remainder = self.rfile.readline(65537)
                        if not remainder or remainder.endswith(b'\n'):
                            break
                continue
            self.server.aggregator.ingest(line.decode('utf-8', errors='replace'))
        LOG.info('client disconnected: %s', self.client_address[0])


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--listen', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=5515)
    ap.add_argument('--fluent-host', default='127.0.0.1')
    ap.add_argument('--fluent-port', type=int, default=5516)
    ap.add_argument('--local-networks', required=True, help='Comma-separated LAN/VLAN CIDRs')
    ap.add_argument('--wan-addresses', required=True, help='Comma-separated WAN public IPs')
    ap.add_argument('--legacy-router-name', default='home-router', help='Name for existing text conntrack senders')
    ap.add_argument('--interval', type=int, default=60)
    ap.add_argument('--max-groups', type=int, default=10000)
    ap.add_argument('--no-dns', action='store_true', help='Disable asynchronous PTR lookups')
    ap.add_argument('--dedup-limit', type=int, default=100000, help='Maximum remembered JSONL connection IDs (RAM only)')
    ap.add_argument('--exclude-router-traffic', action='store_true', help='Exclude router-owned flows (opt-in)')
    ap.add_argument('--router-ip', action='append', default=[], metavar='ROUTER=IP', help='Router interface IP for a specific router_name; repeatable')
    ap.add_argument('--test-line', action='store_true', help='Read conntrack events from stdin, print parsed records')
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    networks = [ipaddress.ip_network(x.strip()) for x in args.local_networks.split(',') if x.strip()]
    wan = {ipaddress.ip_address(x.strip()) for x in args.wan_addresses.split(',') if x.strip()}
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', args.legacy_router_name):
        ap.error('invalid --legacy-router-name')
    if args.dedup_limit < 0:
        ap.error('--dedup-limit must be nonnegative')
    router_ips = {}
    for entry in args.router_ip:
        name, sep, address = entry.partition('=')
        if not sep or not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', name):
            ap.error('--router-ip must be ROUTER=IP with a valid router name')
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            ap.error('--router-ip requires a valid IP address')
        router_ips.setdefault(name, set()).add(ip)
    if args.exclude_router_traffic and not router_ips:
        ap.error('--exclude-router-traffic requires at least one --router-ip')
    if args.test_line:
        import sys
        for line in sys.stdin:
            event = parse_input(line, networks, wan, args.legacy_router_name)
            if event:
                print(json.dumps(event, ensure_ascii=False))
        return
    resolver = None if args.no_dns else DNSResolver()
    aggregator = Aggregator(networks, wan, (args.fluent_host, args.fluent_port), args.interval, args.max_groups, args.legacy_router_name, resolver, args.dedup_limit, args.exclude_router_traffic, router_ips)
    stop = threading.Event()
    def stop_handler(signum, frame):
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()
    with Server((args.listen, args.port), Handler) as server:
        server.aggregator = aggregator
        signal.signal(signal.SIGTERM, stop_handler)
        signal.signal(signal.SIGINT, stop_handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        LOG.info('listening on %s:%d -> %s:%d', args.listen, args.port, args.fluent_host, args.fluent_port)
        while not stop.wait(args.interval):
            aggregator.flush()
        aggregator.flush()
        server.shutdown()
        thread.join(timeout=3)

if __name__ == '__main__':
    main()