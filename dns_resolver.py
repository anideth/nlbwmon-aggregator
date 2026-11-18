"""Non-blocking, cached PTR lookups for conntrack enrichment."""
import logging
import queue
import socket
import ipaddress
import secrets
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

LOG = logging.getLogger('conntrack-aggregator.dns')



def ptr_query(ip, servers, timeout=1.5):
    """Query configured DNS servers in order, without changing system DNS."""
    reverse = ipaddress.ip_address(ip).reverse_pointer
    question = b"".join(bytes([len(label)]) + label.encode("ascii") for label in reverse.split(".")) + b"\x00" + struct.pack("!HH", 12, 1)

    def read_name(packet, pos):
        labels, visited, end = [], set(), None
        while True:
            if pos >= len(packet) or pos in visited:
                raise ValueError("invalid DNS name")
            visited.add(pos)
            length = packet[pos]
            if length & 0xC0 == 0xC0:
                if pos + 1 >= len(packet):
                    raise ValueError("truncated DNS pointer")
                if end is None:
                    end = pos + 2
                pos = ((length & 0x3F) << 8) | packet[pos + 1]
                continue
            if length & 0xC0:
                raise ValueError("invalid DNS label")
            pos += 1
            if length == 0:
                return ".".join(labels), (end if end is not None else pos)
            if pos + length > len(packet):
                raise ValueError("truncated DNS label")
            labels.append(packet[pos:pos + length].decode("ascii"))
            pos += length

    for server in servers:
        try:
            server = str(ipaddress.ip_address(server))
            family = socket.AF_INET6 if ":" in server else socket.AF_INET
            txid = secrets.randbelow(65536)
            request = struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0) + question
            with socket.socket(family, socket.SOCK_DGRAM) as sock:
                sock.settimeout(timeout)
                sock.connect((server, 53))
                sock.send(request)
                packet = sock.recv(4096)
            if len(packet) < 12:
                continue
            ident, flags, qd, an, _, _ = struct.unpack_from("!HHHHHH", packet)
            if ident != txid or not (flags & 0x8000) or flags & 0x0200 or (flags & 0x000F) or qd != 1:
                continue
            name, pos = read_name(packet, 12)
            if name.lower() != reverse.lower() or pos + 4 > len(packet) or struct.unpack_from("!HH", packet, pos) != (12, 1):
                continue
            pos += 4
            for _ in range(an):
                _, pos = read_name(packet, pos)
                if pos + 10 > len(packet):
                    raise ValueError("truncated DNS answer")
                kind, cls, _, size = struct.unpack_from("!HHIH", packet, pos)
                pos += 10
                if pos + size > len(packet):
                    raise ValueError("truncated DNS data")
                if kind == 12 and cls == 1:
                    hostname, end = read_name(packet, pos)
                    if end <= pos + size and hostname:
                        return hostname.rstrip(".")
                pos += size
        except (OSError, ValueError, UnicodeError, struct.error) as exc:
            LOG.debug("PTR query via %s for %s failed: %s", server, ip, exc)
    return None


class DNSResolver:
    def __init__(self, workers=3, cache_ttl=86400, negative_ttl=3600, max_cache=50000, max_pending=2000, servers=None, dns_timeout=1.5):
        self.servers = tuple(servers or ())
        self.dns_timeout = dns_timeout
        self.cache_ttl = cache_ttl
        self.negative_ttl = negative_ttl
        self.max_cache = max_cache
        self.cache = {}
        self.pending = set()
        self.lock = threading.Lock()
        self.queue = queue.Queue(maxsize=max_pending)
        self.workers = []
        for n in range(workers):
            thread = threading.Thread(target=self._worker, name=f'ptr-{n}', daemon=True)
            thread.start()
            self.workers.append(thread)

    def lookup(self, ip):
        """Return cached PTR or None; schedule unseen/expired IP without blocking."""
        now = time.monotonic()
        with self.lock:
            item = self.cache.get(ip)
            if item is not None and item[1] > now:
                return item[0]
            if ip in self.pending:
                return None
            self.pending.add(ip)
            try:
                self.queue.put_nowait(ip)
            except queue.Full:
                self.pending.discard(ip)
        return None

    def _worker(self):
        while True:
            ip = self.queue.get()
            name = None
            try:
                name = (ptr_query(ip, self.servers, self.dns_timeout) if self.servers else socket.gethostbyaddr(ip)[0].rstrip('.')) or None
            except (OSError, UnicodeError) as exc:
                LOG.debug('PTR lookup failed for %s: %s', ip, exc)
            finally:
                now = time.monotonic()
                with self.lock:
                    if len(self.cache) >= self.max_cache:
                        expired = [k for k, (_, until) in self.cache.items() if until <= now]
                        for key in expired:
                            self.cache.pop(key, None)
                        if len(self.cache) >= self.max_cache:
                            self.cache.pop(next(iter(self.cache)))
                    self.cache[ip] = (name, now + (self.cache_ttl if name else self.negative_ttl))
                    self.pending.discard(ip)
                self.queue.task_done()