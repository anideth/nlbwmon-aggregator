#!/usr/bin/env python3
"""Standalone lifecycle TCP collector with durable SQLite outbox.

Runs alongside legacy aggregator; never forwards to the legacy aggregate path.
One SQLite transaction per received line, then ACK by socket close is NOT an
end-to-end delivery guarantee (the exporter currently has no application ACK).
"""
import argparse
import json
import logging
import os
import signal
import socketserver
import sqlite3
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError

from flow_lifecycle import parse_lifecycle
from flow_opensearch import validate_index_prefix
from flow_writer import make_opener, read_secret, send_bulk_updates, send_dns_update
from dns_resolver import DNSResolver

LOG = logging.getLogger("flow-collector")


def is_expired(doc, max_flow_age, now=None):
    """Expire by flow start, not ingress time; do not restore retained-out indexes."""
    if max_flow_age <= 0:
        raise ValueError("max_flow_age must be positive")
    started = datetime.fromisoformat(doc["flow"]["started_at"].replace("Z", "+00:00"))
    if started.tzinfo is None:
        raise ValueError("started_at must have timezone")
    current = time.time() if now is None else now
    return current - started.timestamp() >= max_flow_age



def connect_db(path):
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.execute("PRAGMA busy_timeout=10000")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        router_name TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        received_at INTEGER NOT NULL
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS events_identity ON events(router_name, connection_id, event_type)")
    db.execute("CREATE TABLE IF NOT EXISTS dns_jobs (router_name TEXT NOT NULL, connection_id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(router_name,connection_id))")
    columns = {row[1] for row in db.execute("PRAGMA table_info(dns_jobs)")}
    if "next_attempt" not in columns:
        db.execute("ALTER TABLE dns_jobs ADD COLUMN next_attempt REAL NOT NULL DEFAULT 0")
    return db


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        db = connect_db(self.server.db_path)
        try:
            while True:
                line = self.rfile.readline(65537)
                if not line:
                    return
                if len(line) > 65536:
                    LOG.warning("oversized event discarded")
                    if not line.endswith(b"\n"):
                        while True:
                            tail = self.rfile.readline(65537)
                            if not tail or tail.endswith(b"\n"):
                                break
                    continue
                doc = parse_lifecycle(line.decode("utf-8", errors="replace"))
                if doc is None:
                    LOG.warning("invalid lifecycle event discarded")
                    continue
                try:
                    # Serialize capacity check and insert; never silently evict old events.
                    with self.server.write_lock:
                        db.execute("BEGIN IMMEDIATE")
                        count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                        if count >= self.server.max_events:
                            db.execute("ROLLBACK")
                            LOG.error("queue full; disconnecting sender (event may be lost)")
                            return
                        db.execute("INSERT INTO events(router_name, connection_id, event_type, payload, received_at) VALUES(?,?,?,?,?)",
                                   (doc["router_name"], doc["connection_id"], doc["event_type"],
                                    json.dumps(doc, separators=(",", ":")), int(time.time())))
                        db.execute("COMMIT")
                except sqlite3.Error:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    LOG.exception("queue write failure; disconnecting sender")
                    return
        finally:
            db.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def worker(stop, args):
    """Ordered bulk delivery with durable, replay-safe SQLite outbox."""
    db = connect_db(args.db)
    opener = make_opener(args.url, args.ca_file, args.client_cert, args.client_key, args.insecure)
    password = read_secret(args.password_file) if args.password_file else os.environ.get("OPENSEARCH_PASSWORD")
    username = os.environ.get("OPENSEARCH_USER")
    api_key = read_secret(args.api_key_file) if args.api_key_file else None
    try:
        while not stop.is_set():
            rows = db.execute("SELECT id,payload FROM events ORDER BY id LIMIT ?", (args.bulk_size,)).fetchall()
            if not rows:
                stop.wait(0.5)
                continue
            try:
                docs = [json.loads(payload) for _, payload in rows]
                expired = [is_expired(doc, args.max_flow_age) for doc in docs]
                fresh = [doc for doc, old in zip(docs, expired) if not old]
                sent = (send_bulk_updates(args.url, fresh, username, password,
                                          args.timeout, opener, api_key, index_prefix=args.index_prefix)
                        if fresh else [])
                sent_iter = iter(sent)
                successes = [True if old else next(sent_iter, False) for old in expired]
                if any(expired):
                    LOG.warning("flow events discarded by max-flow-age: %d", sum(expired))
                # A failed item blocks all later items, even if OpenSearch accepted
                # them. Replaying later items is safe: the update script is monotonic
                # (closed never reopens) and repeated NEW/DESTROY is a noop.
                prefix = 0
                for success in successes:
                    if not success:
                        break
                    prefix += 1
                if prefix:
                    db.execute("BEGIN IMMEDIATE")
                    for (ident, payload), doc, old in zip(rows[:prefix], docs[:prefix], expired[:prefix]):
                        if not old and getattr(args, "dns", False):
                            db.execute("INSERT INTO dns_jobs(router_name,connection_id,payload) VALUES(?,?,?) ON CONFLICT(router_name,connection_id) DO UPDATE SET payload=excluded.payload",
                                       (doc["router_name"], doc["connection_id"], payload))
                        db.execute("DELETE FROM events WHERE id=?", (ident,))
                    db.execute("COMMIT")
                    LOG.info("bulk delivered=%d queued_from_id=%d", prefix, rows[0][0])
                if prefix != len(rows):
                    LOG.warning("bulk partial failure at id=%d; retaining ordered suffix", rows[prefix][0])
                    stop.wait(args.retry_delay)
            except HTTPError as exc:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                LOG.error("OpenSearch bulk HTTP %d; batch retained", exc.code)
                stop.wait(args.retry_delay)
            except (URLError, TimeoutError, OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                LOG.error("bulk delivery failure (%s); batch retained", type(exc).__name__)
                stop.wait(args.retry_delay)
    finally:
        db.close()


def dns_worker(stop, args):
    """Background PTR updates; DNS never blocks the event-delivery worker."""
    db = connect_db(args.db)
    resolver = DNSResolver(servers=getattr(args, 'dns_server', None), dns_timeout=getattr(args, 'dns_timeout', 1.5))
    opener = make_opener(args.url, args.ca_file, args.client_cert, args.client_key, args.insecure)
    password = read_secret(args.password_file) if args.password_file else os.environ.get("OPENSEARCH_PASSWORD")
    username = os.environ.get("OPENSEARCH_USER")
    api_key = read_secret(args.api_key_file) if args.api_key_file else None
    try:
        while not stop.is_set():
            row = db.execute("SELECT router_name,connection_id,payload FROM dns_jobs WHERE next_attempt <= ? ORDER BY next_attempt LIMIT 1", (time.time(),)).fetchone()
            if row is None:
                stop.wait(0.5)
                continue
            router, connection, payload = row
            doc = json.loads(payload)
            if is_expired(doc, args.max_flow_age):
                db.execute("DELETE FROM dns_jobs WHERE router_name=? AND connection_id=? AND payload=?",
                           (router, connection, payload))
                LOG.warning("expired PTR job discarded")
                continue
            names = {}
            ready = True
            for field in ("source", "destination"):
                ip = doc[field]["ip"]
                resolver.lookup(ip)
                with resolver.lock:
                    cached = resolver.cache.get(ip)
                    if cached is None or cached[1] <= time.monotonic():
                        ready = False
                    elif cached[0]:
                        names[field] = cached[0]
            if not ready:
                db.execute("UPDATE dns_jobs SET next_attempt=? WHERE router_name=? AND connection_id=? AND payload=?", (time.time() + 0.5, router, connection, payload))
                continue
            try:
                if names:
                    send_dns_update(args.url, doc, names, username, password,
                                    args.timeout, opener, api_key, index_prefix=args.index_prefix)
                db.execute("DELETE FROM dns_jobs WHERE router_name=? AND connection_id=? AND payload=?",
                           (router, connection, payload))
            except HTTPError as exc:
                if exc.code == 404:
                    db.execute("DELETE FROM dns_jobs WHERE router_name=? AND connection_id=? AND payload=?",
                               (router, connection, payload))
                else:
                    LOG.warning("PTR update HTTP %d; retrying", exc.code)
                    db.execute("UPDATE dns_jobs SET next_attempt=? WHERE router_name=? AND connection_id=? AND payload=?", (time.time() + args.retry_delay, router, connection, payload))
            except (URLError, TimeoutError, OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                LOG.warning("PTR update failed (%s); retrying", type(exc).__name__)
                stop.wait(args.retry_delay)
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--listen", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5517)
    ap.add_argument("--db", required=True)
    ap.add_argument("--max-events", type=int, default=100000)
    ap.add_argument("--bulk-size", type=int, default=100)
    ap.add_argument("--max-flow-age", type=int, default=2592000, help="maximum age in seconds since flow start; must be below index retention (default: 30 days)")
    ap.add_argument("--index-prefix", default="conntrack-flows", help="OpenSearch index namespace (default: conntrack-flows)")
    ap.add_argument("--url", required=True)
    ap.add_argument("--ca-file")
    ap.add_argument("--client-cert")
    ap.add_argument("--client-key")
    ap.add_argument("--insecure", action="store_true")
    ap.add_argument("--password-file")
    ap.add_argument("--api-key-file")
    ap.add_argument("--timeout", type=float, default=10)
    ap.add_argument("--retry-delay", type=float, default=5)
    ap.add_argument("--dns", action="store_true", help="enable asynchronous PTR enrichment")
    ap.add_argument("--dns-server", action="append", default=[], metavar="IP", help="PTR DNS server (repeat for fallback); defaults to system resolver")
    ap.add_argument("--dns-timeout", type=float, default=1.5, help="timeout per DNS server in seconds")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from urllib.parse import urlsplit
    u = urlsplit(args.url)
    if u.scheme not in ("http", "https") or not u.netloc or u.username is not None or u.password is not None or u.path not in ("", "/") or u.query or u.fragment:
        ap.error("invalid OpenSearch URL")
    if args.max_flow_age <= 0 or args.max_events <= 0 or not 1 <= args.bulk_size <= 1000 or args.timeout <= 0 or args.retry_delay <= 0 or args.dns_timeout <= 0:
        ap.error("invalid queue or retry parameters")
    try:
        validate_index_prefix(args.index_prefix)
    except ValueError as exc:
        ap.error(str(exc))
    import ipaddress
    for server in args.dns_server:
        try:
            ipaddress.ip_address(server)
        except ValueError:
            ap.error("--dns-server must be an IPv4 or IPv6 address")
    if args.password_file and os.environ.get("OPENSEARCH_PASSWORD") is not None:
        ap.error("password-file conflicts with OPENSEARCH_PASSWORD")
    if args.api_key_file and (args.password_file or os.environ.get("OPENSEARCH_USER") or os.environ.get("OPENSEARCH_PASSWORD")):
        ap.error("API key conflicts with Basic Auth")
    if bool(os.environ.get("OPENSEARCH_USER")) != bool(args.password_file or os.environ.get("OPENSEARCH_PASSWORD")) and not args.api_key_file:
        ap.error("Basic Auth requires both user and password")
    if u.scheme == "http" and (args.insecure or args.ca_file or args.client_cert or args.client_key):
        ap.error("TLS options require HTTPS")
    if args.client_key and not args.client_cert:
        ap.error("client key requires certificate")
    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    db = connect_db(args.db)
    db.close()
    if args.insecure:
        LOG.warning("TLS verification disabled")
    if u.scheme == "http":
        LOG.warning("HTTP transport is unencrypted")
    stop = threading.Event()
    def shutdown(signum, frame):
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()
    with Server((args.listen, args.port), Handler) as server:
        server.db_path = args.db
        server.max_events = args.max_events
        server.write_lock = threading.Lock()
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        thread = threading.Thread(target=worker, args=(stop, args), daemon=True)
        thread.start()
        dns_thread = None
        if args.dns:
            dns_thread = threading.Thread(target=dns_worker, args=(stop, args), daemon=True)
            dns_thread.start()
        LOG.info("lifecycle collector listening on %s:%d", args.listen, args.port)
        server.serve_forever(poll_interval=0.5)
        stop.set()
        thread.join(timeout=args.timeout + 2)
        if dns_thread is not None:
            dns_thread.join(timeout=args.timeout + 2)


if __name__ == "__main__":
    main()
