#!/usr/bin/env python3
"""Read nlbwmon-flow-v1 JSONL from stdin and upsert lifecycle documents.

Independent from the production conntrack aggregator. No third-party packages.
Credentials can be supplied through OPENSEARCH_USER and OPENSEARCH_PASSWORD.
"""
import argparse
import base64
import json
import logging
import os
import ssl
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, build_opener, HTTPSHandler, HTTPHandler, ProxyHandler

from flow_lifecycle import parse_lifecycle
from flow_opensearch import update_request, document_address

LOG = logging.getLogger("flow-writer")


def make_opener(url, ca_file=None, client_cert=None, client_key=None, insecure=False):
    """Direct connection with explicit TLS policy, ignoring ambient proxies."""
    if urlsplit(url).scheme == "http":
        if ca_file or client_cert or client_key or insecure:
            raise ValueError("TLS options require HTTPS")
        return build_opener(ProxyHandler({}), HTTPHandler())
    if client_key and not client_cert:
        raise ValueError("--client-key requires --client-cert")
    context = ssl._create_unverified_context() if insecure else ssl.create_default_context(cafile=ca_file)
    if client_cert:
        context.load_cert_chain(client_cert, keyfile=client_key)
    return build_opener(ProxyHandler({}), HTTPSHandler(context=context))


def read_secret(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().rstrip("\r\n")


def send_update(base_url, doc, username=None, password=None, timeout=10, opener=None, api_key=None):
    index, identifier, body = update_request(doc)
    url = base_url.rstrip("/") + "/" + quote(index, safe="") + "/_update/" + quote(identifier, safe="")
    headers = {"Content-Type": "application/json"}
    if username is not None:
        token = base64.b64encode((username + ":" + (password or "")).encode("utf-8")).decode("ascii")
        headers["Authorization"] = "Basic " + token
    if api_key is not None:
        headers['Authorization'] = 'ApiKey ' + api_key
    request = Request(url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
    with opener.open(request, timeout=timeout) as response:
        result = json.load(response)
    if result.get("result") not in ("created", "updated", "noop"):
        raise ValueError("unexpected OpenSearch update result")
    return result["result"]



def send_bulk_updates(base_url, docs, username=None, password=None, timeout=10, opener=None, api_key=None, index_prefix="conntrack-flows"):
    """Send ordered lifecycle updates via Bulk API; return per-item success flags.

    The caller must retain and replay the entire batch on any uncertain outcome.
    Lifecycle scripts are idempotent and never regress a closed document to active.
    """
    if not docs:
        return []
    lines = []
    for doc in docs:
        index, identifier, body = update_request(doc, index_prefix)
        lines.append(json.dumps({"update": {"_index": index, "_id": identifier}}, separators=(",", ":")))
        lines.append(json.dumps(body, separators=(",", ":")))
    headers = {"Content-Type": "application/x-ndjson"}
    if username is not None:
        token = base64.b64encode((username + ":" + (password or "")).encode("utf-8")).decode("ascii")
        headers["Authorization"] = "Basic " + token
    if api_key is not None:
        headers["Authorization"] = "ApiKey " + api_key
    request = Request(base_url.rstrip("/") + "/_bulk", data=("\n".join(lines) + "\n").encode("utf-8"),
                      headers=headers, method="POST")
    with opener.open(request, timeout=timeout) as response:
        result = json.load(response)
    items = result.get("items")
    if not isinstance(items, list) or len(items) != len(docs):
        raise ValueError("invalid bulk response length")
    successes = []
    for item in items:
        update = item.get("update") if isinstance(item, dict) else None
        if not isinstance(update, dict):
            raise ValueError("invalid bulk update response")
        status = update.get("status")
        successes.append(isinstance(status, int) and 200 <= status < 300
                         and update.get("result") in ("created", "updated", "noop"))
    return successes



def send_bulk_dns_updates(base_url, updates, username=None, password=None, timeout=10,
                          opener=None, api_key=None, index_prefix="conntrack-flows"):
    """Bulk patch existing documents; return per-item success flags (404 is failure)."""
    if not updates:
        return []
    lines = []
    for doc, names in updates:
        if not names or any(field not in ("source", "destination") for field in names):
            raise ValueError("invalid DNS enrichment")
        index, identifier = document_address(doc, index_prefix)
        script = """
if (ctx._source.router_name != params.router ||
    ctx._source.connection_id != params.connection) {
  throw new IllegalArgumentException('flow identity collision');
}
if (params.names.containsKey('source') && ctx._source.source != null) {
  ctx._source.source.reverse_dns = params.names.source;
}
if (params.names.containsKey('destination') && ctx._source.destination != null) {
  ctx._source.destination.reverse_dns = params.names.destination;
}
"""
        body = {"script": {"lang": "painless", "source": script,
                           "params": {"router": doc["router_name"],
                                      "connection": doc["connection_id"], "names": names}}}
        lines.append(json.dumps({"update": {"_index": index, "_id": identifier}}, separators=(",", ":")))
        lines.append(json.dumps(body, separators=(",", ":")))
    headers = {"Content-Type": "application/x-ndjson"}
    if username is not None:
        token = base64.b64encode((username + ":" + (password or "")).encode()).decode("ascii")
        headers["Authorization"] = "Basic " + token
    if api_key is not None:
        headers["Authorization"] = "ApiKey " + api_key
    request = Request(base_url.rstrip("/") + "/_bulk",
                      data=("\n".join(lines) + "\n").encode("utf-8"), headers=headers, method="POST")
    with opener.open(request, timeout=timeout) as response:
        result = json.load(response)
    items = result.get("items")
    if not isinstance(items, list) or len(items) != len(updates):
        raise ValueError("invalid PTR bulk response")
    return [isinstance(item, dict) and isinstance(item.get("update"), dict)
            and item["update"].get("status") in (200, 201)
            and item["update"].get("result") in ("updated", "noop")
            for item in items]


def send_dns_update(base_url, doc, names, username=None, password=None, timeout=10, opener=None, api_key=None, index_prefix="conntrack-flows"):
    """Patch only reverse_dns fields of an existing lifecycle document."""
    if not names or any(field not in ("source", "destination") for field in names):
        raise ValueError("invalid DNS enrichment")
    index, identifier = document_address(doc, index_prefix)
    script = """
if (ctx._source.router_name != params.router ||
    ctx._source.connection_id != params.connection) {
  throw new IllegalArgumentException('flow identity collision');
}
if (params.names.containsKey('source') && ctx._source.source != null) {
  ctx._source.source.reverse_dns = params.names.source;
}
if (params.names.containsKey('destination') && ctx._source.destination != null) {
  ctx._source.destination.reverse_dns = params.names.destination;
}
"""
    body = {"script": {"lang": "painless", "source": script,
                       "params": {"router": doc["router_name"],
                                  "connection": doc["connection_id"], "names": names}}}
    url = base_url.rstrip("/") + "/" + quote(index, safe="") + "/_update/" + quote(identifier, safe="")
    headers = {"Content-Type": "application/json"}
    if username is not None:
        token = base64.b64encode((username + ":" + (password or "")).encode()).decode("ascii")
        headers["Authorization"] = "Basic " + token
    if api_key is not None:
        headers["Authorization"] = "ApiKey " + api_key
    request = Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
    with opener.open(request, timeout=timeout) as response:
        result = json.load(response)
    if result.get("result") not in ("updated", "noop"):
        raise ValueError("unexpected DNS update result")
    return result["result"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="OpenSearch base URL, e.g. https://localhost:9200")
    parser.add_argument("--ca-file", help="PEM CA bundle (default: system trust store)")
    parser.add_argument("--client-cert", help="PEM client certificate for mTLS")
    parser.add_argument("--client-key", help="PEM client private key")
    parser.add_argument("--insecure", action="store_true", help="disable TLS verification (lab only)")
    parser.add_argument("--password-file", help="Basic Auth password file")
    parser.add_argument("--api-key-file", help="OpenSearch API key token file (instead of Basic Auth)")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=1)
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    parsed_url = urlsplit(args.url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc or parsed_url.username is not None or parsed_url.query or parsed_url.fragment or parsed_url.path not in ("", "/"):
        parser.error("--url must be an http(s) URL without embedded credentials")
    if args.timeout <= 0 or args.retries < 0 or args.retry_delay < 0:
        parser.error("timeout must be positive; retries and retry-delay nonnegative")
    username = os.environ.get("OPENSEARCH_USER")
    password = os.environ.get("OPENSEARCH_PASSWORD")
    if args.password_file and password is not None:
        parser.error("use --password-file or OPENSEARCH_PASSWORD, not both")
    if args.password_file:
        password = read_secret(args.password_file)
    api_key = read_secret(args.api_key_file) if args.api_key_file else None
    if api_key is not None and (username is not None or password is not None):
        parser.error("API key cannot be combined with Basic Auth")
    if (username is None) != (password is None):
        parser.error("Basic Auth requires both user and password")
    try:
        opener = make_opener(args.url, args.ca_file, args.client_cert, args.client_key, args.insecure)
    except (ValueError, OSError, ssl.SSLError) as exc:
        parser.error("TLS configuration invalid: " + str(exc))
    if parsed_url.scheme == "http":
        LOG.warning("HTTP is unencrypted: credentials and telemetry travel in plaintext")
    if args.insecure:
        LOG.warning("TLS verification disabled (insecure mode)")
    stats = {"created": 0, "updated": 0, "noop": 0, "invalid": 0, "failed": 0}
    for line_number, line in enumerate(sys.stdin, 1):
        if not line.strip():
            continue
        doc = parse_lifecycle(line)
        if doc is None:
            stats["invalid"] += 1
            continue
        for attempt in range(args.retries + 1):
            try:
                result = send_update(args.url, doc, username, password, args.timeout, opener, api_key)
                stats[result] += 1
                break
            except HTTPError as exc:
                # Only retry transient failures; do not print server body (may contain sensitive data).
                retryable = exc.code in (429, 502, 503, 504)
                if not retryable or attempt == args.retries:
                    LOG.error("line=%d HTTP status=%d", line_number, exc.code)
                    stats["failed"] += 1
                    break
            except (URLError, TimeoutError, OSError) as exc:
                if attempt == args.retries:
                    LOG.error("line=%d transport failure: %s", line_number, type(exc).__name__)
                    stats["failed"] += 1
                    break
            except (ValueError, KeyError, TypeError) as exc:
                LOG.error("line=%d invalid update response: %s", line_number, type(exc).__name__)
                stats["failed"] += 1
                break
            time.sleep(args.retry_delay * (2 ** attempt))
    LOG.info("summary %s", " ".join("%s=%d" % item for item in stats.items()))
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
