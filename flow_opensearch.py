"""Build atomic OpenSearch lifecycle updates; no network I/O.

Documents are routed to the UTC day of started_at, not event arrival day.
The caller must ensure the corresponding index exists (or auto-create is enabled).
"""
import hashlib
from datetime import datetime, timezone

SCRIPT = """
if (ctx.op == 'create') {
  ctx._source.putAll(params.doc);
} else {
  if (ctx._source.router_name != params.doc.router_name ||
      ctx._source.connection_id != params.doc.connection_id) {
    throw new IllegalArgumentException('flow identity collision');
  }
  if (ctx._source.flow.status == 'closed' || params.doc.event_type == 'NEW') {
    ctx.op = 'none';
  } else if (params.doc.event_type == 'DESTROY') {
    def sourceDns = ctx._source.source == null ? null : ctx._source.source.reverse_dns;
    def destinationDns = ctx._source.destination == null ? null : ctx._source.destination.reverse_dns;
    ctx._source.putAll(params.doc);
    ctx._source.flow = params.doc.flow;
    if (sourceDns != null) { ctx._source.source.reverse_dns = sourceDns; }
    if (destinationDns != null) { ctx._source.destination.reverse_dns = destinationDns; }
  } else {
    ctx.op = 'none';
  }
}
"""


def validate_index_prefix(prefix):
    """Restrict index names to a predictable lowercase namespace."""
    import re
    if not isinstance(prefix, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", prefix) or prefix in (".", ".."):
        raise ValueError("invalid index prefix: use 1-63 lowercase letters, digits, '_' or '-', starting with a letter")
    return prefix


def document_address(doc, prefix="conntrack-flows"):
    """Stable address across NEW/DESTROY, including UTC midnight."""
    prefix = validate_index_prefix(prefix)
    router, connection = doc["router_name"], doc["connection_id"]
    started = datetime.fromisoformat(doc["flow"]["started_at"].replace("Z", "+00:00"))
    if started.tzinfo is None:
        raise ValueError("started_at must have a timezone")
    day = started.astimezone(timezone.utc).strftime("%Y.%m.%d")
    digest = hashlib.sha256((router + "\0" + connection).encode("utf-8")).hexdigest()
    return f"{prefix}-{day}", digest


def update_request(doc, prefix="conntrack-flows"):
    """Return (index, id, OpenSearch _update JSON body), without sending."""
    if doc["event_type"] not in ("NEW", "DESTROY"):
        raise ValueError("invalid lifecycle event")
    expected = "active" if doc["event_type"] == "NEW" else "closed"
    if doc["flow"]["status"] != expected:
        raise ValueError("inconsistent lifecycle status")
    index, identifier = document_address(doc, prefix)
    return index, identifier, {
        "scripted_upsert": True,
        "script": {"lang": "painless", "source": SCRIPT, "params": {"doc": doc}},
        "upsert": {},
    }
