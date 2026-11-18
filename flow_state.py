"""Pure, backend-independent flow lifecycle transitions.

No I/O, no scheduler and no OpenSearch side effects. Callers must apply
transitions atomically (e.g. OpenSearch scripted update) to avoid races.
"""
from datetime import datetime, timezone

STATUSES = frozenset(("active", "stale", "closed"))


def transition(current, incoming):
    """Return merged flow document; never reopen a closed connection.

    incoming is a validated parse_lifecycle() result. Missing NEW is supported.
    DESTROY wins over NEW and stale, including out-of-order delivery.
    """
    if incoming["event_type"] not in ("NEW", "DESTROY"):
        raise ValueError("unsupported lifecycle event")
    if current is None:
        return incoming.copy()
    if (current["router_name"], current["connection_id"]) != (
            incoming["router_name"], incoming["connection_id"]):
        raise ValueError("connection identity mismatch")
    old_status = current["flow"]["status"]
    if old_status not in STATUSES:
        raise ValueError("unknown stored status")
    if old_status == "closed" or incoming["event_type"] == "NEW":
        return current.copy()
    # A late DESTROY closes either active or stale. It supplies real end
    # timestamp and final counters; never infer them from stale timeout.
    result = current.copy()
    result.update({k: v for k, v in incoming.items()
                   if k not in ("flow",)})
    result["flow"] = {**current["flow"], **incoming["flow"]}
    return result


def mark_stale(current, now_sec, timeout_sec):
    """Mark an old active record stale without inventing an end timestamp.

    This is a pure decision, not a background job. A backend must enumerate
    candidates and atomically apply the transition with concurrency checks.
    """
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive")
    if current["flow"]["status"] != "active":
        return current.copy()
    started = current["flow"]["started_at"]
    started_sec = datetime.fromisoformat(started.replace("Z", "+00:00")).timestamp()
    if now_sec - started_sec < timeout_sec:
        return current.copy()
    result = current.copy()
    result["flow"] = {**current["flow"], "status": "stale"}
    return result
