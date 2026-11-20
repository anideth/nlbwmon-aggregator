"""Parse individual nlbwmon lifecycle records without changing legacy aggregation.

This module does not write to OpenSearch. It is a validation boundary for
future per-connection indexing; the existing aggregator remains untouched.
"""
import ipaddress
import json
import re
from datetime import datetime, timezone

ROUTER = re.compile(r"^[A-Za-z0-9_.-]{1,63}$")
PROTOCOLS = {1: "icmp", 6: "tcp", 17: "udp", 58: "icmpv6"}


def _iso(seconds, nanos=None):
    base = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanos:09d}Z" if nanos is not None else f"{base}.000Z"


def parse_lifecycle(line):
    """Return validated NEW/DESTROY lifecycle record, or None.

    The exporter-generated connection_id includes boot ID, conntrack ID and
    observed start time. Treat it as opaque; never split on '-' (UUIDs contain
    hyphens). The ID is scoped to router_name.
    """
    try:
        event = json.loads(line)
        if not isinstance(event, dict) or event.get("schema") != "nlbwmon-flow-v1":
            return None
        kind = event.get("event_type")
        if kind not in ("NEW", "DESTROY"):
            return None
        router = event["router_name"]
        connection = event["connection_id"]
        if not isinstance(router, str) or not ROUTER.fullmatch(router):
            return None
        if not isinstance(connection, str) or not 1 <= len(connection) <= 256 or any(ord(c) < 33 for c in connection):
            return None
        family = int(event["family"])
        protocol_number = int(event["protocol"])
        if family not in (2, 10) or protocol_number not in PROTOCOLS:
            return None
        if (protocol_number == 1 and family != 2) or (protocol_number == 58 and family != 10):
            return None
        addresses = {key: ipaddress.ip_address(event[key]) for key in ("orig_src", "orig_dst", "reply_src", "reply_dst")}
        if any(ip.version != (4 if family == 2 else 6) for ip in addresses.values()):
            return None
        started = int(event["started_sec"])
        finished = int(event["finished_sec"])
        has_nanos = "started_nsec" in event and "finished_nsec" in event
        if ("started_nsec" in event) != ("finished_nsec" in event):
            return None
        started_ns = int(event["started_nsec"]) if has_nanos else 0
        finished_ns = int(event["finished_nsec"]) if has_nanos else 0
        if not 0 <= started_ns < 1000000000 or not 0 <= finished_ns < 1000000000:
            return None
        if not 0 < started < 253402300800:
            return None
        if kind == "DESTROY" and not started <= finished < 253402300800:
            return None
        if kind == "DESTROY" and (finished - started) * 1000000000 + finished_ns - started_ns < 0:
            return None
        if kind == "NEW" and finished not in (0, started):
            return None
        counts = {key: int(event[key]) for key in ("orig_packets", "orig_bytes", "reply_packets", "reply_bytes")}
        if any(v < 0 for v in counts.values()):
            return None
        ports = {}
        for key in ("orig_sport", "orig_dport", "reply_sport", "reply_dport"):
            val = int(event[key])
            if not 0 <= val <= 65535:
                return None
            ports[key] = val
        # For ICMP the exporter currently has no dedicated type/code/id
        # fields. Do not mislabel its port-shaped values as TCP/UDP ports.
        source = {"ip": str(addresses["orig_src"])}
        destination = {"ip": str(addresses["orig_dst"])}
        if protocol_number in (6, 17):
            source["port"] = ports["orig_sport"]
            destination["port"] = ports["orig_dport"]
        flow = {"status": "active" if kind == "NEW" else "closed",
                "started_at": _iso(started, started_ns if has_nanos else None)}
        if kind == "DESTROY":
            flow["ended_at"] = _iso(finished, finished_ns if has_nanos else None)
            flow["duration_seconds"] = finished - started
            if has_nanos:
                flow["duration_ns"] = (finished - started) * 1000000000 + finished_ns - started_ns
            source.update(bytes=counts["orig_bytes"], packets=counts["orig_packets"])
            destination.update(bytes=counts["reply_bytes"], packets=counts["reply_packets"])
        return {"router_name": router, "connection_id": connection, "event_type": kind,
                "network": {"transport": PROTOCOLS[protocol_number]},
                "source": source, "destination": destination, "flow": flow}
    except (ValueError, KeyError, TypeError, OverflowError):
        return None
