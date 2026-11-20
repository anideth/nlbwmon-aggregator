# Lifecycle flow telemetry — installation and operations

Verified lab integration: **2026-11-14**. This guide covers the **new** lifecycle pipeline, not the legacy aggregate index. Use placeholder names and addresses; do not commit real hostnames, passwords, private addresses or flow records.


### Lifecycle status and timeout semantics

The exporter does **not** persist intermediate conntrack UPDATE events or periodic counter snapshots. A received NEW produces `flow.status=active`, with start time but no final byte/packet totals; the document remains active during the session. A received DESTROY performs a scripted update of the same OpenSearch `_id` to `flow.status=closed`, with end time, duration and final counters. DESTROY without a preceding NEW creates a closed document. Late NEW and duplicate events after closure are no-ops. PTR enrichment may independently patch reverse-DNS fields.

**Current behavior:** a lost DESTROY leaves `flow.status=active` indefinitely (subject to index retention). No automatic seven-day timeout, stale status or reconciliation job exists in the code. Kernel conntrack expiry does not imply the collector received DESTROY.

**Beta.2 development branch:** `--max-flow-age 2592000` (seconds; 30 days by default) rejects NEW and DESTROY events whose `flow.started_at` is too old. The cutoff is evaluated when processing the durable SQLite outbox, including retries after an OpenSearch outage. Expired events are removed from the outbox without writing to OpenSearch; their count is logged. This does not change existing documents, create a `stale`/`expired` status, or manage ISM. `active` and `closed` remain the only stored statuses.\n\n> **IMPORTANT — retention configuration:** `--max-flow-age` MUST be strictly **less than** the minimum time a flow's destination daily index is retained by OpenSearch ISM. Leave extra margin for UTC daily index boundaries and ISM scheduling. Example: 28 days maximum flow age with a sufficiently long retention policy (at least 30 days from the earliest possible index creation). This is a **manual relationship to document and verify**; the collector does not read ISM policies. This beta.2 option is not available in beta.1.

### SQLite persistence and OpenSearch requirements

The SQLite database uses `PRAGMA journal_mode=WAL` and `synchronous=FULL`, with an `events` table containing accepted NEW/DESTROY payloads and a `dns_jobs` table for asynchronous PTR work. SQLite is a **durable post-receipt delivery queue**, not the authoritative live-connection state table. The writer sends ordered batches through the OpenSearch Bulk API, then deletes only the successfully acknowledged prefix after accepted `created`, `updated` or `noop` results. It can retry after an OpenSearch outage; it cannot recover events never durably received from the router. The `--max-events` limit applies to the main events queue, not a global bound on all SQLite storage.

The code sends Bulk API `update` actions to `POST /_bulk`, each with `scripted_upsert: true`, a Painless `script` and `upsert: {}`. This executes the script whether or not the document exists; it provides idempotent state transitions for duplicates and avoids client-side read/modify/write. OpenSearch officially marks the **Update Document API introduced in 1.0**, and documents scripted upsert: [OpenSearch Update Document API](https://docs.opensearch.org/latest/api-reference/document-apis/update-document/). Enable `_source` in the target index and allow the necessary Painless scripts. The API feature baseline is **OpenSearch 1.0+**; the collector has **not** been compatibility-tested across all OpenSearch versions. Index templates, ISM support and security configuration may have version-specific differences.

## 1. Architecture and guarantees

```text
OpenWrt nlbwmon fork (NEW / DESTROY, JSONL)
    -> plaintext TCP (trusted LAN / protected tunnel)
    -> flow_collector.py :5517
    -> SQLite persistent outbox (WAL, synchronous=FULL)
    -> flow_writer.send_bulk_updates() (OpenSearch Bulk scripted updates)
    -> conntrack-flows-YYYY.MM.DD
    -> OpenSearch Dashboards Discover (time field flow.started_at)
```

Historical interval aggregation via Fluent Bit is not required for the lifecycle collector.

One **mutable** document per connection, addressed by SHA-256 of `router_name + NUL + connection_id`. The index date is the **UTC start date**, including when DESTROY arrives on another day.

| Incoming event | Result |
| --- | --- |
| NEW | Create document with `flow.status=active`; no end timestamp or byte/packet totals |
| DESTROY after NEW | Update **same document** to `closed`, add end, duration, cumulative counters |
| DESTROY without NEW | Create a closed document; possible when an earlier NEW was missed |
| Late NEW or duplicate event after closed | Script returns `noop`; closed flows do not reopen |

**Not exactly-once:** the collector has no application-level acknowledgement back to the router. TCP disconnect, crash before SQLite commit, invalid input, full queue, disk failure or exporter-side drops can lose events. The SQLite outbox survives collector restart and OpenSearch outage *after successful insertion*. The worker deletes a queued event only after `created`, `updated` or `noop`. Delivery uses ordered Bulk API batches; a persistent authentication/mapping error can block later events. No stale-active expiry or guaranteed throughput.

## 2. Field dictionary

| Field | Meaning |
| --- | --- |
| `router_name` | Router identifier configured in OpenWrt UCI; distinguish devices, keep unique |
| `connection_id` | Opaque exporter-generated connection identity; never split by hyphens |
| `event_type` | Latest applied event: `NEW` or `DESTROY` |
| `source.ip`, `source.port` | **Original** conntrack source endpoint (`orig_src`, `orig_sport`) |
| `destination.ip`, `destination.port` | **Original** conntrack destination endpoint (`orig_dst`, `orig_dport`) |
| `source.bytes`, `source.packets` | Cumulative counters in conntrack **original** direction (`orig_*`), on DESTROY |
| `destination.bytes`, `destination.packets` | Cumulative counters in conntrack **reply** direction (`reply_*`), on DESTROY |
| `network.transport` | `tcp`, `udp`, `icmp`, `icmpv6` |
| `flow.status` | `active` or `closed` |
| `flow.started_at` | UTC flow start; **Discover time field** and index date source |
| `flow.ended_at` | UTC flow end; only for closed flows |
| `flow.duration_seconds` | Nonnegative end minus start, seconds; only for closed flows |
| `_id` | Stable hashed OpenSearch document identifier |
| `_version` | OpenSearch document version; commonly 1 after NEW and 2 after DESTROY, but not a guaranteed event counter |

**Direction is not the same as destination's received payload.** `destination.bytes` is the reply-direction byte count, not “bytes delivered to destination.” For a one-way TCP iperf3 upload, reply traffic is mostly TCP acknowledgements (ACK) and control packets. These are conntrack counters, not a direct measurement of application payload. NAT may mean the stored original tuple differs from translated wire addresses. For ICMP/ICMPv6, port fields are omitted; the exporter has no dedicated ICMP type/code/id in this schema. NEW deliberately has no final traffic counters.

Numbers in OpenSearch `_source` are numeric, without separators. Dashboards may display `5201` as `5,201`; set a **Number** field formatter pattern `0` (not `0,0`) for ports, bytes and packets to remove grouping. This affects only presentation.

## 3. OpenSearch: composable index template

Run the following in **OpenSearch Dashboards → Dev Tools** *before first ingestion*. The example is a baseline mapping for this code. If `conntrack-flows-template` already exists, first run `GET _index_template/conntrack-flows-template` and **compare/merge** settings rather than overwriting an established configuration. In particular, preserve existing shard count, replicas, custom mappings and ISM integration.

```http
PUT _index_template/conntrack-flows-template
{
  "index_patterns": ["conntrack-flows-*"],
  "priority": 200,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "router_name": {"type": "keyword"},
        "connection_id": {"type": "keyword"},
        "event_type": {"type": "keyword"},
        "source": {"properties": {
          "ip": {"type": "ip"},
          "reverse_dns": {"type": "keyword", "ignore_above": 1024},
          "port": {"type": "integer"},
          "bytes": {"type": "long"},
          "packets": {"type": "long"}
        }},
        "destination": {"properties": {
          "ip": {"type": "ip"},
          "reverse_dns": {"type": "keyword", "ignore_above": 1024},
          "port": {"type": "integer"},
          "bytes": {"type": "long"},
          "packets": {"type": "long"}
        }},
        "network": {"properties": {"transport": {"type": "keyword"}}},
        "flow": {"properties": {
          "status": {"type": "keyword"},
          "started_at": {"type": "date"},
          "ended_at": {"type": "date"},
          "duration_seconds": {"type": "long"}
        }}
      }
    }
  }
}
```

**Beta.2 sequence metadata:** the exporter includes `exporter_instance_id` (boot ID plus process identity) and a monotonically increasing `event_sequence` within that process. The collector retains `exporter.instance_id`, `flow.new_sequence`, and `flow.destroy_sequence` where present. Sequence gaps indicate missing or filtered records, not necessarily Netlink loss; they are diagnostic and do not repair missing events. Add `exporter.instance_id` as `keyword` and both sequence fields as `long` in the new index template. The Painless close update preserves `flow.new_sequence` when present.\n\n**Beta.2 timestamp mapping:** for NEW indices, use `date_nanos` for `flow.started_at` and `flow.ended_at`, and `long` for `flow.duration_ns`. The exporter sends optional `started_nsec` and `finished_nsec` alongside legacy whole-second fields. An existing index field mapped as `date` **cannot be changed in place** to `date_nanos`; prepare a new template/index generation before deploying the beta.2 timestamp pipeline. Do not blindly replace the live beta.1 template.\n\n`dynamic: false` is an optional strict-schema preference for new deployments; unknown fields remain in `_source` but are not indexed. If the existing template uses dynamic mappings, do not silently change that behavior. Replicas `0` is appropriate for a single-node lab, **not** a general production recommendation. The collector needs index creation privileges if daily indices do not already exist and auto-creation is enabled.

Inspect, simulate, and verify:
```http
GET _index_template/conntrack-flows-template
POST _index_template/_simulate_index/conntrack-flows-2099.01.01
GET conntrack-flows-*/_mapping
GET conntrack-flows-*/_mapping/field/flow.started_at,flow.ended_at,flow.duration_seconds,router_name
```

Changing an index template affects **future** indices only; existing indices keep their mappings. Add compatible missing fields to existing indices using `PUT <specific-index>/_mapping` after inspecting the mapping; do not delete or recreate data casually. No need to precreate each daily index if auto-creation is permitted.

## 4. Retention / ISM

For a 30-day retention target, create an OpenSearch Index State Management policy in **Index Management → State management policies**, e.g. `conntrack-flows-retention`, with a hot state transitioning to delete when index age is at least 30 days. **Deletion is permanent**. Confirm snapshots/retention requirements before enabling it.

Example for OpenSearch installations supporting this ISM API:
```http
PUT _plugins/_ism/policies/conntrack-flows-retention
{
  "policy": {
    "description": "Delete lifecycle flow indices after 30 days",
    "default_state": "hot",
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [{"state_name": "delete", "conditions": {"min_index_age": "30d"}}]
      },
      {
        "name": "delete",
        "actions": [{"delete": {}}],
        "transitions": []
      }
    ],
    "ism_template": [{"index_patterns": ["conntrack-flows-*"], "priority": 100}]
  }
}
```

**Do not blindly PUT over an existing policy:** check `GET _plugins/_ism/policies/conntrack-flows-retention` first. Updating an existing policy can require `if_seq_no` / `if_primary_term` and may affect managed indices. Verify `GET _plugins/_ism/explain/conntrack-flows-*` and inspect policy attachment. ISM age refers to index age, not each flow's start timestamp; daily indices may be created late from delayed events. Never apply this policy to the legacy `conntrack-agg-*` family unless explicitly intended.

## 5. Dashboards: index pattern / data view

1. Open **Dashboards Management → Index Patterns** (menu name depends on version).
2. Create `conntrack-flows-*` after the first index exists (or after mappings are available).
3. **Select `flow.started_at` as the time field**. Do **not** choose `flow.ended_at`: NEW has no end timestamp and will disappear from Discover.
4. Open **Discover**, select this pattern, set the time range to include the flow's **start**, and add useful columns: `router_name`, `flow.status`, `source.ip`, `source.port`, `destination.ip`, `destination.port`, `source.bytes`, `destination.bytes`, `flow.duration_seconds`.
5. Refresh / auto-refresh Discover. A NEW row changes to closed on DESTROY; there is not a separate event history row. A long-running connection started before the chosen window is hidden even if it ended recently.
6. Optional number display: for port/byte/packet fields use **Number → pattern `0`** to avoid thousands commas. Stored numeric values are unchanged.

Example Discover filters (field availability/mapping required):
```text
router_name:"lab-router" AND flow.status:"active"
router_name:"lab-router" AND flow.status:"closed"
```

## 6. Debian-family collector installation

Requirements: Python 3 standard library, OpenSearch with scripted updates enabled, trusted network between router and collector, a persistent filesystem for SQLite. No Python packages, Fluent Bit, or sqlite3 CLI are needed for the new path. Work from a reviewed pinned revision.

```sh
cd /opt/nlbwmon-aggregator && python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py && python3 -m unittest -v test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
id flow-collector >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin flow-collector
getent group flow-secrets >/dev/null || groupadd --system flow-secrets
usermod -aG flow-secrets flow-collector
install -d -o flow-collector -g flow-collector -m 750 /opt/nlbwmon-data
install -d -o root -g flow-secrets -m 750 /etc/flow-writer
```

Create `/etc/flow-writer/password` using a secure method (never commit it, paste it into a command line or expose it in a log); set ownership `root:flow-secrets`, mode `0640`. Keep `/opt/nlbwmon-data` writable by `flow-collector`, and repository source readable. For an existing DB, preserve and chown the DB and any WAL/SHM files consistently **while the service is stopped**.

Example unit file `/etc/systemd/system/flow-collector.service` (replace `COLLECTOR_LAN_IP`, `search.example`, trust paths and username):
```ini
[Unit]
Description=nlbwmon Flow Lifecycle Collector
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=flow-collector
Group=flow-collector
SupplementaryGroups=flow-secrets
WorkingDirectory=/opt/nlbwmon-aggregator
Environment=PYTHONUNBUFFERED=1
Environment=OPENSEARCH_USER=writer
ExecStart=/usr/bin/python3 /opt/nlbwmon-aggregator/flow_collector.py --listen COLLECTOR_LAN_IP --port 5517 --db /opt/nlbwmon-data/flows.db --url https://search.example:9200 --ca-file /etc/ssl/certs/private-ca.pem --password-file /etc/flow-writer/password
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/nlbwmon-data
StandardOutput=journal
StandardError=journal
SyslogIdentifier=flow-collector

[Install]
WantedBy=multi-user.target
```

For an isolated **beta test collector**, keep the same service account and permissions, but set `--index-prefix conntrack-flows-tests` in `ExecStart` (the default is the production `conntrack-flows` prefix). Bind `--listen` to the test VM's intended LAN IP, use a separate local SQLite DB, and reuse the existing OpenSearch writer account where appropriate. The test router must point to the test VM listener; do not redirect the production router. Optional `--dns --dns-server <resolver> --dns-timeout 1.5` enables asynchronous PTR enrichment. Verify the password file is readable by `flow-collector` before startup without displaying its contents: `runuser -u flow-collector -- test -r /etc/flow-writer/password`. The ISM policy for `conntrack-flows-tests-*` should use a higher `ism_template` priority than the overlapping `conntrack-flows-*` policy and delete after 30 days. An index is not created until the first successful write. OpenSearch Dashboards configuration is not required for this headless beta verification.

For a **lab only**, `--insecure` can replace `--ca-file` when the certificate cannot yet be validated; do not treat this as production-ready TLS. Plaintext router TCP should stay within a trusted network or protected tunnel. Bind to the intended interface; do not expose the collector on the public Internet.

```sh
systemd-analyze verify /etc/systemd/system/flow-collector.service && systemctl daemon-reload
systemctl start flow-collector.service && systemctl status flow-collector.service --no-pager -l
systemctl enable flow-collector.service
systemctl is-enabled flow-collector.service && systemctl is-active flow-collector.service
ss -ltnp 'sport = :5517'
```

Only enable after confirming healthy startup. Keep one worker/service per SQLite queue; do not run two collectors against the same DB.

## 7. OpenWrt nlbwmon exporter

The JSONL exporter is built into the **project fork** of nlbwmon; stock distribution packages may not have `flow_export`. Verify exact firmware architecture, branch/commit, installed package and its dependencies **before** replacing a production package. Do not use a laboratory APK blindly on another router.

Example `/etc/config/nlbwmon` section:
```uci
config flow_export
    option enabled '1'
    option router_name 'lab-router'
    option mark '0x00000000'
    option mark_mask '0x00000000'
    option tcp_host '192.0.2.10'
    option tcp_port '5517'
    option tcp_queue_size '1024'
```

Zero mark mask matches all marks and can include router-originated, broadcast or other traffic; the lifecycle collector does **not** apply the legacy aggregator's WAN/forwarded-traffic filters. Confirm desired scope before production. Export is best-effort, plaintext TCP; queue capacity is not a delivery guarantee.

On a lab router with the correct exporter already installed:
```sh
uci get nlbwmon.flow_export.tcp_host; uci get nlbwmon.flow_export.tcp_port; uci get nlbwmon.flow_export.router_name
/etc/init.d/nlbwmon restart
logread | grep -i nlbwmon | tail -10
```

For a safe rollback, record the original `tcp_host` and `tcp_port` before editing and restore only those values. Restarting nlbwmon may affect in-memory monitoring state; schedule changes appropriately.

## 8. Verification and troubleshooting

On the collector host:
```sh
systemctl is-enabled flow-collector.service && systemctl is-active flow-collector.service
ss -tnp '( sport = :5517 or dport = :5517 )'
journalctl -u flow-collector.service -n 30 --no-pager
python3 -c 'import sqlite3; c=sqlite3.connect("/opt/nlbwmon-data/flows.db"); print("queued",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"integrity",c.execute("PRAGMA quick_check").fetchone()[0]); c.close()'
```

`ESTAB` proves a TCP connection, **not** event ingestion. `queued=0` can mean successfully delivered or never received; inspect OpenSearch documents. Successful worker logs include `delivered id=N result=created|updated|noop`. The worker logs only successful deliveries, not every TCP read.

Dev Tools:
```http
GET conntrack-flows-*/_search
{
  "size": 10,
  "sort": [{"flow.started_at": {"order": "desc"}}],
  "query": {"term": {"router_name": "lab-router"}}
}
```

Verify the **same `_id`** transitions `active → closed` and `_version` increases (often 1 → 2), with `flow.ended_at`, duration and final counters. A closed `_version=1` document can legitimately be DESTROY without NEW. Compare directional bytes against a controlled iperf3 test, accounting for protocol overhead; don't expect exact equality to application bytes.

Failure checks: listener bind/address, DNS and TCP reachability, exporter UCI, OpenWrt `logread`, collector service account DB/WAL and password permissions, queue length, HTTP 401/403/429/5xx, index template/mapping, ISM, certificate trust, and Discover time field/time window. Persistent OpenSearch failures keep queued rows and may block new deliveries. Avoid deleting the SQLite DB to “fix” a backlog.

## 9. Lab result and production migration gate

**Observed 2026-11-14:** laboratory OpenWrt connected over TCP to collector :5517; real NEW documents appeared `active` in `conntrack-flows-2026.11.14`. Controlled iperf3 flow later became `DESTROY`, `closed`, `_version=2` **with the same `_id`**, duration 101 s and cumulative byte/packet counters. Collector service was `enabled` and `active`; SQLite queue was empty at the observed check. This establishes the main functional end-to-end path, **not** long-term losslessness or production readiness.

Before touching the physical production router: inventory firmware/CPU architecture/package manager, installed nlbwmon version and dependencies, available storage, current UCI and legacy sender, source/branch and build artifact provenance, snapshot/backup/recovery path, safe remote access and rollback. Verify current production routing and monitoring baseline; plan a short maintenance window. Test the correct exporter package on a matching spare/VM if possible. Do **not** overwrite the running package or stop the old sender until the new path and rollback are verified. Do not change unrelated monitoring services as part of this installation.


## Asynchronous PTR enrichment (experimental)

Run the collector with `--dns` to enable optional PTR lookups after the lifecycle
document has been delivered. The original event is indexed first; a separate
worker performs cached PTR lookups and sends an identity-checked partial update
of `source.reverse_dns` and `destination.reverse_dns`. DNS failure never blocks
the primary event worker. Jobs are persisted in the `dns_jobs` SQLite table;
the table is created automatically and should be backed up with the main database.
The DNS worker has its own retry loop; DNS failure leaves the document intact.
No backfill of historical documents is performed.

**Mapping deployment:** Before enabling `--dns`, inspect the existing
`conntrack-flows-template` and merge the two `reverse_dns` keyword mappings
shown above, retaining all other settings and mappings. Because the baseline
template uses `dynamic: false`, unmapped PTR fields remain in `_source` but
are not searchable. Also add the same two fields to the current live daily index
using `PUT conntrack-flows-YYYY.MM.DD/_mapping` (after inspection); updating
the template alone affects only new indices. No existing documents need updating.

The DNS worker makes one update per document when at least one PTR exists,
and no update if both PTRs are absent. The worker currently processes jobs
serially; it is best-effort and is not a throughput benchmark.

## Lab validation record (2026-11-17)

A separate test collector and laboratory OpenWrt exporter were validated against an isolated `conntrack-flows-tests-*` namespace. OpenSearch returned two `NEW`/`active` documents and a `DESTROY`/`closed` document; a follow-up read confirmed that one of the previously active documents changed to `DESTROY`/`closed` **without changing `_id`**, with `_version=4`, 142-second duration, and final byte/packet counters. `GET _plugins/_ism/explain/<test-index>` confirmed `conntrack-flows-tests-retention` attached with `enabled=true`. This confirms the end-to-end functional lifecycle in one lab; it does **not** establish lossless delivery, recovery after outage/restart, or actual 30-day deletion. Real addresses, hostnames and connection identifiers are intentionally omitted.

For isolated tests, configure `--index-prefix conntrack-flows-tests` and create a matching index template and retention policy before ingestion. Template selection priority and ISM policy priority are independent; where the test pattern overlaps `conntrack-flows-*`, give the test rules higher priorities and verify effective selection. The test index is created automatically on first successful write when OpenSearch auto-creation is permitted. A Dashboards data view (`conntrack-flows-tests-*`, time field `flow.started_at`) is **optional** and is not required for ingestion.
