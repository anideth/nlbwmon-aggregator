# End-to-end installation: OpenWrt nlbwmon -> lifecycle collector -> OpenSearch

**Release candidate guide (2026-11-16); not yet a published beta.** This guide covers the per-connection lifecycle path. The current lifecycle collector does not require Fluent Bit; historical aggregation is outside this installation guide.


## Lifecycle semantics and minimum OpenSearch API

**NEW:** upsert a single `active` flow document, without final byte/packet totals. **While active:** no intermediate conntrack UPDATE or periodic counter snapshots are stored; optional asynchronous PTR can update DNS fields. **DESTROY:** scripted upsert of the **same** document to `closed`, with end time, duration and final cumulative counters. DESTROY without NEW creates a closed document; late NEW/duplicate events after closure are ignored. If DESTROY is missing, the document remains `active` indefinitely until index retention removes it. **There is no implemented seven-day timeout.** Proposed configurable seven-day `stale` detection, a disabled mode and `stale -> closed` handling are deferred to the next release along with optional CIDR exclusions.

SQLite is the collector's **durable post-receipt event outbox** (`WAL`, `synchronous=FULL`) and async PTR job store, not a full connection-state database. It preserves already accepted events during OpenSearch downtime; it does not guarantee receipt from the router.

The collector requires the OpenSearch Update Document API with Painless **scripted upsert** (`POST /{index}/_update/{id}`, `scripted_upsert: true`, `upsert: {}`). The official [OpenSearch Update Document API reference](https://docs.opensearch.org/latest/api-reference/document-apis/update-document/) marks the API **introduced in OpenSearch 1.0**. Require `_source` enabled and scripts permitted. This is a documented **API minimum**, not a tested compatibility matrix for OpenSearch 1.x/2.x/3.x. For exact status transitions and the next-release stale design see [LIFECYCLE_SETUP.md](LIFECYCLE_SETUP.md).

## Components and responsibilities

| Component | Where | Repository |
| --- | --- | --- |
| Patched `nlbwmon` and `nlbw` | OpenWrt router | [anideth/nlbwmon](https://github.com/anideth/nlbwmon) |
| `flow_collector.py`, SQLite outbox and async PTR | Debian-family monitoring host | [anideth/nlbwmon-aggregator](https://github.com/anideth/nlbwmon-aggregator) |
| Scripted document updates, `conntrack-flows-*` index template, optional ISM | OpenSearch | [Lifecycle setup](LIFECYCLE_SETUP.md) |
| OpenSearch Dashboards | Monitoring UI | Index pattern `conntrack-flows-*`, time field `flow.started_at` |
| Optional Prometheus Lua exporter and Grafana dashboard | Router / metrics stack | [Third-party integration](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/docs/PROMETHEUS_AND_BACKUP.md) |

The lifecycle collector is **not** a Prometheus exporter, does not require Fluent Bit and does not bundle the third-party Lua collector or dashboard. TCP from router to collector is plaintext, unauthenticated and best-effort; restrict access to trusted router addresses or a protected tunnel.

## 0. Preflight and backup

1. Record router firmware version/target/architecture, installed `nlbwmon` package version, UCI settings, daemon status, conntrack marking policy and any existing Lua collector. Verify that the router's release and SDK target match the APK; never install an APK built for another OpenWrt release or architecture.
2. Record monitoring host Python version, OpenSearch version and current index template, ISM policy, service unit and available disk. If upgrading an existing collector, record its exact Git commit and take a **consistent** SQLite backup, including pending events, before changing code.
3. Back up `/etc/config/nlbwmon` and the configured nlbwmon accounting database outside the router. Save the existing official package and tested rollback method. Use a Proxmox snapshot for lab installation/rollback; do not assume the deploy helper automatically restores a failed install.
4. Create an OpenSearch least-privilege writer credential and store it in a root-controlled file, not in Git or shell history. Plan a trusted CA certificate for TLS; do not use `--insecure` in a published production recipe.

## 1. Install the OpenWrt fork

Build or obtain a verified **target-specific** APK from an immutable, tested revision. No public beta binary is asserted by this guide. Detailed SDK build and rollback: [BUILD_AND_ROLLBACK.md](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/docs/BUILD_AND_ROLLBACK.md).

The source checkout currently documents:

```sh
bash build-nlbwmon.sh --target x86-64
bash build-nlbwmon.sh --target filogic
```

These scripts require the matching OpenWrt **25.12.6** SDK, package feed and expected development branch. Validate the generated APK metadata, SHA256 and included init/UCI files. Use the interactive `deploy-nlbwmon.sh` only after reviewing its target checks and rollback assumptions; it is **not** an unattended production installer.

Before changing the router, check `/etc/config/nlbwmon`. The fork adds an opt-in `config flow_export` section. Illustrative settings (replace placeholders; UCI syntax may need adapting to your installed config):

```uci
config flow_export
    option enabled '1'
    option router_name 'router-a'
    option tcp_host '192.0.2.10'
    option tcp_port '5517'
    option tcp_queue_size '1024'
    option mark '0x00000000'
    option mark_mask '0x00000000'
```

The `192.0.2.10` address is a documentation-only TEST-NET address. A zero mask matches **all** conntrack marks; it can generate substantial traffic and load. To select marked flows, a site-specific nftables/firewall4 rule must set **`ct mark`**, not merely `meta mark`; the fork does not install such rules. Do not change live firewall rules just to make a sample configuration work. Check [flow export settings](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/docs/FLOW_EXPORT.md) before enabling. Restart only `nlbwmon` as part of a controlled installation and verify `logread` and service status.

## 2. Configure OpenSearch first

Read [LIFECYCLE_SETUP.md](LIFECYCLE_SETUP.md), sections 3–5. Review the current `conntrack-flows-template` before creating or merging mappings. Required fields include `router_name`, `connection_id`, `flow.status`, `flow.started_at` (date), `flow.ended_at` (date), numeric counters, `source.ip`/`destination.ip` (ip) and optional `source.reverse_dns`/`destination.reverse_dns` (keyword).

Do **not** overwrite an existing index template or ISM policy blindly. A 30-day delete policy is optional and destructive once it reaches retention age; verify actual policy attachment and backup requirements. For a single-node lab, zero replicas is reasonable, but it is not a general recommendation.

Create OpenSearch Dashboards index pattern `conntrack-flows-*` using **`flow.started_at`** as its time field. Selecting `flow.ended_at` would hide active NEW flows.

## 3. Install the Debian collector

Use [FLOW_COLLECTOR_DEPLOYMENT.md](FLOW_COLLECTOR_DEPLOYMENT.md) as the authoritative copy/paste procedure and [examples/flow-collector.service](../examples/flow-collector.service) as a reviewed unit template. Summary:

1. Clone the collector repository and pin the exact tested Git revision under `/opt/nlbwmon-aggregator`.
2. Run `python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py dns_resolver.py` and `python3 -m unittest -v test_dns_resolver test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector`.
3. Create dedicated non-login `flow-collector` account, `flow-secrets` group, persistent `/opt/nlbwmon-data` and restricted `/etc/flow-writer/password` as described in that guide.
4. Copy the sample systemd unit to `/etc/systemd/system/flow-collector.service`; replace bind address, OpenSearch endpoint, CA path and credentials file location. Limit inbound TCP port 5517 to the router. Avoid binding `0.0.0.0` without firewall rules.
5. Validate the unit with `systemd-analyze verify /etc/systemd/system/flow-collector.service`, then `systemctl daemon-reload && systemctl enable --now flow-collector.service`.
6. Check `systemctl status flow-collector.service --no-pager` and `journalctl -u flow-collector.service -n 100 --no-pager`. The optional `--dns` flag enables asynchronous PTR updates. Keep only **one** service instance per SQLite outbox.

## 4. Acceptance test: one real connection

1. Generate a known TCP transfer from a client behind the router. Capture its endpoint tuple and time without posting private addresses publicly.
2. Confirm the exporter has connected to the collector, and the collector log has no authentication, mapping, SQLite or retry errors.
3. Search OpenSearch for the unique `router_name` and `connection_id`. NEW should create an `active` document. After kernel conntrack DESTROY, the **same** `_id` should become `closed`, with end time, duration and cumulative counters. A missed DESTROY can leave a stale `active` document.
4. With `--dns`, confirm PTR fields are added when a PTR record exists; missing PTR is not necessarily an error. The OpenSearch index's deleted-document count can increase as documents are updated.
5. Check SQLite outbox counts without modifying it:

```sh
python3 -c 'import sqlite3; c=sqlite3.connect("file:/opt/nlbwmon-data/flows.db?mode=ro",uri=True); print("events",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"dns_jobs",c.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0]); c.close()'
```

6. In OpenSearch Dev Tools, read only:

```http
GET _cat/indices/conntrack-flows-*?v&h=index,health,docs.count,docs.deleted,pri.store.size
GET _plugins/_ism/explain/conntrack-flows-*
GET conntrack-flows-*/_mapping/field/flow.started_at,flow.ended_at,source.ip,destination.ip
```

Index health Green and document growth show ingestion and shard availability, **not** complete delivery. No application-level ACK exists between router and collector; queue pressure, Netlink overrun, crash and reconnect can lose records.

## 5. Optional Grafana metrics

For nlbwmon accounting metrics, install the **independent** Lua Prometheus collector by [giuliomagnifico](https://github.com/giuliomagnifico/openwrt-nlbwmon-prometheus-collector) and verify `nlbwmon_rx_bytes`, `nlbwmon_tx_bytes` and `nlbwmon_connections` in Prometheus/VictoriaMetrics. Use its dashboard example only with appropriate attribution and license review. Do not confuse the Lua scrape endpoint with the TCP lifecycle collector.

## 6. Upgrades and rollback

- **Router:** use the tested official-package downgrade path or Proxmox lab snapshot; preserve existing UCI config and accounting database. Do not assume `deploy-nlbwmon.sh` provides automatic rollback. See the router [build/rollback guide](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/docs/BUILD_AND_ROLLBACK.md).
- **Collector:** record prior Git SHA, service unit and SQLite schema; stop only `flow-collector.service`, deploy a tested revision, verify the queue, restart. If reverting, restore compatible code/unit without discarding queued SQLite events. See [collector deployment guide](FLOW_COLLECTOR_DEPLOYMENT.md).
- **OpenSearch:** do not delete `conntrack-flows-*` or `conntrack-agg-*` indices to resolve a mapping issue without an explicit migration/backup plan.
- **Network security:** secure TCP ingestion and validate OpenSearch TLS. Do not place credentials in systemd ExecStart or Git.

## Release scope and known limitations

This release candidate includes NEW/DESTROY lifecycle collection, durable post-receipt SQLite outbox, scripted OpenSearch updates and optional asynchronous PTR. **CIDR exclusion is deferred to the next release**. No flow losslessness, full TCP transport authentication, stale-active expiry, full NAT attribution or guaranteed router memory minimum is claimed. A successful install/rollback on the user's lab does not establish compatibility with every OpenWrt device. Publish a GitHub beta tag and release assets only after license, provenance, Git-history privacy review, build verification and owner sign-off.
