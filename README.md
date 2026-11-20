# nlbwmon-aggregator

**English** | [Русский](README.ru.md)

**Installation:** [English](INSTALL.md) | [Русский](INSTALL.ru.md)

> [!CAUTION]
> **Experimental project, not a guaranteed network-flow audit trail.** Router-to-collector TCP is unencrypted, unauthenticated, and has no application-level acknowledgement. Events may be lost. Do not use the results as the sole source for billing, investigations, or security audits. Validate configuration, OpenSearch TLS, permissions, backups, and rollback before use.

**Published source-only beta [`v0.1.0-beta.1`](https://github.com/anideth/nlbwmon-aggregator/releases/tag/v0.1.0-beta.1):** [complete installation](INSTALL.md), [end-to-end setup](docs/INSTALLATION_END_TO_END.md), [systemd deployment](docs/FLOW_COLLECTOR_DEPLOYMENT.md), [OpenWrt nlbwmon fork](https://github.com/anideth/nlbwmon). The Git tag and GitHub prerelease are published without packaged binaries. The project uses the [MIT License](LICENSE).

## Why this project exists

This Python companion to the [nlbwmon fork](https://github.com/anideth/nlbwmon) receives conntrack lifecycle events and makes individual connections searchable in OpenSearch **without introducing a second competing conntrack collector on the router**.

The router's nlbwmon continues standard traffic accounting while also emitting `NEW` and `DESTROY` as JSONL (`nlbwmon-flow-v1`). The collector accepts the events, stores them in SQLite WAL, and forwards them to OpenSearch. This is an observability tool, **not** packet capture, NetFlow/IPFIX, or a guaranteed-delivery system.

## Maintainer, AI assistance and project maturity

The project is maintained by a DevOps/infrastructure engineer experienced in Linux and networking, with limited formal software development experience. Python code has been developed with substantial AI-tool assistance; architecture, deployment and testing have been carried out in a personal lab. No independent security audit or broad compatibility certification has been completed.

**Status: published experimental beta `v0.1.0-beta.1`.** Successful ingestion in a lab does not prove event completeness, correct attribution in every NAT topology, or compatibility with every OpenWrt/OpenSearch version. Intended primarily for lab and self-hosted experimentation.

## Architecture

```text
OpenWrt / nlbwmon fork (NEW, DESTROY)
    -> TCP JSONL (trusted network / protected tunnel)
    -> flow_collector.py :5517
    -> SQLite WAL (accepted-event queue)
    -> OpenSearch Bulk API (scripted upsert)
    -> conntrack-flows-YYYY.MM.DD
    -> OpenSearch Dashboards / Grafana
```

**Fluent Bit is not required for this pipeline.** Each connection has one document with a stable identity. Optional reverse-DNS (PTR) enrichment runs asynchronously.

## Connection lifecycle

- **NEW:** creates a document with `flow.status=active`, start time, and original endpoint addresses/ports. Final traffic counters are not available yet.
- **DESTROY:** updates the **same document** to `flow.status=closed`, recording end time, duration, and final byte/packet counters.
- **DESTROY without NEW:** creates a closed document, since the NEW event may have been missed.
- **Duplicate events or late NEW:** handled idempotently; a closed document is not reopened.
- **Missing DESTROY:** the document may remain `active` until index retention deletes it. Automatic stale-flow detection is **not implemented**.

Intermediate UPDATE events and periodic per-connection counter snapshots are not stored. DESTROY traffic values are cumulative conntrack counters, not necessarily application payload bytes. NAT can affect how original and translated endpoints should be interpreted.

See [lifecycle semantics and fields](docs/LIFECYCLE_SETUP.md).

## SQLite, OpenSearch and delivery guarantees

SQLite acts as a durable queue for lifecycle events **already accepted** by the collector (WAL, `synchronous=FULL`). PTR results are cached per IP in SQLite (`dns_cache`), while late PTR follow-ups are bounded and kept in memory (up to 2,000 connections for 30 seconds). No per-flow `dns_jobs` are created. Late PTR enrichment can be missed after restart or overflow. Once committed, an event can survive collector restart and temporary OpenSearch outages. Events lost on the router or before SQLite commit cannot be recovered.

The collector sends updates through the **OpenSearch Bulk API** using scripted document updates. OpenSearch must allow scripts, have `_source` enabled, and use correct mappings for `conntrack-flows-*`. The API relies on capabilities present since OpenSearch 1.0, but compatibility with every release has not been tested.

For Dashboards, use `conntrack-flows-*` and the **`flow.started_at`** time field. Index dates are derived from the flow's UTC **start** date. See [LIFECYCLE_SETUP.md](docs/LIFECYCLE_SETUP.md) for mappings, ISM and Discover setup.

**Limitations:** no router-side application ACK, exactly-once guarantee or complete event recovery. Queue overflow, disk problems, TCP and Netlink errors can cause loss. `--max-events` limits the primary event queue, not total SQLite size. OpenSearch access should be restricted to the minimum necessary permissions.

## Features

- TCP reception of `NEW`/`DESTROY` in `nlbwmon-flow-v1` format.
- Per-connection documents with `router_name`, IP addresses, ports, protocol, timestamps and final counters.
- SQLite WAL persistence before OpenSearch delivery.
- Batched OpenSearch updates with retries.
- Optional asynchronous cached PTR enrichment.
- Python 3 implementation without third-party Python packages.
- Example systemd service and automated tests.

## Requirements and quick start

Requires Linux (Debian/systemd examples), Python 3 with standard-library `sqlite3` and `ssl`, persistent storage, and OpenSearch with scripted updates enabled.

**Start with [INSTALL.md](INSTALL.md)** for source validation, dedicated service account, credentials, systemd, OpenSearch, router connection, diagnostics and rollback. Replace all documentation placeholders before use.

```sh
python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py dns_resolver.py
python3 -m unittest -v test_dns_resolver test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
python3 flow_collector.py --help
```

Main options: `--listen` (default `127.0.0.1`), `--port` (`5517`), required `--db` and `--url`, `--bulk-size` (`100`), `--max-events` (`100000`), `--ca-file`, `--password-file` or `--api-key-file`, and optional `--dns` and `--dns-server`. Do not use `--insecure` for routine operation.

## Verification and operations

**Isolated testing on an existing OpenSearch cluster:** set `--index-prefix conntrack-flows-tests` on the test collector (default remains `conntrack-flows`). This changes **both lifecycle Bulk writes and asynchronous PTR updates** to `conntrack-flows-tests-YYYY.MM.DD`. Create a separate index template and least-privilege credentials restricted to `conntrack-flows-tests-*` before connecting the VM. Use a dedicated least-privilege test credential where feasible; if reusing an existing writer account in a lab, document the exception and verify index isolation. Do not alter production templates.


After starting, inspect the TCP 5517 listener, systemd logs, and SQLite queue. Generate a controlled test connection from the router: verify that NEW creates `active` and DESTROY changes the **same document** to `closed`. A growing queue may indicate connectivity, OpenSearch, permissions or mapping problems.

Before upgrading, record the current commit, back up the unit and make a consistent SQLite backup. Deploy a pinned tested revision and do not delete queued events during rollback.

## Security and privacy

Events contain IP addresses, ports, network identifiers, and optionally hostnames from PTR lookups. This data can be sensitive. Do not publish real configuration, addresses, passwords, keys, flow dumps or employer information.

Restrict the collector's TCP listener to trusted sources or protect it with a secure tunnel. Use HTTPS with certificate verification and least-privilege OpenSearch credentials. Audit the **entire Git history**, not just current files, before making the repository public.

## Historical aggregation mode

The repository also contains `aggregator.py`, an earlier processing mode for conntrack DESTROY text and selected JSONL events. It groups completed connections into time windows and forwards summaries to Fluent Bit, commonly through TCP `5515 → 5516`, into `conntrack-agg-*` indices.

This is **not** the new lifecycle collector installation path. The older implementation is documented in [DEPLOYMENT.md](docs/DEPLOYMENT.md) and [examples/](examples/). Do not mix its aggregated documents with `conntrack-flows-*`.

## Project layout and documentation

- `flow_collector.py` — TCP receiver and delivery coordination.
- `flow_lifecycle.py` — connection event model.
- `flow_opensearch.py`, `flow_writer.py` — OpenSearch update preparation and delivery.
- `dns_resolver.py` — PTR enrichment.
- `aggregator.py` — historical interval aggregator.
- [INSTALL.md](INSTALL.md) / [INSTALL.ru.md](INSTALL.ru.md) — installation.
- [LIFECYCLE_SETUP.md](docs/LIFECYCLE_SETUP.md) — mappings, ISM, lifecycle semantics and Dashboards.
- [FLOW_COLLECTOR_DEPLOYMENT.md](docs/FLOW_COLLECTOR_DEPLOYMENT.md) — systemd, backups and rollback.

## License and publication

The project uses the [MIT License](LICENSE). For subsequent releases, continue checking code provenance and Git history for secrets and private data, and validating installation and rollback on clean systems. The first source-only beta has been published; this does not imply a completed security audit.
