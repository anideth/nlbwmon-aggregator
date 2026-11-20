# Lifecycle collector: reproducible Debian/systemd deployment (beta preparation)

This guide describes the **separate** `flow_collector.py` lifecycle service. It does not require Fluent Bit. The companion OpenWrt fork is https://github.com/anideth/nlbwmon .

## Requirements and data path

- Debian-family Linux with systemd, Python 3 standard library (including `sqlite3` and `ssl`), persistent local storage for SQLite WAL, and network connectivity to OpenSearch.
- OpenSearch must permit scripted document updates and have a `conntrack-flows-*` template with correct mappings. Consult [LIFECYCLE_SETUP.md](LIFECYCLE_SETUP.md) for mapping and ISM examples; do not overwrite existing policies without review.
- OpenWrt nlbwmon fork emits `nlbwmon-flow-v1` newline-delimited JSON `NEW` and `DESTROY` to TCP 5517. Router-to-collector TCP is **plaintext, unauthenticated and best-effort**: bind only on a trusted interface, firewall it to router IPs, or use a secure tunnel. There is no application ACK or end-to-end losslessness guarantee.
- OpenSearch TLS validation is enabled by default; prefer `--ca-file` for a private CA. `--insecure` disables server verification and must not be a recommended production default.
- `flow.started_at` is the Dashboards time field. Indices use UTC **start date**; closed flows update the same document ID.
- PTR is optional (`--dns`); repeated `--dns-server` selects DNS servers; default uses the system resolver. Cache is in-process: 24h positive, 1h negative, max 50k entries; it is not a shared/durable cache. PTR creates additional OpenSearch updates and a durable `dns_jobs` queue.

## Install from a pinned source revision

1. Pin a reviewed Git commit/tag and clone the repository to `/opt/nlbwmon-aggregator`; do not use an unreviewed moving branch for production updates. Run `python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py dns_resolver.py` and `python3 -m unittest -v test_dns_resolver test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector`.
2. Create a dedicated non-login account and restricted directories. Example commands, run as root:

```sh
id flow-collector >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin flow-collector
getent group flow-secrets >/dev/null || groupadd --system flow-secrets
usermod -aG flow-secrets flow-collector
install -d -o flow-collector -g flow-collector -m 750 /opt/nlbwmon-data
install -d -o root -g flow-secrets -m 750 /etc/flow-writer
```

3. Create `/etc/flow-writer/password` **without placing its contents in Git, shell history or logs**. Set owner/group `root:flow-secrets`, directory mode `0750` and password file mode `0640`. Create a least-privilege OpenSearch writer restricted to `conntrack-flows-*`. For an existing SQLite database, stop all collector instances before changing ownership; preserve its `-wal` and `-shm` files and use the SQLite backup API for consistent online backups.
4. Copy [the example systemd unit](../examples/flow-collector.service) to `/etc/systemd/system/flow-collector.service`. Edit the placeholders for the collector bind address, OpenSearch endpoint, CA path, writer username, and optional DNS resolvers. Do not embed a password or private network address in the public repository. Do not bind to `0.0.0.0` without an explicit firewall policy.
5. Validate `systemd-analyze verify /etc/systemd/system/flow-collector.service`, then `systemctl daemon-reload && systemctl enable --now flow-collector.service`. Inspect `systemctl status flow-collector.service` and `journalctl -u flow-collector.service -n 100 --no-pager`. Verify TCP listener and OpenSearch `NEW`/closed `DESTROY` behavior with controlled test traffic **before** cutting over a router.

The service uses `ProtectSystem=strict` with `ReadWritePaths=/opt/nlbwmon-data`. Code checkout and credentials remain read-only to the daemon. One service instance per SQLite outbox; do not run two workers against the same database.

## Verify queues and mappings

```sh
python3 -c 'import sqlite3; c=sqlite3.connect("file:/opt/nlbwmon-data/flows.db?mode=ro",uri=True); print("events",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"dns_jobs",c.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0]); c.close()'
```

```http
GET conntrack-flows-*/_mapping/field/source.ip,destination.ip,source.reverse_dns,destination.reverse_dns
GET _cat/indices/conntrack-flows-*?v&h=index,docs.count,docs.deleted,pri.store.size
```

The lifecycle event worker sends ordered batches through the OpenSearch Bulk API (default `--bulk-size 100`); only an acknowledged successful prefix is removed from SQLite, and a failed suffix remains queued. The service can accumulate a backlog if OpenSearch or DNS is unavailable. `--max-events` limits the primary event queue, not the `dns_jobs` table. Beta.2 adds `--max-flow-age` filtering of events by flow start time when processing the SQLite outbox (default 30 days); use 29 days (`2505600`) only when the ISM retention and timing margin have been reviewed. This does not expire existing `active` documents. No per-network CIDR exclusion is implemented. An `active` OpenSearch document is not proof that the conntrack entry is still alive.

## Upgrade and rollback

Before upgrades, record `git rev-parse HEAD`, back up the existing unit and a **consistent** SQLite database snapshot, preserve the current credential file permissions and the OpenSearch template. Restart only the lifecycle collector being upgraded. Update code to a **tested, pinned commit**, run tests, inspect unit diff, then restart only the lifecycle collector. Roll back by checking out the previously recorded commit and restoring the previous unit; keep the durable queue, verify schema compatibility and never delete queued events to force an upgrade.

## Current validation and release limitations (2026-11-15)

A real router has produced live NEW records with PTR names and a live workstation TCP connection was independently confirmed by `ss`. Index size after more than 12 hours was reported at approximately 400 MB; one-day ~1 GB is an estimate, not a benchmark. The complete chain's CPU, RSS, throughput, dropped events, backlog growth, and prolonged DESTROY completeness are not yet quantified.

**Beta release gate:** audit code/license provenance, verify license and code provenance, test clean installation on a new Debian VM, verify no credentials or private IPs in examples, test rollback and controlled NEW/DESTROY with DNS enabled, and verify OpenSearch mappings. Only then tag the tested commit and publish a GitHub prerelease. The tag alone is not a deployment validation.

## Beta.2 verification update (2026-11-20)

An isolated end-to-end test confirmed stable `_id` through NEW/DESTROY, precise `flow.duration_ns`, `date_nanos` mapping, exporter instance and event sequences, and final counters. The index template must map `flow.started_at` and `flow.ended_at` as `date_nanos`, `flow.duration_ns` and both sequence fields as `long`, and `exporter.instance_id` as `keyword`. The full template, test-index instructions and remaining resilience checks are in [LIFECYCLE_SETUP.md](LIFECYCLE_SETUP.md#beta2-end-to-end-validation-2026-11-20). Existing indices retain their original mapping; update templates for future indices or recreate **only disposable test** indices after stopping ingestion.
