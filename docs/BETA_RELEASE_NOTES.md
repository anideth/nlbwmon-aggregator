# Draft beta release notes — nlbwmon lifecycle collector

**Status: DRAFT / NOT PUBLISHED.** No version/tag has been published. The repository currently contains an MIT LICENSE; code provenance still needs verification. Publication remains blocked pending owner-approved license and Git-history privacy audit.


## Lifecycle semantics and dependencies

- NEW creates `active`; no intermediate conntrack UPDATE events or periodic traffic counter snapshots are written. DESTROY updates the same document to `closed`, recording final counters and duration. A DESTROY without NEW can create a closed document; duplicate/late NEW after closed is ignored. PTR enrichment may separately update reverse DNS.
- Missing DESTROY leaves the document `active` indefinitely, subject to index retention; **no seven-day timeout is currently implemented**. Next release proposal: configurable 7-day stale detection (default `604800` seconds), scan interval `3600`, disable with zero, status `stale`, and late `stale -> closed`. Flags are **not yet available**.
- SQLite `WAL`/`synchronous=FULL` stores accepted event payloads and PTR jobs as a durable retry outbox, **not** a live-connection state table; events can be lost before SQLite receipt.
- OpenSearch uses Painless `scripted_upsert` via the Update Document API, [documented as introduced in 1.0](https://docs.opensearch.org/latest/api-reference/document-apis/update-document/). `_source` and script execution are required. API availability is not a guarantee of project compatibility across all versions.

## New functionality
- Dedicated `flow_collector.py` ingests `nlbwmon-flow-v1` NEW/DESTROY JSONL from the [OpenWrt nlbwmon fork](https://github.com/anideth/nlbwmon).
- SQLite WAL durable event outbox **after successful receipt**, direct scripted OpenSearch updates to `conntrack-flows-YYYY.MM.DD`, stable connection document identity.
- Optional asynchronous cached PTR enrichment with `source.reverse_dns` and `destination.reverse_dns`.
- Example hardened systemd unit, OpenSearch index template/ISM examples, [end-to-end installation guide](INSTALLATION_END_TO_END.md) and [collector deployment/rollback guide](FLOW_COLLECTOR_DEPLOYMENT.md).
- Existing `aggregator.py` / Fluent Bit `conntrack-agg-*` path is separate and is not automatically replaced.

## Live observations
On 2026-11-16, user-provided OpenSearch screenshot showed three green indices: 2026.11.14 424630 docs / 109.7 MB, 2026.11.15 1671311 docs / 432.1 MB, 2026.11.16 1027299 docs / 637.7 MB (partial day). Total 3123240 docs, ~1179.5 MB. All indices had 1 primary and 0 replicas, managed by a policy. This demonstrates multi-day ingestion but **does not** prove lossless NEW/DESTROY pairing or measured 24h rate. `docs.deleted` may grow due to OpenSearch document updates.

## Known limitations
- Router->collector TCP has no application ACK, no built-in authentication/encryption; restrict the listener to trusted senders.
- Stale `active` documents possible if DESTROY is lost. No stale-flow expiry.
- No CIDR exclusion in this beta: **planned for next release**, default capture preserved.
- Primary SQLite event queue is bounded by `--max-events`; PTR job outbox has separate limits; batched OpenSearch Bulk API updates, possible backlog.
- TLS `--insecure` is not an acceptable public production default. Verify a trusted CA and least-privilege writer.
- No guarantees of universal NAT attribution, full-day CPU/RSS budget or exactly-once ingestion.

## Release gate
- [x] MIT LICENSE present on beta preparation branch; [ ] owner verifies external code provenance and redistribution rights.
- [ ] Audit current files, **all branches and relevant Git history** for credentials and private infrastructure details; old deleted Markdown persists in history until rewritten.
- [ ] Execute tests on exact release revision, clean Debian VM installation, OpenSearch mappings, TLS, PTR, restart and rollback.
- [ ] Obtain user-modified Grafana JSON and verify author credit/license; do not invent or copy an unprovided dashboard.
- [ ] Tag tested commit, publish GitHub pre-release and verify all links.
