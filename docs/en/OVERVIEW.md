# nlbwmon-aggregator: project overview

**English** | [Русский](../ru/OVERVIEW.md)

This project contains **two distinct conntrack processing paths**. They must not be confused during deployment.

## 1. Legacy aggregator

`aggregator.py` accepts legacy conntrack DESTROY text and `nlbwmon-flow-v1` JSONL events, groups traffic data, and forwards aggregated documents to Fluent Bit. For JSONL, DESTROY contributes counters; NEW is not double-counted.

## 2. Lifecycle collector

`flow_collector.py` accepts NEW/DESTROY events, stores them in SQLite (WAL), and updates OpenSearch documents directly. The release-preparation branch uses batched OpenSearch Bulk API delivery and supports optional asynchronous PTR enrichment. This path does not require Fluent Bit.

These services have different purposes and configurations. Verify the receiver schema before directing events to a listening port.

## Limitations and security

- The router exporter has no application-level TCP acknowledgement: receiver-side SQLite cannot recover events lost before persistence.
- Records contain IP addresses, ports, and connection metadata. Restrict access to the receiver, OpenSearch, and logs.
- Keep OpenSearch credentials out of Git and verify TLS.
- Back up working configuration and validate rollback before upgrading.

> Status: beta preparation. Deployment instructions must be checked against the final release code before they are treated as user-ready.
