# nlbwmon-aggregator v0.1.0-beta.1 — source-only beta

> **Release candidate notes.** The annotated Git tag and GitHub Release must be created after final verification. This document does not itself publish a release.

Experimental Python companion to the [nlbwmon OpenWrt fork](https://github.com/anideth/nlbwmon). Receives `nlbwmon-flow-v1` NEW/DESTROY JSONL events over TCP, persists received events in SQLite WAL and forwards updates to OpenSearch using the Bulk API. Optional PTR enrichment runs asynchronously. No Fluent Bit is required for the lifecycle collector.

## Distribution and installation

**Source code only; no binaries or APKs attached.** Run the Python collector on a separate Linux system using the documented systemd service and OpenSearch index template. See [INSTALL.md](INSTALL.md), [INSTALL.ru.md](INSTALL.ru.md), [end-to-end guide](docs/INSTALLATION_END_TO_END.md), and [deployment guide](docs/FLOW_COLLECTOR_DEPLOYMENT.md). The router firmware and APK are built separately in the nlbwmon repository.

## Validation

Python unit/integration tests passed in GitHub Actions on Python 3.11, 3.12 and 3.13 in the clean repository before release preparation. The lab confirmed event ingestion into test OpenSearch indices. Neither finding proves end-to-end delivery completeness or production readiness.

## Important limitations

Router-to-collector TCP is **unencrypted, unauthenticated, and lacks application-level acknowledgements**. Events can be lost before SQLite persistence, and no durable queue can reconstruct events never received. Use a trusted network or secured tunnel. Validate OpenSearch mappings, TLS, privileges, backups and recovery. Do not use as a sole audit, billing, compliance or security source. No independent security audit or broad compatibility testing has been completed.

## Authorship and license

MIT-licensed companion project, developed with substantial AI assistance and tested in a personal lab. See [README](README.md) for scope and maturity. The old private development archive is separate from this clean public repository and is not part of this release.
