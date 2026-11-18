# OpenSearch lifecycle writer and TCP collector (experimental)

The independent `flow_writer.py` reads `nlbwmon-flow-v1` JSONL from standard input and performs atomic OpenSearch scripted upserts into `conntrack-flows-YYYY.MM.DD`. It has no TCP listener or durable queue by itself. For live router traffic, `flow_collector.py` provides a TCP listener, persistent SQLite outbox, and calls `flow_writer.send_update()` directly. During migration the old aggregator and Fluent Bit continue independently.

## TLS and authentication

| Option | Behavior |
| --- | --- |
| `--url https://search.example:9200` | HTTPS; validates server certificate and hostname using the OS trust store by default |
| `--ca-file /path/ca.pem` | Trust an additional/custom PEM CA bundle instead of the default system bundle |
| `--client-cert /path/client.pem` | Present a client certificate for mutual TLS |
| `--client-key /path/client.key` | Private key for mTLS (optional if included in client PEM) |
| `--insecure` | Disable certificate and hostname validation; **lab use only** |
| `--url http://127.0.0.1:9200` | Plain HTTP for trusted local networks; warns that credentials and telemetry are unencrypted |
| `OPENSEARCH_USER` and `OPENSEARCH_PASSWORD` | Optional HTTP Basic authentication |
| `OPENSEARCH_USER` and `--password-file /secure/path` | Basic authentication with a password stored outside the process command line |
| `--api-key-file /secure/path` | Optional `Authorization: ApiKey` token, mutually exclusive with Basic Auth |

The writer connects directly and ignores `HTTP_PROXY`/`HTTPS_PROXY` environment variables. Do not put credentials into the URL. Keep secret files restricted (e.g. mode 0600), outside Git, and avoid entering passwords into shell history. Use a least-privileged OpenSearch role scoped to `conntrack-flows-*`.

### Examples

Verified HTTPS with a custom CA and password file:

```sh
OPENSEARCH_USER=writer python3 flow_writer.py --url https://search.example:9200 --ca-file /etc/ssl/certs/private-ca.pem --password-file /etc/flow-writer/password < events.jsonl
```

Verified HTTPS with client certificate and key:

```sh
python3 flow_writer.py --url https://search.example:9200 --client-cert /etc/flow-writer/client.crt --client-key /etc/flow-writer/client.key < events.jsonl
```

Lab with an untrusted/self-signed certificate:

```sh
OPENSEARCH_USER=writer python3 flow_writer.py --url https://127.0.0.1:9200 --insecure --password-file /etc/flow-writer/password < events.jsonl
```

OpenSearch indices and mappings must already exist, or index auto-creation must be allowed. Index dates derive from the flow start time in UTC, not ingestion time.

## Flow duration mapping

For DESTROY, `flow.duration_seconds` is the nonnegative integer difference
between exporter-provided `finished_sec` and `started_sec`. NEW does not
include a duration. The calculation works even if DESTROY arrives without NEW.
Already closed flows remain unchanged by subsequent NEW or DESTROY events.

Add the following field under `flow.properties` in the OpenSearch index
template `conntrack-flows-template` and in existing index mappings:

```json
"duration_seconds": {"type": "long"}
```

Updating the Python code does not automatically modify OpenSearch mappings.
Existing documents are not retroactively backfilled; reindex or update them
explicitly if needed.

## Operational limitations

- **Standalone writer only:** `flow_writer.py` has no durable queue and consumes stdin. Use `flow_collector.py` for a persistent SQLite queue, subject to the limitations below.
- Retries apply to transient HTTP 429/502/503/504 and transport errors. A terminal error is counted and produces a nonzero process exit code at EOF.
- `flow_writer.py` has no TCP listener or supervision. `flow_collector.py` supplies a TCP listener and persistent queue, but no bundled systemd unit yet, no automatic stale-flow expiry and no router-side delivery acknowledgement.
- If upstream input contains personal/internal IPs, it is sensitive telemetry. Use transport encryption and retention controls.
- Test offline: `python3 -m unittest -v test_flow_writer`.

## Lifecycle document and Discover visibility

The target model is **one document per connection**, not one document per event, in the single index family `conntrack-flows-*`. `router_name` and `connection_id` determine a stable document ID. The index date is based on `flow.started_at` in UTC, including when DESTROY crosses midnight.

| Event | Same document | Fields |
| --- | --- | --- |
| NEW | Create if absent | `event_type=NEW`, `flow.status=active`, `flow.started_at`; no `flow.ended_at` |
| DESTROY | Update existing or create closed if NEW was missed | `event_type=DESTROY`, `flow.status=closed`, `flow.ended_at`, `flow.duration_seconds`, directional bytes/packets |
| Duplicate or late NEW/DESTROY after closed | No-op | A closed document cannot become active again |

The OpenSearch index pattern / Dashboards data view **MUST use `flow.started_at` as its time field**, NOT `flow.ended_at`. Otherwise Discover hides all active NEW connections because they have no end timestamp. With `flow.started_at`, the NEW document appears immediately (after refresh); when DESTROY arrives, the **same row/document** becomes closed and retains its original start time. Set Discover's time range to include the **start**, clear conflicting filters, and refresh or enable auto-refresh. A long-lived connection started before the selected time window will not appear even if it just ended. `flow.ended_at` remains available as a normal field for completed connections, not as the data view time filter.

This design records start and end *in one mutable document*; it does not retain a separate immutable NEW event row after DESTROY.

## Standalone TCP collector and persistent queue

`flow_collector.py` accepts newline-delimited `nlbwmon-flow-v1` NEW/DESTROY over TCP, validates each line, inserts normalized events into a SQLite database, then a worker posts atomic scripted upserts to OpenSearch. It does **not** use Fluent Bit for this lifecycle path. Python's standard-library `sqlite3` module is sufficient; the `sqlite3` command-line package and a separate database server are not required.

Example, with documentation-only hostnames and an existing restricted password file:

```sh
OPENSEARCH_USER=writer python3 -u flow_collector.py --listen 127.0.0.1 --port 5517 --db /opt/nlbwmon-data/flows.db --url https://search.example:9200 --ca-file /etc/ssl/certs/private-ca.pem --password-file /etc/flow-writer/password
```

`--db` is **required and configurable**; it is not hard-coded. The process creates the parent directory, SQLite database, `events` table and index automatically if filesystem permissions allow. Keep the DB outside the Git checkout on persistent storage, with write permission for the service account. SQLite uses WAL and `synchronous=FULL`; adjacent `-wal` and `-shm` files are normal. Preserve all three during a live backup, or use SQLite's backup API. Avoid deleting or relocating the DB while a worker is active.

The worker removes an event from SQLite **only after** OpenSearch confirms `created`, `updated`, or `noop`. Queued events survive normal process restarts and OpenSearch outages. If OpenSearch applied an update but the collector crashed before deleting the queued event, replay is safe due to the idempotent scripted update. The worker currently sends one HTTP request per event, in queue order; no Bulk API or throughput guarantee is implemented.

**Important limits:** TCP has no application-level ACK to the router exporter, so a crash between socket receipt and SQLite commit can lose an event. Queue saturation (`--max-events`, default 100000) disconnects a sender and can lose that event; previously queued events are retained. Invalid or oversized input lines are discarded. An inaccessible DB or disk-full condition prevents durable acceptance. Failed HTTP deliveries are retained and retried, including permanent authentication/mapping errors, which can block later queued events. No automatic stale-flow expiry, deduplication of queue rows, dead-letter queue, or delivery metrics yet. Protect plaintext router-to-collector TCP with a trusted network/tunnel and firewall; prefer verified HTTPS to OpenSearch. `--insecure` is for lab testing only.

Check the queue without requiring a SQLite CLI:

```sh
python3 -c 'import sqlite3; c=sqlite3.connect("/opt/nlbwmon-data/flows.db"); print("journal",c.execute("PRAGMA journal_mode").fetchone()[0],"queued",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"integrity",c.execute("PRAGMA quick_check").fetchone()[0]); c.close()'
```

Run offline tests:

```sh
python3 -m unittest -v test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
```

The collector tests cover TCP insertion, DB reopen, queue limit, failed delivery/restart and selective deletion with a mocked OpenSearch sender. They do not prove end-to-end losslessness or production throughput. An actual test on 2026-11-14 confirmed a NEW sent over TCP, a `created` OpenSearch response, and an emptied SQLite queue; live DESTROY integration and router cutover still require validation.
