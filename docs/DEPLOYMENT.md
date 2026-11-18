# Deployment and migration (Debian / OpenSearch)

**Legacy path (temporary):** `aggregator.py` accepts legacy text DESTROY and JSONL on TCP 5515, ignores JSONL NEW, aggregates completed connections, and forwards to Fluent Bit on 127.0.0.1:5516 and `conntrack-agg-*`.

**Target path:** `flow_collector.py` receives JSONL NEW/DESTROY on a separate TCP listener (example 5517), persists a SQLite outbox, and sends atomic scripted updates directly to OpenSearch `conntrack-flows-*`. One document per connection: NEW creates active, DESTROY updates it to closed. Fluent Bit is **not** required for lifecycle indexing. After all routers migrate, retire the legacy conntrack aggregation path and `conntrack-agg-*`; other Fluent Bit uses are unaffected.

## Files

- `examples/conntrack-aggregator.service` — sample systemd unit.
- `examples/fluent-bit-conntrack.conf` — Fluent Bit TCP input and OpenSearch output.

All IPs and hostnames in examples are **documentation placeholders**. Adjust them to your environment before starting the service. In particular, `203.0.113.2` is a reserved example WAN address and will not match real traffic.

## Install / rollback

On the monitoring server, preserve the currently running code and unit **before** updating:

```sh
backup_dir="/opt/conntrack-aggregator-backup-$(date +%Y%m%d-%H%M%S)" && install -d -m 700 "$backup_dir" && cp -a /opt/conntrack-aggregator/aggregator.py /opt/conntrack-aggregator/dns_resolver.py "$backup_dir/" && cp -a /etc/systemd/system/conntrack-aggregator.service "$backup_dir/" && echo "$backup_dir"
```

The current server unit may differ from the example; **preserve the existing `ExecStart` arguments** (especially `--wan-addresses`) when upgrading. The new `--legacy-router-name` option is optional and defaults to `home-router`.

Run syntax and parser tests before restarting:

```sh
cd /opt/conntrack-aggregator && python3 -m py_compile aggregator.py dns_resolver.py && python3 -m unittest -v test_aggregator
```

If the example unit is used for a **new** deployment, copy it to `/etc/systemd/system/conntrack-aggregator.service`, edit with `vim`, set the actual network ranges and WAN addresses, then run:

```sh
systemctl daemon-reload && systemctl enable --now conntrack-aggregator.service && systemctl status conntrack-aggregator.service
```

For an existing deployment, review differences first and restart only after tests pass. To roll back, restore the backed-up Python files and unit, run `systemctl daemon-reload`, then `systemctl restart conntrack-aggregator.service`.

## Fluent Bit

The sample Fluent Bit config listens **only on 127.0.0.1:5516**. The aggregator alone listens on the router-facing port 5515. Include the file from your Fluent Bit main configuration, if it is not already included.

Change the OpenSearch hostname, credentials and TLS trust settings. The example uses `TLS.Verify On`; if your existing environment has `TLS.Verify Off`, fixing certificate trust should be a separate, deliberate change rather than part of this migration. **Never commit a real password or production network addresses.**

Check the Fluent Bit config against your installed version before restarting. Do not overwrite an existing working config without a backup.

## Router identity

Legacy text events receive `router_name=home-router` by default, configurable with `--legacy-router-name`. JSONL events carry `router_name` from the router's `/etc/config/nlbwmon` `flow_export` section. JSONL `NEW` events are ignored for aggregate byte counts; only `DESTROY` contributes.

TCP export is plaintext and best-effort. Restrict port 5515 to trusted routers at the firewall, and keep OpenSearch credentials out of source control.

## Non-disruptive lifecycle collector rollout

Do not replace or restart the working legacy listener on TCP 5515, Fluent Bit on 5516, or any separate lab listeners during initial validation. Use a **different port** for the new collector, and configure the new router's `flow_export.tcp_port` accordingly only after the collector passes tests. The sample below binds to loopback for local TCP testing; to accept router traffic, bind to an appropriate interface and restrict it to trusted routers via firewall.

Run all lifecycle tests before starting:

```sh
cd /opt/nlbwmon-aggregator && python3 -m py_compile flow_collector.py && python3 -m unittest -v test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
```

Create a persistent data directory outside the Git checkout and give the eventual service user write access (example location, not hard-coded):

```sh
install -d -m 750 /opt/nlbwmon-data
```

Start manually for lab verification, replacing example hostname and CA/password locations. No SQLite CLI package is required; Python creates the database and schema automatically:

```sh
cd /opt/nlbwmon-aggregator && OPENSEARCH_USER=writer python3 -u flow_collector.py --listen 127.0.0.1 --port 5517 --db /opt/nlbwmon-data/flows.db --url https://search.example:9200 --ca-file /etc/ssl/certs/private-ca.pem --password-file /etc/flow-writer/password
```

The OpenSearch index template must map `flow.started_at` and `flow.ended_at` as dates, `connection_id` as keyword, and `flow.duration_seconds` as long. Configure retention and access controls independently. Index naming is based on the UTC **start** date, not end date.

**Critical Dashboards setting:** create/select index pattern `conntrack-flows-*` with **time field `flow.started_at`**. Do not select `flow.ended_at`: NEW records have no end time and will be invisible in Discover. With the correct time field, NEW appears as `active`; after DESTROY, the **same document** becomes `closed` with end time and counters. Refresh Discover to see changes. The record remains visible while its **start** is inside the selected time range. Existing index patterns may need to be recreated to change the time field; deleting an index pattern does not delete indexed documents, but can affect saved visualizations.

Observe the collector's `delivered ... result=created|updated|noop` log, inspect the queue with Python's `sqlite3`, and verify the document directly via OpenSearch Dev Tools before interpreting an empty Discover result. A successful TCP client `sendall()` alone does not prove OpenSearch delivery. See [FLOW_WRITER.md](FLOW_WRITER.md) for queue diagnostics, guarantees, and failure cases.

After validating NEW and DESTROY end-to-end, deploy a dedicated systemd unit with a restricted user, writable DB directory, secure password file, automatic restart and log supervision. **No systemd unit for the new collector has been installed by this guide.** Do not cut over the production router before these checks.

Rollback during migration: switch the test router's export back to its previous endpoint, stop the standalone collector, and leave the legacy aggregator and Fluent Bit untouched. Preserve the SQLite database until its queue is empty; stopping the collector does not erase it. Once every router is migrated and retention/rollback are reviewed, retire legacy conntrack services and indices deliberately rather than deleting them during initial rollout.

## Current validation status (2026-11-14)

The existing 19 parser/OpenSearch/writer tests and 5 collector tests passed on the monitoring host. A manual TCP NEW test was acknowledged as `result=created`, SQLite queue became empty, and a direct OpenSearch query found the document in `conntrack-flows-2026.11.14`. Discover initially hid it because the index pattern used `flow.ended_at`; changing the time field to `flow.started_at` made NEW visible. A synthetic DESTROY updated the same OpenSearch document (version 2, status closed, 60-second duration and final counters). SQLite queue recovery was verified after graceful stop and SIGKILL, with OpenSearch intentionally unreachable; on reconnect the queued event was delivered and the queue emptied. Real router cutover, systemd restart and production load remain unverified.

## Dedicated service account (Debian-family example; 2026-11-14)

Manual setup only; commands vary by distribution. Run as root after stopping any manually launched collector using the same database. Do **not** run two collectors against one queue.

```sh
id flow-collector >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin flow-collector
getent group flow-secrets >/dev/null || groupadd --system flow-secrets
usermod -aG flow-secrets flow-collector
chown -R flow-collector:flow-collector /opt/nlbwmon-data && chmod 750 /opt/nlbwmon-data && chmod 600 /opt/nlbwmon-data/*.db
chgrp flow-secrets /etc/flow-writer /etc/flow-writer/password && chmod 750 /etc/flow-writer && chmod 640 /etc/flow-writer/password
runuser -u flow-collector -- python3 -c 'import sqlite3; c=sqlite3.connect("/opt/nlbwmon-data/flows.db"); print("SQLite:",c.execute("PRAGMA quick_check").fetchone()[0]); c.close(); open("/etc/flow-writer/password").read(); print("Password: readable")'
```

The checkout remains root-owned and readable by the service account. SQLite WAL/SHM files require write access to the DB directory. The password remains root-owned and group-readable only. Review directory paths, existing ownership and any other users of the credential before applying these commands. A successful read check is **not** a write or systemd test. No service unit or installer script is committed yet; systemd is optional and distribution-specific.


## Current lifecycle installation guide (supersedes earlier lab-only status)

See **[LIFECYCLE_SETUP.md](LIFECYCLE_SETUP.md)** for the complete verified NEW/DESTROY pipeline, field definitions, OpenSearch index template and ISM examples, Dashboards data view setup, systemd unit, troubleshooting and production migration checklist.

**Update 2026-11-14:** The lab OpenWrt was switched to collector TCP 5517. The dedicated systemd service is enabled and active, listening on the monitoring host LAN interface. A real iperf3 flow was observed first as NEW/active and then DESTROY/closed with the **same OpenSearch _id**, document `_version=2`, duration 101 seconds and cumulative counters. This supersedes earlier statements above that systemd or live router lifecycle was not tested. The legacy production pipeline remains unchanged; production router migration, prolonged soak, router-to-collector loss testing and TLS verification hardening remain outstanding.
