# Install the nlbwmon lifecycle collector (RC candidate)

**English** | [Русский](INSTALL.ru.md)

> [!CAUTION]
> **Experimental prerelease, not a guaranteed audit trail.** Router-to-collector TCP has no authentication, encryption or application-level ACK. Events may be lost before SQLite persistence; the queue cannot recover those events. Use a trusted network or secured tunnel and test with noncritical data first.

This guide installs the **new** Python `flow_collector.py` service, which stores received NEW/DESTROY events in SQLite WAL and sends **Bulk API** scripted updates to OpenSearch. Optional PTR enrichment runs asynchronously. It **does not require Fluent Bit**.

Router-side setup: [nlbwmon INSTALL](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/INSTALL.md) ([Русский](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/INSTALL.ru.md)).

## 1. Prerequisites

- Debian-family Linux, systemd, Python 3 with standard-library `sqlite3` and `ssl`, persistent disk space, network access to OpenSearch.
- OpenSearch with scripted updates and index creation permissions for `conntrack-flows-*`; an appropriate index template and mappings must exist **before ingestion**.
- A reviewed, **pinned commit** of this repository. Do not install from an unreviewed moving branch.
- A trusted router-to-collector path; permit TCP **5517** only from intended router addresses.

The full OpenSearch template, ISM retention and Dashboards data-view setup are in [LIFECYCLE_SETUP.md](docs/LIFECYCLE_SETUP.md). Check existing templates/policies before applying examples: **do not overwrite or delete existing indices or policies blindly**. Use `flow.started_at` as the Dashboards time field. Index names follow the UTC flow **start** date.

## 2. Install and test the source

Clone the repository to `/opt/nlbwmon-aggregator` and check out a reviewed, fixed revision. For an already installed system, **do not overwrite its working directory**; first record its current commit and back up its unit and database.

```sh
git clone https://github.com/anideth/nlbwmon-aggregator.git /opt/nlbwmon-aggregator
cd /opt/nlbwmon-aggregator && git checkout REVIEWED_COMMIT_OR_TAG
python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py dns_resolver.py && python3 -m unittest -v test_dns_resolver test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
```

## 3. Service account, storage and credentials

Run these commands as root on the **collector host**:

```sh
id flow-collector >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin flow-collector
getent group flow-secrets >/dev/null || groupadd --system flow-secrets
usermod -aG flow-secrets flow-collector
install -d -o flow-collector -g flow-collector -m 750 /opt/nlbwmon-data
install -d -o root -g flow-secrets -m 750 /etc/flow-writer
```

Create `/etc/flow-writer/password` **without putting the password in Git, shell history, commands or logs**. Give it owner `root`, group `flow-secrets`, mode `0640`. Use a least-privilege OpenSearch writer limited to `conntrack-flows-*`. For private CAs, provide the CA certificate and keep TLS verification enabled. The example uses Basic Auth; the code also supports a restricted API key.

## 4. Configure and start systemd

Copy [examples/flow-collector.service](examples/flow-collector.service) to `/etc/systemd/system/flow-collector.service`. Edit with `vim` and replace **every** placeholder: collector bind IP, OpenSearch URL, CA certificate path, writer username and credentials location. The example's `192.0.2.10` and `search.example` are **documentation placeholders**, not usable destinations. Do not expose TCP 5517 publicly.

```sh
cp /opt/nlbwmon-aggregator/examples/flow-collector.service /etc/systemd/system/flow-collector.service && vim /etc/systemd/system/flow-collector.service
systemd-analyze verify /etc/systemd/system/flow-collector.service && systemctl daemon-reload && systemctl enable --now flow-collector.service
systemctl status flow-collector.service; journalctl -u flow-collector.service -n 100 --no-pager
```

The beta.2 example adds `--max-flow-age 2505600` (29 days), to be paired with a verified 30-day ISM policy. The default is 30 days; ensure the configured maximum is strictly shorter than actual index retention. The example enables `--dns` (optional PTR) and uses `--password-file`. The code defaults to `--bulk-size 100` and `--max-events 100000`; tune only after measuring queue growth and OpenSearch throughput. **Never run two instances using the same SQLite database.** The service writes only under `/opt/nlbwmon-data`.

## 5. Connect OpenWrt and verify

On the router, configure the fork's existing UCI `flow_export` section with a unique `router_name`, collector address and **TCP port 5517**; enable only after the collector is listening and the network firewall allows the router. Follow [nlbwmon INSTALL](https://github.com/anideth/nlbwmon/blob/feature/flow-lifecycle-events/INSTALL.md) for the exact export and mark/mask caveats.

Check the listener and queue:

```sh
ss -ltn | grep ':5517'
python3 -c 'import sqlite3; c=sqlite3.connect("file:/opt/nlbwmon-data/flows.db?mode=ro",uri=True); print("events",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"dns_cache",c.execute("SELECT COUNT(*) FROM dns_cache").fetchone()[0]); c.close()'
```

In OpenSearch Dashboards, inspect `conntrack-flows-*` with time field `flow.started_at`. Generate a controlled test connection: NEW creates `flow.status=active`; DESTROY updates the **same document** to `closed` with final counters. If DESTROY is lost, `active` can remain indefinitely. Monitor journal errors and SQLite queue depth; an increasing backlog may indicate OpenSearch failure, slow delivery, or authentication/mapping errors. Optional PTR creates extra OpenSearch updates.

## 6. Upgrade, rollback and publication readiness

Before upgrading: record `git rev-parse HEAD`, back up the unit and a **consistent SQLite snapshot** (including consideration of WAL), and record OpenSearch mappings. Stop `flow-collector.service` before updating its files. Check out the tested pinned revision, run tests and restart. On rollback, restore the previous revision/unit **without deleting queued events**; verify schema compatibility.

For detailed operational checks and backup guidance, see [FLOW_COLLECTOR_DEPLOYMENT.md](docs/FLOW_COLLECTOR_DEPLOYMENT.md). The previous documentation may describe per-event OpenSearch requests; the beta-preparation code uses **Bulk API**.

**Before opening this repository:** audit all files, examples, configuration and **Git history** for credentials, private addresses, hostnames, personal data and employer information; rotate any exposed secrets. Confirm ownership and add a suitable project license, perform a clean VM installation, verify rollback and test the complete router → SQLite → OpenSearch path. **Do not make the repository public or publish a release before that review.**

## Isolated test environment

**Isolated testing on an existing OpenSearch cluster:** set `--index-prefix conntrack-flows-tests` on the test collector (default remains `conntrack-flows`). This changes **both lifecycle Bulk writes and asynchronous PTR updates** to `conntrack-flows-tests-YYYY.MM.DD`. Create a separate index template and least-privilege credentials restricted to `conntrack-flows-tests-*` before connecting the VM. Prefer separate least-privilege test credentials. If the lab reuses an existing writer credential, record the exception and verify index isolation; do not alter production templates.

### Beta.2 validation

The 2026-11-20 isolated lab test verified NEW→DESTROY in one document, stable connection identity, final counters, sequence fields and nanosecond timestamps with `date_nanos` mapping. See [the validation record and complete beta.2 index template](docs/LIFECYCLE_SETUP.md#beta2-end-to-end-validation-2026-11-20). These observations do not prove outage recovery or lossless delivery. The older `date` template must not be used for new beta.2 indices.
