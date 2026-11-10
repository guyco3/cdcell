# cdcell v1 design (revision 5)

cdcell turns the SQLite changes of celld Durable Objects into an append-only
**change log** in Apache Iceberg tables in the customer's object storage. It
reads only what celld already writes to its bucket. Customers build current
state (SCD type 1) and history (SCD type 2) with their own engine
(ClickHouse, Trino, Spark, …): see [`consumer-guide.md`](consumer-guide.md).

Revision 5 simplifies revisions 1-4: no CloudEvents/Debezium envelope, no
before images, no copies of celld state in the customer bucket. History of
the earlier revisions is in git. Facts about celld, object storage and
notifications are in [`invariants.md`](invariants.md), cited by ID.

## 1. Contract

**cdcell guarantees, per Durable Object (cell):**
- **Complete and truthful:** every change that is part of the cell's restored
  history appears in the change log; nothing outside it (no phantom writes
  from fenced nodes, D9-D11) ever does.
- **Ordered:** each row carries `(__epoch, __txid)`, the cell's own commit
  order. Compare as a pair. Rows of a cell are written in that order.
- **Net per capture batch:** one row per changed key per LTX unit (a TXID or
  a merged range). A unit never splits a SQLite transaction but can merge
  several (B3, B4).
- **At least once:** in normal operation each change is written once. After a
  cdcell crash a bounded window can be written again; duplicates are
  identical in `(__cell_id, __key, __epoch, __txid)` or carry a later version.

**cdcell does not:** run the customer's engine, maintain type-1/type-2 tables
(they derive them), install anything in celld or the Durable Objects, or
promise real time (target: under 2 minutes; minutes are fine).

## 2. Output: the change log

One append-only Iceberg (format v2) table per `(script, class, SQLite
table)`: `cdcell.<script>.<class>.<table>`, partitioned by `day(__ts)`,
`bucket(16, __cell_id)`.

| Column | Type | Meaning |
|---|---|---|
| `__cell_id` | string | the Durable Object, `Class:<64 hex>` |
| `__epoch`, `__txid` | long | order key, compare as a pair |
| `__op` | string | `c` insert, `u` update, `d` delete, `r` initial state (bootstrap) |
| `__key` | string | canonical JSON array of the primary key (rowid when none) |
| `__ts` | timestamptz | capture time from the LTX header (owner's clock; display only, B9) |
| `__schema_version` | long | the cell's table schema version (§5) |
| `__ingested_at` | timestamptz | when cdcell wrote the row |
| *table columns* | by affinity | after values; on `d` only the key columns |
| `__raw` | map<string,string> | values that do not fit their column type, JSON-encoded |

- Type mapping by declared affinity: INTEGER → long, REAL/NUMERIC → double,
  TEXT → string, BLOB → binary, untyped → string (canonical JSON scalar).
  A value of another storage class goes to `__raw` and its column is null.
- User columns starting with `__` are written as `u__…`.
- The Durable Object KV API (`_cf_KV`) becomes `_cdcell_kv`: columns `k`,
  `value_type` (`json`|`v8`|`stub`|`legacy_text`), `value_json` (V8 values as
  tagged JSON: `$date`, `$bigint`, `$map`, `$set`, `$bytes`, `$ref`, …),
  `value_raw`.
- Internal tables are not emitted: `_litestream_*`, `_cf_WAKE`, `_cf_FACETS`,
  `_cf_METADATA`, `_cf_ALARM`, `sqlite_*`, virtual tables.
- `cdcell.<script>._cdcell_progress`: one row per touched cell per commit
  `(_cell_id, _epoch, _txid, captured_at, committed_at, batch)`: "complete up to here".

## 3. Architecture

```
celld bucket ──notifications──► broker (SQS | NATS JetStream)
   ▲  GET objects, own.json, nodes/*.json, rare LIST          │
   └──────────────────────────────── cdcell worker
        intake → per-cell: order, confirm, apply, decode → committer ──► Iceberg REST catalog ──► customer bucket
                                                                          _cdcell/ (checkpoint, manifests, lease)
```

1. **Intake:** URL-decode the key; drop what the key alone allows (L1/L9 keys
   are kept as repair candidates; facets skipped in v1); GET the object
   promptly and concurrently (bundles are GC'd ~30-40 s after coverage, C8);
   split bundles into per-cell units.
2. **Per cell:** buffer units by `(epoch, min_txid)`; confirm (§4); apply in
   order to the cell's in-memory state; decode the changed pages into row
   changes (after values, deletes, insert vs update).
3. **Committer:** every `CDCELL_COMMIT_INTERVAL_S` (60 s; up to an hour is
   fine): schema evolution (§5), then one atomic multi-table commit (§6).

## 4. Correctness: which units are history

A unit is history once every future restore must contain it. A successor
CASes a record before it reads data (D5, D6), so if cdcell sees the data and
afterwards reads that record unchanged, the data is history.

| Unit from | Read after the GET | Confirmed if |
|---|---|---|
| bundle `log/<node>/<gen>/bundle/e<LE>-…` | `nodes/<node>.json` | same generation, `log.state == open` at a log epoch ≥ LE (C12) |
| per-cell file `cells/<cell>/ltx/e<E>/…` | `cells/<cell>/own.json` | `epoch == E` |

- Units are applied only after confirmation, so the state never holds a
  phantom and nothing is ever undone. Reads are batched (~1 s rounds).
- **Suspects** (a record moved) wait in memory and are resolved by the next
  epoch that has an opener:
  - a paged marker `(c+1)-(c+1)`: units with max ≤ c are history, above c
    are phantoms (after proving no intermediate epoch has objects);
  - a TXID-1 snapshot or any later full image: diff it against the state and
    emit the net change (synthetic rows at the new `(epoch, txid)`);
  - no opener yet: keep waiting (`cdcell_parked_cells`).
- **Gaps** never get skipped: units wait until the missing TXIDs arrive;
  after `CDCELL_GAP_TIMEOUT_S` they are fetched from the per-cell prefix
  (every level: L0, L1, L9) or from live bundles; tail loss is caught by a
  periodic LIST of live sessions' bundles.
- **Epoch changes:** a new epoch's TXID-1 snapshot (or L1 from 1) is diffed
  against the state; ranges (merged L0, L1 folds) apply as net changes.

## 5. Worker state and schemas

**Per cell, in memory:** the decoder state (currently the cell's pages at the
confirmed point; a key-only state is planned, §9), the schema cookie, and
per table its columns and fingerprint.

**Bootstrap** (a cell first seen, or after a restart): rebuild from the celld
bucket. Take the newest full image of the cell's current epoch (TXID-1
snapshot, L1 from 1, L9) and the objects after it, up to the committed point
when restarting. Nothing of celld's state is copied elsewhere.

**Schemas live in the database itself** (`sqlite_schema`, page 1; the schema
cookie changes on every DDL), so they arrive in the same ordered stream:
1. When a unit changes the schema cookie, re-read `sqlite_schema` and load
   column metadata (SQLite `PRAGMA table_xinfo`). Rows in the same unit
   already use the new schema.
2. Fingerprint each table: the normalized column list (names, declared
   types, PK positions, NOT NULL, defaults, WITHOUT ROWID).
3. **Registry** per output table: `fingerprint → __schema_version`, stored in
   the Iceberg table property `cdcell.schemas`, assigned in order of first
   sight. Cells that reached the same columns by different migrations share a
   version.
4. **Iceberg evolution** before the commit that needs it: add missing columns
   (nullable); a value whose type conflicts with an existing column goes to
   `<col>__<type>`; dropped columns stay (null from then on); a renamed
   column is added under its new name.

celld facts this rests on (verified by experiment and code): a deploy never
changes cell ids, class identity or epochs; a cell's schema changes only when
its own code runs DDL (lazily, on its next event); cells of one class can
keep different schemas indefinitely; `PRAGMA user_version` is not available
to Durable Objects.

## 6. Commits and crash recovery

Per batch:
1. Schema evolution and registry update per touched table (additive).
2. Stage every table's Parquet data files, then a create-only batch manifest
   `_cdcell/batches/<seq>.json` (per-cell committed points), then **one atomic
   multi-table commit** through the catalog (Lakekeeper
   `/transactions/commit`, verified atomic).
3. Checkpoint `_cdcell/checkpoint.json` (per cell: committed `(epoch, txid)`),
   then acknowledge the notifications.

Restart: read the checkpoint, roll forward any manifest whose batch is
visibly committed (snapshot property `cdcell.batch`), then rebuild cells
lazily from celld at their committed points. If the exact point is no longer
reconstructible (bundles GC'd, only a coarser L1 remains), rebuild at the
best earlier point and re-emit the window up to the committed point. If the
only full image ends *beyond* the committed point (celld folded the history
into an L1 while cdcell was down), **resync**: emit the image's rows as
updates plus deletes for keys live in the cell's own change log but absent
from the image, as one net change at the image's `(epoch, txid)`
(`tests/m5-l1-resync.sh`). If the committed epoch itself is gone (celld's
opt-in epoch GC, `CELLD_LTX_RETENTION_SECS`), resync the same way from the
newest full image in the owner's current epoch named by `own.json`; that
epoch is history, phantoms only live in fenced owners' older epochs
(`tests/m5-epoch-gc.sh`). Intermediate versions in that window are lost;
the final state is exact.

A global lease (`_cdcell/lease.json`, conditional PUT, TTL) lets one process
commit; a lost renewal stops the process.

## 7. Operating requirements

cdcell's guarantees (§1) hold only while the deployment provides these.
Breaking one is outside the contract, not a cdcell bug:

1. **Notification coverage.** Every object created under `cells/` and `log/`
   in the celld bucket produces a notification to cdcell's queue.
2. **Queue retention.** The queue keeps unacknowledged notifications longer
   than cdcell's longest outage (SQS allows up to 14 days; JetStream: the
   stream's `max_age`).
3. **Read access** to the celld bucket per `deploy/iam/celld-read-policy.json`,
   and read/write to the lake bucket and the Iceberg catalog.
4. **One cdcell process per celld deployment** (v1). A second one is fenced
   by the lease but adds nothing.
5. **celld's storage guarantees** (strong read-after-write, list-after-write;
   celld `docs/guarantees.md`).

**History detail is a choice, not a requirement.** cdcell emits one version
per celld TXID while it keeps up with celld's compaction window: until an L1
covers a range (`CELLD_LTX_COMPACTION_MIN_TXIDS`, default 256 TXIDs, or
`CELLD_LTX_COMPACTION_MIN_MB`), plus ~30-40 s of bundle GC delay. When cdcell
falls further behind (an outage, a backlog), it emits one net change for each
compacted range: current state stays exact, intermediate versions inside the
range are absent from type 2. Raising the compaction thresholds widens the
window at the cost of slower celld takeovers and more objects. With epoch GC
enabled (`CELLD_LTX_RETENTION_SECS`), cdcell still recovers (§6), at the same
net-change granularity. One SQLite transaction is the finest grain that
exists: celld stores changed pages, not statements.

## 8. Many workers (after v1)

Partition cells by hash; one lease per partition with its epoch stamped on
every commit; a handoff commits, checkpoints and releases before the next
owner acquires, and the next owner reads the checkpoint after acquiring;
each worker gets every notification (its own queue/consumer) and keeps its
partitions' cells; one checkpoint per partition. Schema changes to shared
tables are optimistic (assert the current schema id; reload and retry).

## 9. Operations

Prometheus on `:9464`: `cdcell_watermark_age_seconds_{max,p99}`,
`cdcell_parked_cells`, `cdcell_parked_age_seconds_max`, `cdcell_open_gaps`,
`cdcell_events_total`, `cdcell_units_total`, `cdcell_commits_total`,
`cdcell_commit_seconds_{sum,count}`, `cdcell_{phantoms,duplicates}_dropped_total`,
`cdcell_not_found_total`, `cdcell_decode_errors_total`,
`cdcell_rebuilds_total` (cells rebuilt or resynced from celld),
`cdcell_reconciled_bundles_total`, `cdcell_confirm_reads_total`,
`cdcell_s3_requests_total{op}`. Each commit logs the cells with the oldest
pending work ("lagging cell").

`cdcell maintain` expires snapshots of cdcell's tables and sets
`write.metadata.delete-after-commit.enabled`.
`deploy/iam/celld-read-policy.json`: least-privilege read of the celld bucket
(fleet secret, R2 app data, Worker code and KV blobs denied).

**Cost** (us-east-1 list prices; re-check): per 1M celld objects, one
notification (~$0.48 with SQS) and one GET (~$0.40) dominate; commits,
confirmations and schema reads are small. With commits every 15-60 min and,
in fleet mode, a LIST of live sessions instead of SQS, roughly $0.40 per 1M
objects plus a small always-on worker.

## 10. Status

| | |
|---|---|
| Built and gated (e2e) | M0 stack; M1 decoder; M2 single node; M3 fleet; M4 pause/stall/zombie/kill; M5 crash, lost notifications, outage, bootstrap; rev 5 change log and schema evolution; M6 metrics, maintenance, IAM |
| Planned | key-only decoder state (memory); per-partition leases and multi-worker; LIST-based fleet intake; durable parking; loss declarations; facets; `_cf_ALARM`; D1/KV/Queue/Workflow cells; GCS/Azure; column renames as Iceberg renames; full 60-minute soak |

Every gate: `e2e/scripts/all-gates.sh` (see [`../e2e/README.md`](../e2e/README.md)).
