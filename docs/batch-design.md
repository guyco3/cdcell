# cdcell batch design (proposal)

Status: proposal, replacing the streaming architecture of
[`design.md`](design.md) (revision 5). The output contract (change-log
columns, schema evolution, the consumer guide) stays as it is. What changes
is how cdcell reads celld and how it commits.

## 1. Idea

The celld bucket is the landing zone. celld never deletes per-cell objects
(`cells/<cell>/ltx/…`) unless its opt-in epoch GC is on, so a scheduled job
can read them hours later. Each run picks up every cell exactly where it
left off, turns new celld objects into change rows, and commits them to
Iceberg in idempotent batches.

Revision 5 raced celld's bundle GC (~30-40 s) to see every capture. That
needed always-on intake, live confirmation, gap timers and every cell's
state in memory, which is where most bugs were. The batch design gives up
the race and promises **the history celld keeps durably**, best effort finer
(§4).

### What celld leaves in the bucket

Verified in celld's source (paths relative to the celld repo):

| Object | Grain | Lifetime |
|---|---|---|
| bundle row `log/<node>/<gen>/bundle/e<LE>-<seq>.ltxb` | one capture (TXID) | deleted ~30 s after every row is covered by per-cell objects (`node_log.rs` `gc_bundles`, `logic/log_tier.rs:349`) |
| L1 `cells/<cell>/ltx/e<E>/0001/<min>-<max>.ltx` | a fold: ≥ `CELLD_LTX_COMPACTION_MIN_TXIDS` (256) captures or 32 MiB, written straight from bundle rows in fleet mode (`ltx_repl.rs:2714`) | kept until epoch GC (off by default) |
| L0 `…/0000/<min>-<max>.ltx` | one capture (bucket mode), or a merged run at drain points: eviction, handoff, recovery (`ltx_repl.rs:4146`) | kept until epoch GC; folds never delete sources (`ltx/src/replica_compactor.rs:1`) |
| L9 `…/0009/1-<max>.ltx` | full image at handoff | kept until epoch GC |
| `cells/<cell>/own.json` | owner `{node, epoch}` | current |

A capture already merges the SQLite commits since the previous one
(`ltx/src/wal.rs:529`), so no design can see every statement.

**Run interval** is a setting: `CDCELL_RUN_INTERVAL` (default `3h`) for the
long-running mode, or the customer's own schedule calling `cdcell run
--once`. Shorter intervals give fresher data and, in fleet mode, more
per-capture rows from the live tail; longer ones give fewer, larger files.

## 2. Where a run left off: cursors and partitions

**The cursor is per cell: its committed `(epoch, txid)`.** A cell's units
are totally ordered by `(epoch, txid)` and celld's objects for a cell are
immutable, so "continue after `(E, t)`" is exact and needs nothing else.
There is no global cursor over the bucket: keys are not ordered by time, and
cells advance independently.

**Partitions group cells** for parallelism and bookkeeping:
`partition = hash(cell_id) mod P` over a 32-bit hash range, P a power of two
(default 16). Each partition has its own:
- lease (`_cdcell/p<k>/lease.json`) and fence (§3.3): one writer at a time;
- checkpoint shard (`_cdcell/p<k>/checkpoint.json`): cell → cursor and
  snapshot key;
- batch sequence and manifests (`_cdcell/p<k>/batches/<seq>.json`).

A run processes partitions concurrently (one task each, or one container
each for large fleets). Doubling P splits every hash range in two, so a
shard splits without moving cells between unrelated partitions
(`cdcell repartition`, run while no job is running).

### 2.1 Finding the cells to process ("dirty cells")

The queue is the only discovery mechanism. celld's bucket sends a
notification for every object created under `cells/` and under `log/` to a
queue (SQS or JetStream), at least once (§7).

| Source | What it gives | Cost | When |
|---|---|---|---|
| **Per-cell notifications** (`cells/`) | the exact new per-cell object keys | one message per celld per-cell object | every run |
| **Bundle notifications** (`log/`) for the live tail | bundle keys with not-yet-folded rows | one message and one GET per bundle (one per node per `CELLD_LOG_FLUSH_MS`, default 2 s), most of them 404 by run time; plus node lease GETs | every run in fleet mode (`CDCELL_TAIL=off` skips bundles; bundle notifications are then not needed) |

- A run drains the queue first, writes the keys to `_cdcell/p<k>/dirty/<run>.json`,
  then acknowledges them, so no queue setting has to outlast a run.
- The live tail GETs the queued bundle keys. A 404 means celld already
  covered and GC'd that bundle; its rows are in per-cell objects, which are
  notified too, so a 404 is fine. Node leases are still read (GET) for
  confirmation (§2.2).
- Why the live tail matters: in fleet mode a quiet cell that stays resident
  can keep fewer than a fold's captures in bundles until it is evicted, so
  per-cell objects alone give it no freshness bound.
- Cells that existed before cdcell was deployed and never write again are
  not exported: no notification ever names them. A one-time, operator-run
  `cdcell backfill` is planned (§10); it is a manual command, not part of runs.

**When cdcell LISTs.** In exactly one case: **the queued keys for a cell do
not form an unbroken sequence from what cdcell already has** (its cursor and
cell state). Then, and only then, cdcell LISTs that one cell's prefix from
its cursor (`cells/<cell>/ltx/` with delimiter for epochs ≥ E, then per
level `StartAfter = <t+1 hex>`). This one rule covers:

- a missing key in the middle of a run's keys (late or lost notification);
- a cell seen for the first time whose first queued object is not a full
  image (L9, or a TXID-1/full-image opener): the cell has nothing yet, so its
  base is missing (bootstrap);
- a cell whose saved cell snapshot is missing, so its state must be rebuilt
  from celld's newest full image;
- an epoch change whose opener or intermediate epochs are not all in the
  queue;
- the fallback resync after celld's epoch GC.

Optimization: a single-epoch gap usually is one object whose key is known up
to its level, so first GET the guessed L1, then L0 key, ~12× cheaper than a
LIST. LIST on a 404, a multi-object gap or an epoch change.

There is no other LIST anywhere: no sweep, no daily job, no LIST of bundles
or live sessions.

**Late, lost and out-of-order notifications.** Every capture's content is in
the bucket (first a bundle row, then inside a per-cell object), even though a
TXID does not keep a file of its own once folded. The rules:

| Case | Handling |
|---|---|
| Out of order | Keys are sorted per cell by `(epoch, min)` before planning; arrival order never matters. |
| Missing in the middle (cursor 299; queue has `556-811`, not `300-555`) | The ranges do not cover every TXID from the cursor on: the planner never skips. The gap rule applies (GET guesses, else LIST the cell from the cursor); it finds the object and continues in the same run. |
| Missing at the end (queue has `300-555`, not `556-811`) | The cell advances to 555, the last point it can prove in sequence. A late notification is processed next run. |
| Late, after a LIST already covered it | At or below the cursor: ignored. |
| Lost, and it was the cell's only notification | The cell catches up on its next write: its next notified key leaves a gap, and the gap LIST finds the missing object. S3 delivers at least once, so this needs a broken setup (§7). |

### 2.2 Planning one cell

Inputs: cursor `(E, t)`; the cell's objects past it, from the queue (a
LIST only under the gap rule of §2.1); its live bundle rows (queued bundle
keys that still exist).

1. **GET** the units the plan needs (step 3), **then read `own.json`** and
   the node leases of bundle rows. That order is the confirmation rule
   (a successor CASes the record before reading data, invariants D5, D6):
   - per-cell object in epoch `E'`: history if `own.epoch == E'`, or if a
     later epoch's opener proves its cut (paged marker `(c+1)-(c+1)`, or a
     full image that is diffed);
   - bundle row: history if the node is on the same generation with its log
     open at an epoch ≥ the bundle's.
   Unproven units wait for the next run; the cell stops at its last proven
   point (`parked`).
2. **Finest chain:** objects overlap (L0s and L1s of one range, bundle rows
   over both). Pick the path from `t` to the proven head with the most
   steps (dynamic programming over TXID positions). That is the finest
   history that still exists. `CDCELL_GRAIN=fold` picks the fewest steps
   instead (cheapest).
3. **Apply** in order to the cell state and decode each step into rows. A
   range `a..b` with `a ≤ t+1 ≤ b` applies exactly: LTX pages are full page
   images, so the state at `t` plus the range's pages is the state at `b`,
   and the decoder emits the net change from `t` to `b`.

### 2.3 Cell state between runs

Decoding needs the cell's pages at its cursor. The **cell snapshot**
(`_cdcell/p<k>/cells/<cell>/<epoch>-<txid>.zst`, zstd SQLite pages,
immutable) is a cache over celld's objects:

- If the cursor lies on a per-cell object boundary, the state can be
  rebuilt from an older snapshot (or celld's newest full image) plus the
  objects up to the cursor. Those objects are kept, so a snapshot is
  optional there. A new one is written only when the replay would exceed
  `CDCELL_SNAPSHOT_EVERY` objects (default 8). A PUT costs about 12× a GET.
- If the cursor came from bundle rows (the tail), no durable celld object
  ends there once they are GC'd. A snapshot is **required** at that point.
- `--every` mode also keeps recently used states in memory (bounded by
  `CDCELL_STATE_CACHE_MB`) and skips the GET.
- Bootstrap (a new cell): the newest full image of the owner's chain is
  emitted as `r` rows. Full-history bootstrap from the oldest epoch is a
  later option.

## 3. Batches: idempotent and simple to retry

A **batch** is `(partition, seq)`: a set of that partition's cells moved
from their cursors to new proven points, committed once.

### 3.1 Life of a batch

1. **Plan:** choose dirty cells until `CDCELL_BATCH_ROWS` (estimated) or
   `CDCELL_BATCH_CELLS`. Write the manifest create-only:
   `{seq, lease_epoch, planned_at, cells: {cell: {from, to, snapshot}}}`.
2. **Execute:** GET, confirm, apply, decode; write the cell snapshots it
   needs; stage Parquet files at deterministic paths
   (`…/data/<partition values>/p<k>-b<seq>-<n>.parquet`). `__ingested_at` is
   `planned_at`, so a rerun writes identical rows.
3. **Commit, atomically, in one catalog transaction** (Lakekeeper
   `/transactions/commit`): every touched change-log table, the
   `_cdcell_progress` rows (the new cursors), snapshot summary
   `cdcell.batch=p<k>-<seq>`, and the partition fence (§3.3).
4. **Finish:** checkpoint shard (new cursors); delete consumed dirty lists
   and superseded snapshots.

### 3.2 Retry rules

| Failure | What happens |
|---|---|
| Before the commit (any error, crash) | The batch did not happen. The next attempt plans again from the checkpoint cursors with a new seq. Staged files of the abandoned batch are orphans (`cdcell maintain` removes them). |
| Commit ambiguous (timeout, 5xx) | Look for a snapshot with `cdcell.batch=p<k>-<seq>`: present → step 4; absent → resend the same commit. The fence requirement makes a double commit impossible. |
| Commit rejected by the fence | Another worker owns the partition: stop. |
| After the commit, before the checkpoint | On start, roll forward: every manifest with seq > the checkpoint's that is visibly committed updates the cursors. |
| One cell fails (unreadable object, transient error) | The cell is left out of the batch with its cursor unchanged, logged and counted, and retried next run. Other cells continue. |

Result: **each batch is committed exactly once**, and every cell's rows
appear once. Duplicates (rev 5 rule 4) no longer arise from crashes.

### 3.3 Fencing inside the commit

A lease alone cannot stop a paused old worker from committing late. The
catalog has to reject it. A tiny table `cdcell.<script>._cdcell_fence` has
one branch per partition (`p<k>`). Every batch commit includes the
requirement `assert-ref-snapshot-id(p<k>, <id seen at lease time>)` and
advances that branch. A new owner first commits an empty snapshot to `p<k>`,
which makes every older owner's pending commit fail as a whole. To verify:
Lakekeeper accepts branch refs in multi-table transactions (an e2e test).

## 4. History detail (customer choice)

| Goal | celld setting | Grain |
|---|---|---|
| Current state, coarse history | defaults | one version per fold (≥ 256 captures or 32 MiB); live-tail rows per capture |
| Finer history | lower `CELLD_LTX_COMPACTION_MIN_TXIDS` (16-64) | per that many captures; more celld PUTs, slower takeovers |
| Per-capture history | `CELLD_DURABILITY=bucket` | per capture; write acks wait for S3 |
| Every event | insert-only events table in the app | exact |

Later: on a versioned bucket, GC'd bundles stay readable as noncurrent
versions, which gives per-capture history without bucket mode. The fold
check of §2.2 (bundle rows must replay to the L1 that covers them) keeps
that safe.

## 5. Iceberg layout

### 5.1 Partitioning and sort order (change-log tables)

- **Partition spec: `day(__ts)`.** Time-bounded queries and type-2 "as of"
  queries prune by day. Late data (an outage, a bootstrap) just appends to
  older days. Hidden partitioning, so queries never mention it.
- **Sort order: `__cell_id, __key, __epoch, __txid`** (declared as the
  table's sort order, applied when writing each file). Per-object queries
  (`__cell_id = …`) and type-1 MERGEs prune by column statistics without
  extra partitions. Engines that compact keep the order.
- **No `bucket(__cell_id)` by default.** With runs every few hours it would multiply
  the files per run by the bucket count for little gain on sorted files.
  Enable per table (`CDCELL_BUCKETS=<table>=<n>`, applied by partition
  evolution, no rewrite) once a table writes more than ~10 GB a day or a
  Spark MERGE wants bucketed joins.
- **Incremental consumers** filter `__ingested_at > <last>`: each file holds
  exactly one batch, so file statistics prune to the new files without a
  partition on `__ingested_at`. Engines with incremental snapshot reads
  (Spark `start-snapshot-id`) can use those instead.
- Files: zstd Parquet, target 256 MiB
  (`write.target-file-size-bytes`). A batch writes one file per (table, day)
  unless it is larger. Format v2, append only.

### 5.2 Other tables

- `_cdcell_progress`: `day(committed_at)`, sorted by `_cell_id`; one row per
  cell per batch. The newest row per cell is its cursor, and the checkpoint
  can be rebuilt from it.
- `_cdcell_kv`: like change-log tables.
- `_cdcell_fence`: no data, branches only.

### 5.3 Maintenance

`cdcell maintain` (scheduled, e.g. daily): expire snapshots, remove orphan
files of abandoned batches, delete metadata after commit. It also
bin-packs small files of past days within each partition, keeping the sort
order. Customers' engines can do the same (`OPTIMIZE`, `rewrite_data_files`).

## 6. Cost per run

| Item | Requests |
|---|---|
| Per dirty cell | 1 `own.json` GET; 1 GET per new celld object on the chain (by default one per fold); a snapshot GET unless cached; a snapshot PUT only when §2.3 requires one; a LIST only in the gap fallback (§2.1) |
| Per run | the queue drain; node lease GETs and one GET per queued bundle key, most 404 (fleet); one fence commit per partition |
| Per batch | a few catalog calls; one Parquet file per (table, day) |

Compared with revision 5:
- no read per capture;
- no confirmation reads every second;
- no commit every 60 s: at the default 3 h interval, 180× fewer Iceberg
  snapshots and files;
- no always-on worker.

`docs/cost.md` will measure this on the e2e stack (MinIO request metrics),
as a function of cells, write rate and run interval.

## 7. Operating requirements

1. celld's epoch GC off (default), or its grace above the longest gap
   between successful runs. Otherwise the fallback resync applies:
   current state exact, history in the gap lost.
2. Read access to the celld bucket; read/write to the lake bucket and the
   catalog.
3. A scheduler: cron, Kubernetes CronJob, ECS scheduled task (`cdcell run
   --once`); or one long-running `cdcell run` that repeats every
   `CDCELL_RUN_INTERVAL` (default `3h`; any duration, e.g. `15m` or `24h`).
4. Required: notifications of every object created under `cells/` and
   under `log/` (bundles) to a queue (SQS or JetStream), at least once, as
   S3 delivers. `log/` is not needed with `CDCELL_TAIL=off`. A lost
   notification is a setup fault (a client invariant), not a cdcell bug.

## 8. What carries over from the streaming code

| Kept | Replaced |
|---|---|
| decoder (`cdcell-core`), emit, schema registry and evolution, `iceberg_out` staging and atomic commit, restore-chain planning (`resync::plan_chain`), resync fallback, keys, metrics, IAM policy, maintain | `pipeline.rs` streaming state machine, `run.rs` intake loop, gap timers, in-memory parking, JetStream ack handling |

The test kit (fake celld with real SQLite and LTX, `MemSink`, paused time)
carries over: tests queue notifications, then call `run_once`.

## 9. To verify before building

1. Lakekeeper accepts branch-ref requirements and updates in
   `/transactions/commit` (fencing, §3.3).
2. Freshness of quiet resident cells with the live tail on and off in fleet
   mode (e2e).
3. celld with `CELLD_LTX_COMPACTION_MIN_TXIDS=16`: write-path and takeover
   cost.
4. The finest-chain planner against every overlap celld produces (merged
   drain L0s, L1s from the bundle overlay, recovery windows): property test
   in the test kit.

## 10. Planned

- `cdcell backfill`: a one-time command the operator runs at
  install. It LISTs `cells/` once and exports cells that existed before
  cdcell and never write again. It is a manual command, not part of runs.
