# cdcell batch design (proposal)

Status: proposal, replacing the streaming architecture of
[`design.md`](design.md) (revision 5). The output columns, schema evolution
and the consumer guide stay as they are. What changes is how cdcell reads
celld, how it commits, and what it guarantees.

## 1. Contract

The celld bucket is the landing zone. celld never deletes per-cell objects
(`cells/<cell>/ltx/…`) unless its opt-in epoch GC is on, so a scheduled job
can read them hours later. Each run picks up every cell where it left off,
turns new celld objects into change rows, and commits them to Iceberg in
idempotent batches.

Revision 5 raced celld's bundle GC (~30-40 s) to see every capture. That
needed always-on intake, live confirmation, gap timers and every cell's
state in memory, which is where most bugs were. The batch design gives up
the race. It guarantees that history is valid, not that it is complete:
history can be coarser than every capture.

**Guarantees, per cell (one Durable Object):**

| Property | Guarantee |
|---|---|
| Validity | Every emitted version `(epoch, txid)` is a state the cell really had in its restorable history (a prefix of its history). Versions may be skipped. No version is a state that never existed; no phantom (fenced-node) write ever appears. |
| Order | `(__epoch, __txid)`, compared as a pair, is a total order. Epochs only increase (A6). TXIDs restart only at an epoch boundary: snapshot epochs restart at 1 (D1), paged epochs continue at cut+1 (D2). So `(3, 900) < (4, 1)`. |
| Consistent views | All tables of one cell at one version come from the same database state (§1.1). |
| Exactly once | The rows of a committed batch appear once (§3.3). Only the fallback resync may restate rows, as one net change. |
| Grain | Best effort (§4). |

Ordering and consistency are per Durable Object, matching the Durable
Object model (objects are independent; there are no cross-object
transactions).

Live-tail rows (bundle rows, fleet mode) are confirmed by a celld behavior,
not a guarantee (C12). If a later paged cut shows that a confirmed tail row
was not history, the cell is resynced as one net change and counted
(§2.2). `CDCELL_TAIL=off` does not use bundle rows.

### 1.1 Reading a consistent view

An engine may read tables at different snapshots. A cell's rows are
committed in order, so this recipe works however tables are read: take each
cell's committed point from `_cdcell_progress` (its newest row) and filter
every table to rows at or below it.

```sql
WITH p AS (
  SELECT _cell_id, _epoch, _txid,
         row_number() OVER (PARTITION BY _cell_id ORDER BY batch DESC) AS rn
  FROM _cdcell_progress)
SELECT t.* FROM orders t JOIN p ON t.__cell_id = p._cell_id AND p.rn = 1
WHERE t.__epoch < p._epoch
   OR (t.__epoch = p._epoch AND t.__txid <= p._txid)
```

Read `_cdcell_progress` first, or at a snapshot no newer than the other
tables.

### 1.2 What celld leaves in the bucket

Verified in celld's source (paths relative to the celld repo):

| Object | Grain | Lifetime |
|---|---|---|
| bundle row `log/<node>/<gen>/bundle/e<LE>-<seq>.ltxb` | one capture (TXID) | deleted ~30 s after every row is covered by per-cell objects (`node_log.rs` `gc_bundles`, `logic/log_tier.rs:349`) |
| L1 `cells/<cell>/ltx/e<E>/0001/<min>-<max>.ltx` | a fold: ≥ `CELLD_LTX_COMPACTION_MIN_TXIDS` (256) captures or 32 MiB, written straight from bundle rows in fleet mode (`ltx_repl.rs:2714`) | kept until epoch GC (off by default) |
| L0 `…/0000/<min>-<max>.ltx` | one upload pass in bucket mode (TXIDs captured since the last upload, merged under backlog), or a merged run at drain points: eviction, handoff, recovery (`ltx_repl.rs:4146`) | kept until epoch GC; folds never delete sources (`ltx/src/replica_compactor.rs:1`) |
| L9 `…/0009/1-<max>.ltx` | full image at handoff | kept until epoch GC |
| `cells/<cell>/own.json` | owner `{node, epoch}` | current |

Keys sort by min TXID within a level (B1). A capture already merges the
SQLite commits since the previous one (`ltx/src/wal.rs:529`), so no design
can see every statement.

**Run interval** is a setting: `CDCELL_RUN_INTERVAL` (default `3h`) for the
long-running mode, or the customer's own schedule calling `cdcell run
--once`. Shorter intervals give fresher data and, in fleet mode, more
per-capture rows from the live tail; longer ones give fewer, larger files.

## 2. Where a run left off: cursors and partitions

**The cursor is per cell: its committed `(epoch, txid)`.** A cell's units
are totally ordered by `(epoch, txid)` and celld's objects for a cell are
immutable, so "continue after `(E, t)`" is exact. There is no global cursor
over the bucket: keys are not ordered by time, and cells advance
independently.

**Partitions group cells** for parallel processing and bookkeeping:
`partition = hash(cell_id) mod P` over a 32-bit hash range, P a power of two
(default 16). Partitions are the unit of planning, GETs, decoding and
staging data files. Each has:
- a checkpoint shard (`_cdcell/p<k>/checkpoint.json`): cell → cursor,
  durable boundary, snapshot and journal keys (§2.3);
- a pending set (`_cdcell/p<k>/pending.json`): keys of parked or failed
  cells and the bundle rows they still need, carried to the next run.

Partitions do not lease, fence or commit. One committer per run commits all
partitions' staged work (§3). Doubling P splits every hash range in two;
`cdcell repartition` (run while no job is running) only moves checkpoint
shards and pending sets.

### 2.1 Finding the cells to process ("dirty cells")

The queue is the only discovery mechanism. celld's bucket sends a
notification for every object created under `cells/` and under `log/` to a
queue (SQS or JetStream), at least once (§8). Rules filter to `.ltx` and
`.ltxb` keys (G5); anything else that slips through (`own.json`,
`retired.json`) is dropped.

| Source | What it gives | Cost | When |
|---|---|---|---|
| **Per-cell notifications** (`cells/`) | the exact new per-cell object keys | one message per celld per-cell object | every run |
| **Bundle notifications** (`log/`) for the live tail | bundle keys with not-yet-folded rows | one message and one GET per bundle (one per node per `CELLD_LOG_FLUSH_MS`, default 2 s), most of them 404 by run time; plus node lease GETs | every run in fleet mode (`CDCELL_TAIL=off` skips bundles; bundle notifications are then not needed) |

- A run drains the queue once, splits the keys by partition, writes them
  to `_cdcell/p<k>/dirty/<run>.json`, then acknowledges them, so no queue
  setting has to outlast a run. Each partition's work is its dirty list
  plus its pending set.
- Bundle keys do not say which cells they hold, so the run GETs bundles
  once, centrally, and splits their rows into per-partition row lists. A
  404 means celld already covered and GC'd that bundle; its rows are in
  per-cell objects, which are notified too. Node leases are read (GET) for
  confirmation (§2.2).
- Why the live tail matters: in fleet mode a quiet cell that stays resident
  can keep fewer than a fold's captures in bundles until it is evicted
  (C7), so per-cell objects alone give it no freshness bound.
- Cells that existed before cdcell was deployed and never write again are
  not exported: no notification ever names them. A one-time, operator-run
  `cdcell backfill` is planned (§11).

**When cdcell LISTs.** Only for one cell, and only when **its queued keys
do not by themselves give an unbroken, provable chain** from what cdcell
already has (cursor and cell state). This covers:

- a missing key in the middle (late or lost notification);
- a cell seen for the first time whose first queued object is not a full
  image (L9, or a TXID-1 opener, D15): its base is missing (bootstrap);
- a cell whose saved state is missing, so it must be rebuilt;
- an epoch change whose opener is not queued, or whose intermediate epochs
  must be proven empty (epochs are routine, A6);
- the fallback resync after celld's epoch GC.

How it LISTs. Keys sort by min (B1), so `StartAfter` skips any object that
starts before it: L1s that straddle the cursor, every L9 (`1-<max>`), every
object starting at TXID 1. So:

| Case | LIST |
|---|---|
| Gap inside epoch E | `e<E>/0001/` and `e<E>/0009/` in full (one object per fold or handoff, few); `e<E>/0000/` with `StartAfter` at the cell's durable boundary (§2.3) |
| Epoch change | a delimiter LIST of `cells/<cell>/ltx/` for the epochs; then each epoch to walk as above, from TXID 0 |
| Bootstrap, rebuild, resync | from TXID 0, walking the epoch chain down from the newest epoch (celld's `EpochChain` semantics, §9) |

There is no other LIST: no sweep, no daily job, no LIST of bundles or live
sessions, and no guessed-key GETs (an L1's max is not predictable: the fold
size is a threshold).

**Holes.** If the same gap stays open after `CDCELL_GAP_LISTS` (default 3)
LISTs in different runs, the cell is parked (metric
`cdcell_cell_parked{reason="hole"}`) and stops LISTing. It resumes when a
later epoch's opener arrives in the queue, with one net change (§2.2).

**Late, lost and out-of-order notifications.** Every capture's content is in
the bucket (first a bundle row, then inside a per-cell object), even though a
TXID does not keep a file of its own once folded.

| Case | Handling |
|---|---|
| Out of order | Keys are sorted per cell by `(epoch, min)` before planning; arrival order never matters. |
| Missing in the middle (cursor 299; queue has `556-811`, not `300-555`) | The ranges do not cover every TXID from the cursor on: the planner never skips. The LIST rule applies and the run continues with what it finds. |
| Missing at the end (queue has `300-555`, not `556-811`) | The cell advances to 555, the last point it can prove in sequence. A late notification is processed next run. |
| Late, after a LIST already covered it | At or below the cursor: ignored. |
| Lost, and it was the cell's only notification | The cell catches up on its next write: its next key leaves a gap, and the gap LIST finds the missing object. S3 delivers at least once, so this needs a broken setup (§8). |
| Owner restarted into a snapshot epoch before the run read the old tail | The old epoch's unproven tail and the new epoch's first writes become one net change at the opener (§4). |

### 2.2 Planning one cell

Inputs: cursor `(E, t)`; the cell's objects past it (queue, or a LIST under
§2.1); its live bundle rows from the run's split.

1. **GET** the units the plan needs (step 3), **then read `own.json`** and
   the node leases of bundle rows. That order is the confirmation rule
   (a successor CASes the record before reading data, D5, D6):
   - per-cell object in epoch `E'`: history if `own.epoch == E'`, or if a
     later paged epoch's marker `(c+1)-(c+1)` proves the cut (D2) and the
     object ends at or below `c`. Files can land in `E'` after `own.json`
     moved on (D10): that makes them unproven, not phantom;
   - bundle row: history if the node is on the same generation with its log
     open at an epoch ≥ the bundle's (C12, behavior).

   A full image proves no cut (D1). A snapshot epoch's TXID-1 image is
   diffed against the state at the last proven point, and the difference is
   emitted as one net change at `(E'', 1)`. Unproven units wait for the
   next run; the cell stops at its last proven point (`parked`).
2. **Finest chain:** objects overlap (L0s and L1s of one range, bundle rows
   over both). Pick the path from `t` to the proven head with the most
   steps (dynamic programming over TXID positions). `CDCELL_GRAIN=fold`
   picks the fewest steps instead (cheapest).
3. **Apply** in order to the cell state and decode each step into rows. A
   range `a..b` with `a ≤ t+1 ≤ b` applies exactly: LTX pages are full page
   images, so the state at `t` plus the range's pages is the state at `b`,
   and the decoder emits the net change from `t` to `b`.

**Cursor above a later cut.** If a paged marker proves a cut `c` below a
cursor that came from a confirmed bundle row, those rows were not history.
The cell is rebuilt to `c` and resynced as one net change, with metric
`cdcell_tail_rollback_total`. This relies on C12 being true almost always;
it is not expected in a healthy fleet.

### 2.3 Cell state between runs

Decoding needs the cell's pages at its cursor. cdcell keeps:

- the **durable boundary**: the last cursor point that is the end of a
  per-cell object. The state there can always be rebuilt from celld's
  objects (newest full image plus objects), since celld keeps them;
- the **tail journal** (`<state>/<cell>/journal/<epoch>-<from>-<to>.ltx`):
  the raw LTX bytes of confirmed bundle rows applied since the durable
  boundary. Small, because it only spans the not-yet-folded tail. State at
  the cursor = state at the boundary + journal;
- an optional **cell snapshot** (`<state>/<cell>/<epoch>-<txid>.zst`, zstd
  SQLite pages, immutable): a cache, written only when the replay from the
  newest snapshot or full image would exceed `CDCELL_SNAPSHOT_EVERY`
  objects (default 8). A PUT costs about 12× a GET.

Journals and snapshots older than the newest boundary and snapshot are
deleted after the commit. `--every` mode also keeps recently used states in
memory (bounded by `CDCELL_STATE_CACHE_MB`).

`<state>` is `CDCELL_STATE_URL`, a prefix or bucket separate from the
Iceberg data, with its own IAM and KMS key. Snapshots and journals hold
whole SQLite pages: internal tables and freelist residue that the change
log never exposes.

Bootstrap (a new cell): the newest full image of the owner's chain is
emitted as `r` rows. Full-history bootstrap from the oldest epoch is a
later option.

## 3. Batches: one committer, idempotent

A **batch** is one atomic commit: a set of cells moved from their cursors
to new proven points. Batch ids (`seq`) increase with every committed
batch. A run commits one batch, or a few sequential ones if it is very
large.

Why one committer: concurrent commits to the same Iceberg table conflict,
because snapshot sequence numbers are per table and shared by all branches
(Lakekeeper 0.15.0: 409 `CommitFailedException`, "sequence number older
than last"). Schema-version assignment (the `cdcell.schemas` table
property, last writer wins) would race too.

### 3.1 Life of a run

1. **Lease and fence:** take the run lease (`_cdcell/lease.json`). A new
   holder commits an empty snapshot to the fence branch (§3.4) and
   remembers its id.
2. **Roll forward** any batch committed after the last checkpoint (§3.3).
3. **Drain** the queue and split bundles (§2.1). Record `planned_at` and the
   batch seq in `_cdcell/batches/<seq>.json` (create-only).
4. **Process partitions concurrently:** plan, GET, confirm, apply, decode;
   write journals and snapshots; stage Parquet at deterministic paths
   (`…/data/<partition values>/b<seq>-p<k>-<n>.parquet`), rows sorted by
   the table sort order. Each partition writes its plan
   `_cdcell/batches/<seq>/p<k>.json` (cells, from, to, staged files)
   create-only; the committer reads these (works across machines, §3.2). `__ingested_at` is
   `planned_at`, so a rerun writes identical rows.
5. **Commit** in the committer, in one catalog transaction (Lakekeeper
   `/transactions/commit`): every touched change-log table, the
   `_cdcell_progress` rows (new cursors and `batch`), snapshot summary
   `cdcell.batch=<seq>`, and the fence (§3.4). Schema evolution and
   registry visibility (`cdcell.schemas`) is updated here. A partition that
   meets a new shape evolves the table itself before staging (§3.2).
6. **Finish:** checkpoint shards and pending sets; delete the consumed
   dirty lists and superseded journals and snapshots.

### 3.2 Scaling out: plan, stage, commit

Each Durable Object's work needs only its own objects: a 1:1 map, no joins
or shuffles. So a run splits into three steps any runner can execute:

```
cdcell plan                 lease, fence, roll forward, drain the queue, split bundles;
                            writes the run plan: batch seq, planned_at, partitions → tasks
cdcell stage --task <i>     N in parallel: decode the task's partitions, write cell state
                            and Parquet, write p<k>.json per partition (staged files)
cdcell commit               read every p<k>.json; one atomic commit with the fence
```

- **Only `plan` reads the queue.** It drains it with concurrent pollers,
  writes the keys per partition (`_cdcell/runs/<seq>/p<k>/keys.json`), GETs
  and splits bundles into per-partition row files (a bundle mixes cells of
  many partitions, so this happens once, centrally), and acknowledges the
  messages only after those files are written. Stage tasks read their
  partitions' files from the state prefix: no competing consumers, no
  visibility timeout or ack deadline spanning a run, no ordering needed.
  Files of a failed run stay as the pending set; the next `plan` merges
  them with new messages.
- **Customers do not partition their queue.** S3 notifications filter only
  by prefix/suffix (no hashing), so one ordinary queue receives everything
  and `plan` does the partitioning. The queue carries folds and bundles,
  not captures, so one drainer keeps up; if it ever does not, `plan` runs
  several pollers on the same queue (SQS and JetStream allow concurrent
  consumers). A Kafka/Kinesis adapter whose topic is keyed by cell id
  (customer-routed) could let stage tasks read their own partitions; later.
- One machine: `cdcell run` does all three, partitions as threads (default).
- Several machines: any fan-out runner (a Kubernetes Indexed Job, Cloud Run
  job tasks, N ECS/Fargate tasks, an Airflow/Argo/Dagster fan-out, or a Spark
  job that runs `stage` on executors) runs `stage` N times, then `commit`.
  No cluster framework is required.
- A failed stage task is rerun: same plan, same cursors, same file names.
  Nothing is visible until the commit. `commit` waits for every task's
  files; a missing partition is left for the next run (its cursor does not
  move).
- Stage tasks never commit data. Schema is the only shared thing they
  touch: a task that meets a new shape adds the columns in a schema-only
  commit with `assert-current-schema-id`, reloading and retrying on 409
  (adding an existing column is a no-op), then writes files with the new
  field ids. Schema versions (`fingerprint → __schema_version`) are assigned
  through a conditional-PUT counter in the state prefix
  (`_cdcell/schemas/<table>/`), not a last-writer-wins table property; the
  committer copies the registry into `cdcell.schemas` for readers.

### 3.3 Retry rules

| Failure | What happens |
|---|---|
| Before the commit (any error, crash) | The batch did not happen. The next attempt plans again from the checkpoint cursors with a new seq. Staged files of the abandoned batch are orphans (`cdcell maintain` removes them). |
| Commit ambiguous (timeout, 5xx) | Load the fence branch head: our new snapshot id → committed, go to step 6; the previous id → resend the same commit; anything else → fenced out, stop. |
| 409 on the fence (`CatalogCommitConflicts`, "Branch or tag … snapshot has changed" or "already exists") | Another run took over: stop. |
| Any other 409 (e.g. sequence number, from `maintain` or a customer's compaction) | Reload the tables, rebuild only the snapshot and manifest list over the new parent, reusing the staged data files and manifests; retry with jitter. |
| After the commit, before the checkpoint | On start, roll forward from the fence branch head (its summary carries `cdcell.batch`) and the `_cdcell_progress` rows, not from table snapshot summaries, which `maintain` expires. |
| One cell fails (unreadable object, transient error) | The cell is left out with its cursor unchanged, logged and counted, and its keys go to the pending set for the next run. Other cells continue. |

Result: **each batch is committed exactly once**, batches commit in seq
order, and every cell's rows appear once. Duplicates (rev 5 rule 4) no
longer arise from crashes.

### 3.4 Fencing inside the commit

A lease alone cannot stop a paused old run from committing late. The
catalog has to reject it. A tiny table `cdcell.<script>._cdcell_fence` has
one branch, `run`. Every batch commit includes
`assert-ref-snapshot-id(run, <id this committer last wrote>)` and advances
the branch with a new snapshot whose id the committer picks, so it knows
the new head without a reload. A new run holder first commits an empty
snapshot to `run`, which makes every older holder's pending commit fail as
a whole.

Verified on Lakekeeper 0.15.0: a stale `assert-ref-snapshot-id` rejects the
whole multi-table request with 409 and no table changes; fence snapshots
need no data files; success is 200 with an empty body.

## 4. History detail (customer choice)

| Goal | celld setting | Grain |
|---|---|---|
| Current state, coarse history | defaults | one version per fold (≥ 256 captures or 32 MiB) while a cell stays active between runs; live-tail rows per capture |
| Finer history | lower `CELLD_LTX_COMPACTION_MIN_TXIDS` (16-64) | per that many captures; more celld PUTs, slower takeovers |
| Finer history, bucket mode | `CELLD_DURABILITY=bucket` | per upload pass, usually per capture (under backlog `sync_cell` merges the TXIDs since the last upload into one L0); write acks wait for S3 |
| Every event | insert-only events table in the app | exact |

**Restarts between runs.** Every activation advances the epoch (A6). A
database under `CELLD_LTX_PAGED_MIN_MB` (default 256 MiB) gets a snapshot
epoch: it opens with a TXID-1 full image that already holds the first new
writes, and nothing records which TXID of the old epoch was restored (D1).
Read hours later, after `own.json` moved on, the old epoch's last objects
cannot be proven. So the old epoch's unproven tail and the new epoch's
first writes collapse into one net change at the new opener: across a
restart between runs, one version per activation for the unproven part.
With `CELLD_IDLE_EVICT_S` or frequent deploys this is common. Paged epochs
(≥ 256 MiB) carry the cut (D2) and keep their grain. Shorter run intervals
and the live tail reduce the unproven part.

Later: on a versioned bucket, GC'd bundles stay readable as noncurrent
versions, which gives per-capture history without bucket mode. That needs
a fold check (not built): bundle rows must replay to the L1 that covers
them.

## 5. Iceberg layout

### 5.1 Partitioning and sort order (change-log tables)

- **Partition spec: configurable, default `day(__ts)`.** Iceberg hidden
  partitioning with a transform, the standard for Iceberg (not Hive-style
  `year=/month=/day=` columns): a plain `WHERE __ts >= …` prunes, and queries
  never mention partitions. Pick the granularity by volume, aiming at
  partitions of ~1 GB or more and files of 128 MiB-1 GiB:

  | Table volume | Spec |
  |---|---|
  | small (< ~1 GB a month) | `month(__ts)` |
  | medium (default) | `day(__ts)` |
  | very large (many GB a day) | `hour(__ts)`, often with `bucket(N, __cell_id)` |

  Settings: `CDCELL_PARTITION` (all tables, e.g. `day(__ts)`) and
  `CDCELL_PARTITION_<table>` (one table, e.g. `month(__ts),bucket(8,__cell_id)`).
  The time column may be `__ts` or `__ingested_at`. A changed setting is
  applied by Iceberg partition evolution: new files use the new spec, old
  files stay, no rewrite. Late data (an outage, a bootstrap) appends to
  older partitions. `__ts` is the owner's wall clock (B9): clock skew can
  create stray partitions, and correctness never depends on `__ts`; order
  and as-of logic use `(__epoch, __txid)`.
- **Sort order: `__cell_id, __key, __epoch, __txid`** (declared as the
  table's sort order, applied when writing each file). Per-object queries
  (`__cell_id = …`) and type-1 MERGEs prune by column statistics without
  extra partitions. Engines that compact keep the order. The order within a
  file is for pruning; consumers still order by `(__epoch, __txid)`.
- **No `bucket(__cell_id)` by default.** With runs every few hours it would
  multiply the files per run by the bucket count for little gain on sorted
  files. Add it per table through `CDCELL_PARTITION_<table>` once a table
  writes more than ~10 GB a day or an engine's MERGE wants bucketed joins.
- **Incremental consumers** read by batch: `_cdcell_progress.batch` or the
  snapshot summary `cdcell.batch`. One committer commits batches in seq
  order, so `batch > <last seen>` is a safe cursor. Iceberg snapshot-based
  incremental reads (Spark `start-snapshot-id`) work too.
  `__ingested_at` is not a cursor: it is the batch's `planned_at` (fixed so
  reruns are identical) and can be older than rows already visible. A
  consumer that must use it reads `__ingested_at > <last> - <max batch
  latency>` and dedups.
- Files: zstd Parquet, target 256 MiB (`write.target-file-size-bytes`). A
  batch writes one file per (table, day) unless it is larger. Format v2,
  append only.

### 5.2 Other tables

- `_cdcell_progress`: `day(committed_at)`, sorted by `_cell_id`; one row per
  cell per batch. The newest row per cell is its cursor, and the checkpoint
  can be rebuilt from it.
- `_cdcell_kv`: like change-log tables.
- `_cdcell_fence`: no data, one branch.

### 5.3 Maintenance

`cdcell maintain` (scheduled, e.g. daily): expire snapshots (never the
fence branch head), remove orphan files of abandoned batches, delete
metadata after commit. It also bin-packs small files of past days within
each partition, keeping the sort order. Customers' engines can do the same
(`OPTIMIZE`, `rewrite_data_files`). These commits can conflict with a run's
commit; the run retries (§3.3).

## 6. Cost per run

| Item | Requests |
|---|---|
| Per dirty cell | 1 `own.json` GET; 1 GET per new celld object on the chain (by default one per fold); a snapshot GET unless cached; a journal PUT when the cursor ends in tail rows; a snapshot PUT only per `CDCELL_SNAPSHOT_EVERY` |
| Per LISTing cell | gap: 3 LISTs (L0, L1, L9); epoch change: 1 delimiter LIST plus 3 per walked epoch. Epoch changes are routine (A6), so expect this for cells restarted since the last run |
| Per run | the queue drain; one GET per queued bundle key, most 404, split centrally (fleet); node lease GETs; one lease write; one catalog transaction (or a few for a huge run) |
| Per batch | one Parquet file per (table, day) |

Compared with revision 5:
- no read per capture;
- no confirmation reads every second;
- no commit every 60 s: at the default 3 h interval, 180× fewer Iceberg
  snapshots and files;
- no always-on worker.

`docs/cost.md` will measure this on the e2e stack (MinIO request metrics),
as a function of cells, write rate, restart rate and run interval.

## 7. Deployment

cdcell is one container image (a static binary inside) that runs a batch and
exits (`cdcell run --once`), or repeats every `CDCELL_RUN_INTERVAL`. It needs
no Spark or other cluster: the work is decoding each object's changes in
order with per-object state, I/O-bound and sequential per object, and it
writes Iceberg itself. Parallelism comes from partitions inside the process
(and later from several containers). It only uses standard interfaces, so it
is not tied to one vendor:

| Piece | Interface | Examples |
|---|---|---|
| Scheduler | runs a container on a schedule | Kubernetes CronJob, ECS scheduled task, Cloud Run jobs, Nomad periodic job, cron on a VM, a task in Airflow / Dagster / Argo |
| celld bucket, lake bucket, state prefix | S3 API | AWS S3, MinIO, Cloudflare R2, GCS (S3 interoperability) |
| Catalog | Iceberg REST catalog spec | Lakekeeper, Apache Polaris, Nessie, AWS Glue (REST endpoint), S3 Tables, Unity Catalog, Snowflake Open Catalog |
| Queue | a small adapter | SQS, NATS JetStream; others (Kafka, Pub/Sub) are an adapter each |
| Consumers | Iceberg tables | Spark, Trino, ClickHouse, DuckDB, Snowflake, BigQuery (BigLake), Athena, Flink |

**Queues by storage provider** (S3-style notifications must reach the queue
natively; check the provider's current docs):

| celld bucket on | Native notification targets | Queue cdcell reads |
|---|---|---|
| AWS S3 | SQS, SNS, Lambda, EventBridge | SQS (Kafka/MSK would need a forwarder) |
| MinIO | Kafka, NATS JetStream, AMQP, Redis, MQTT, webhooks, … | Kafka or JetStream |
| Ceph RGW | Kafka, AMQP, HTTP | Kafka |
| Cloudflare R2 | Cloudflare Queues | Cloudflare Queues (HTTP pull) |
| Google Cloud Storage | Pub/Sub | Pub/Sub |
| Azure Blob | Event Grid → Storage Queue or Event Hubs | Event Hubs (Kafka endpoint) |

Adapters, in order: SQS; the Kafka protocol (Apache Kafka, Redpanda,
Confluent, MSK, Aiven, WarpStream, Event Hubs): `plan` reads with a consumer
group and commits offsets after writing the key files; NATS JetStream
(built); Cloudflare Queues; Pub/Sub. One interface: fetch a batch of keys,
acknowledge after the key files are durable. Retention (SQS up to 14 days,
Kafka `retention.ms`, JetStream `max_age`) must exceed the longest outage.

Engines such as Spark belong on the consumer side, if the customer has one
(type-1/type-2 MERGEs; consumer guide). None is required.

## 8. Operating requirements

1. celld's epoch GC off (default), or its grace above the longest gap
   between successful runs. Otherwise the fallback resync applies:
   current state exact, history in the gap lost.
2. Read access to the celld bucket; read/write to the lake bucket, the
   catalog and `CDCELL_STATE_URL` (its own IAM and KMS, §2.3).
3. A scheduler: cron, Kubernetes CronJob, ECS scheduled task (`cdcell run
   --once`); or one long-running `cdcell run` that repeats every
   `CDCELL_RUN_INTERVAL` (default `3h`; any duration, e.g. `15m` or `24h`).
4. Required: notifications of every object created under `cells/` (suffix
   `.ltx`) and under `log/` (suffix `.ltxb`) to a queue (SQS or JetStream),
   at least once, as S3 delivers (G2, G5). `log/` is not needed with
   `CDCELL_TAIL=off`. A lost notification is a setup fault (G3), not a
   cdcell bug.
5. Queue retention longer than cdcell's longest outage (SQS up to 14 days;
   JetStream `max_age`). Without it, a quiet cell's final objects may never
   be exported: no later write reveals the gap.

## 9. What carries over from the streaming code

| Kept | Changed or replaced |
|---|---|
| decoder (`cdcell-core`), emit, schema registry and evolution (now committer-only), `iceberg_out` staging and atomic commit, resync fallback, keys, metrics, IAM policy, maintain | `pipeline.rs` streaming state machine, `run.rs` intake loop, gap timers, in-memory parking, JetStream ack handling |
| | `resync::plan_chain`: not celld's `EpochChain`. celld skips an intermediate epoch whose clipped objects do not end at the cut and keeps walking down (`crates/ltx/src/client/epochs.rs:50-75`); `plan_chain` returns None. Port `EpochChain` exactly. |
| | Rev-5 tables use `bucket(16, __cell_id)`: adopting `day(__ts)` needs partition evolution (no rewrite). |
| | Rev-5 file names are random and rows unsorted: now deterministic names and sorted rows (§3.1). |

The test kit (fake celld with real SQLite and LTX, `MemSink`, paused time)
carries over: tests queue notifications, then call `run_once`.

## 10. To verify or build before shipping

1. Freshness of quiet resident cells with the live tail on and off in fleet
   mode (e2e).
2. celld with `CELLD_LTX_COMPACTION_MIN_TXIDS=16`: write-path and takeover
   cost.
3. The finest-chain planner against every overlap celld produces (merged
   drain L0s, L1s from the bundle overlay, recovery windows): property test
   in the test kit.
4. The `EpochChain` port, property-tested against celld's crate.
5. The L0 `StartAfter` bound: no L0 straddles a durable boundary
   (§2.1). If one can, list L0 of the epoch in full.
6. Lakekeeper fencing (§3.4) is verified on 0.15.0; keep it as an e2e
   test.

## 11. Planned

- `cdcell backfill`: a one-time command the operator runs at install. It
  LISTs `cells/` once and exports cells that existed before cdcell and
  never write again. It is a manual command, not part of runs.
- Multi-container workers with one committer per partition. Concurrent
  commits to shared tables conflict on sequence numbers (§3), so this needs
  the rebase-retry of §3.3 on every commit and serialized schema updates.
