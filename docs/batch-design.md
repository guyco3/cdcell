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
| Validity | Every emitted version `(epoch, txid)` is a state the cell really had in its restorable history (a prefix of its history). Versions may be skipped. No version is a state that never existed; no phantom (fenced-node) write ever appears. No exception: cdcell reads only per-cell objects, confirmed by `own.json` (D5, D6). |
| Order | `(__epoch, __txid)`, compared as a pair, is a total order. Epochs only increase (A6). TXIDs restart only at an epoch boundary: snapshot epochs restart at 1 (D1), paged epochs continue at cut+1 (D2). So `(3, 900) < (4, 1)`. |
| Consistent views | All tables of one cell at one version come from the same database state (§1.1). |
| Exactly once | The rows of a committed batch appear once (§3.3). Only the fallback resync may restate rows, as one net change. |
| Grain | Best effort (§4). |
| Freshness | A change reaches the lake once it is in a per-cell object: after a fold (`CELLD_LTX_COMPACTION_MIN_TXIDS` captures or 32 MiB) or a drain (eviction, handoff, recovery), then the next run. A quiet cell that stays resident can wait until its next fold or eviction. `CELLD_IDLE_EVICT_S` bounds this (idle eviction writes the cell's changes to a per-cell object); the next activation starts a new epoch, so history across that restart is coarser (§4). |

These guarantees hold by default. Freshness and history detail depend on
the customer's celld settings (§5).

Ordering and consistency are per Durable Object, matching the Durable
Object model (objects are independent; there are no cross-object
transactions).

The optional live tail (`CDCELL_TAIL=on`, §2.4) trades strict validity for
freshness. With it on, validity holds except a rare case that is detected
and self-corrects (`cdcell_tail_rollback_total`).

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
--once`. Shorter intervals give fresher data (and, with the opt-in live
tail, more per-capture rows); longer ones give fewer, larger files.

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
  durable boundary and snapshot keys (§2.3; journal keys with the tail on);
- a pending set (`_cdcell/p<k>/pending.json`): keys of parked or failed
  cells, carried to the next run (with the tail on, also the bundle rows
  they still need).

Partitions do not lease, fence or commit. One committer per run commits all
partitions' staged work (§3). P is pinned in `_cdcell/layout.json`
(written once); every command refuses to run if its P differs. Doubling P
splits every hash range in two; `cdcell repartition` (offline: no job
running, lease held) only moves checkpoint shards and pending sets, then
rewrites `layout.json`.

### 2.1 Finding the cells to process ("dirty cells")

A **message broker** is the only discovery mechanism. celld's bucket sends a
notification for every object created under `cells/` to the broker (SQS,
Kafka, JetStream, …; any works, §8), at least once (§9). Ordering is not
required: keys are sorted per cell before planning. Rules filter to `.ltx`
keys (G5); anything else that slips through (`own.json`, `retired.json`) is
dropped.

| Source | What it gives | Cost | When |
|---|---|---|---|
| **Per-cell notifications** (`cells/`) | the exact new per-cell object keys | one message per celld per-cell object | every run |
| **Bundle notifications** (`log/`) | bundle keys with not-yet-folded rows | see §2.4 | only with `CDCELL_TAIL=on` (§2.4) |

- A run drains the broker once, splits the keys by partition, writes them
  to `_cdcell/batches/<seq>/p<k>.keys.json`, then acknowledges them, so no
  broker setting has to outlast a run (§3.1). Each partition's work is its
  keys file plus its pending set.
- Freshness: a cell's changes are found once they are in a per-cell object,
  after a fold or a drain (eviction, handoff, recovery). A quiet cell that
  stays resident keeps fewer than a fold's captures in bundles (C7) and can
  wait until its next fold or eviction. `CELLD_IDLE_EVICT_S` bounds the
  wait: idle eviction writes the cell's changes to a per-cell object. The
  next activation starts a new epoch, so history across that restart is
  coarser (§4, §5). The opt-in live tail (§2.4) is the other way.
- Cells that existed before cdcell was deployed and never write again are
  not exported: no notification ever names them. A one-time, operator-run
  `cdcell backfill` is planned (§12).

**When cdcell LISTs.** Only for one cell, and only when **its notified keys
do not by themselves give an unbroken, provable chain** from what cdcell
already has (cursor and cell state). This covers:

- a missing key in the middle (late or lost notification);
- a cell seen for the first time whose first notified object is not a full
  image (L9, or a TXID-1 opener, D15): its base is missing (bootstrap);
- a cell whose saved state is missing, so it must be rebuilt;
- an epoch change whose opener is not notified, or whose intermediate epochs
  must be proven empty (epochs are routine, A6);
- the fallback resync after celld's epoch GC.

How it LISTs. Keys sort by min (B1), so `StartAfter` skips any object that
starts before it: L1s that straddle the cursor, every L9 (`1-<max>`), every
object starting at TXID 1. An L0 can straddle too: a drain uploads one
merged L0 from the replica's last uploaded position (`ltx_repl.rs:4110`,
`:4150-4160`), and folds and bundle flushes do not move that position, so a
drain can write L0 `1-600` over L1s `1-256`, `257-512`. Idle eviction
normally publishes an L9 instead; a drain L0 comes from a paged cell, a
snapshot over budget, a failed or invisible L9, cleanup, bucket mode or
degraded mode (spike `docs/spikes/pr0.md` §1). So:

| Case | LIST |
|---|---|
| Gap inside epoch E | `e<E>/0001/` and `e<E>/0009/` in full (one object per fold or handoff, few); `e<E>/0000/` with `StartAfter` at the highest known **L0** end (or the paged marker) at or below the cursor, from 0 if none is known; never at an L1 or L9 end. If that does not close the gap, the next LIST lists `e<E>/0000/` in full |
| Epoch change | a delimiter LIST of `cells/<cell>/ltx/` for the epochs (dirs are unpadded decimal, so it returns every epoch, paginated); then each epoch to walk as above, from TXID 0 |
| Bootstrap, rebuild, resync | from TXID 0, walking the epoch chain down from the newest epoch (celld's `EpochChain` semantics, §10) |

LIST results are sorted by `(epoch, level, min, max)` before planning; no
ordering is assumed from the store.

There is no other LIST: no sweep, no daily job, no LIST of bundles or live
sessions, and no guessed-key GETs (an L1's max is not predictable: the fold
size is a threshold).

**Holes.** If the same gap stays open after `CDCELL_GAP_LISTS` (default 3)
LISTs in different runs, the cell is parked (metric
`cdcell_cell_parked{reason="hole"}`) and stops LISTing. It resumes when a
later epoch's opener arrives in the broker, with one net change (§2.2).

**Late, lost and out-of-order notifications.** Every capture's content ends
up inside a per-cell object, even though a TXID does not keep a file of its
own once folded.

| Case | Handling |
|---|---|
| Out of order | Keys are sorted per cell by `(epoch, min)` before planning; arrival order never matters. |
| Missing in the middle (cursor 299; broker has `556-811`, not `300-555`) | The ranges do not cover every TXID from the cursor on: the planner never skips. The LIST rule applies and the run continues with what it finds. |
| Missing at the end (broker has `300-555`, not `556-811`) | The cell advances to 555, the last point it can prove in sequence. A late notification is processed next run. |
| Late, after a LIST already covered it | At or below the cursor: ignored. |
| Lost, and it was the cell's only notification | The cell catches up on its next write: its next key leaves a gap, and the gap LIST finds the missing object. S3 delivers at least once, so this needs a broken setup (§9). |
| Owner restarted into a snapshot epoch before the run read the old epoch's last objects | Those unproven objects and the new epoch's first writes become one net change at the opener (§4). |

### 2.2 Planning one cell

Inputs: cursor `(E, t)`; the cell's objects past it (broker, or a LIST under
§2.1); with the tail on, its bundle rows from the run's split (§2.4).

1. **GET** the units the plan needs (step 3), **then read `own.json`** (and,
   with the tail on, the node leases of bundle rows). That order is the
   confirmation rule (a successor CASes the record before reading data, D5,
   D6):
   - per-cell object in epoch `E'`: history if `own.epoch == E'`, or if the
     next epoch `E'+1` proves a cut `c` (D2) and the object ends at or below
     `c`. Files can land in `E'` after `own.json` moved on (D10): that makes
     them unproven, not phantom;
   - bundle row (tail on only): see §2.4.

   **A cut is proven by content, never by key.** In bucket mode every L0
   is `n-n`, so a single-TXID key is not a marker. A cut `c` is proven
   only by (a) a fetched object of `E'+1` whose LTX header has zero pages
   and `wal_size == 0` (celld's marker, `ltx/src/db.rs:961-985`), with
   min `c+1`; or (b) `E'+1`'s lowest TXID from a full listing of `E'+1`
   from TXID 0 (`EpochChain` semantics). An object of `E'+1` that is not
   its lowest never sets a cut. A full image proves no cut (D1). A snapshot epoch's TXID-1 image is
   diffed against the state at the last proven point, and the difference is
   emitted as one net change at `(E'', 1)`. Unproven units wait for the
   next run; the cell stops at its last proven point (`parked`).
2. **Finest chain:** objects overlap (L0s and L1s of one range; with the
   tail on, bundle rows over both). Pick the path from `t` to the proven
   head with the most steps (dynamic programming over TXID positions).
   `CDCELL_GRAIN=fold` picks the fewest steps instead (cheapest).
3. **Apply** in order to the cell state and decode each step into rows. A
   range `a..b` with `a ≤ t+1 ≤ b` applies exactly: LTX pages are full page
   images, so the state at `t` plus the range's pages is the state at `b`,
   and the decoder emits the net change from `t` to `b`.

### 2.3 Cell state between runs

Decoding needs the cell's pages at its cursor. Without the tail, the cursor
always lies on a per-cell object boundary (the **durable boundary**), so the
state there is a snapshot, or is rebuilt from celld's objects (newest full
image plus objects), since celld keeps them. cdcell keeps:

- an optional **cell snapshot**
  (`<state>/snapshots/<urlenc(cell)>/<epoch:020>-<txid:020>.zst`, zstd
  SQLite pages, immutable): a cache, written only when the replay from the
  newest snapshot or full image would exceed `CDCELL_SNAPSHOT_EVERY`
  objects (default 8). A PUT costs about 12× a GET. Exception: a paged cell
  (database at or above `CELLD_LTX_PAGED_MIN_MB`) is snapshotted on first
  sight and after every run that changes it, ignoring
  `CDCELL_SNAPSHOT_EVERY`, because rebuilding it means replaying a large
  full image. Snapshots are written and read streaming, within the memory
  budget (§3). The root is separate from `cells/`, so cell ids that nest
  (facets under `cells/`) never collide with a snapshot key.

After the checkpoint is written, only the previous checkpoint's snapshot
key for that cell is deleted (never a listing or prefix delete). The tail
journal exists only with the tail on (§2.4). `--every` mode also keeps
recently used states in memory (bounded by `CDCELL_STATE_CACHE_MB`).

`<state>` is `CDCELL_STATE_URL`, a prefix or bucket separate from the
Iceberg data, with its own IAM and KMS key. Snapshots (and journals) hold
whole SQLite pages: internal tables and freelist residue that the change
log never exposes.

Bootstrap (a new cell): the newest full image of the owner's chain is
emitted as `r` rows. Full-history bootstrap from the oldest epoch is a
later option. Bootstrap and rebuild still run the proof (§2.2): if the
owner's newest epoch is empty (celld's `EpochChain` skips it,
`epochs.rs:64`), nothing in older epochs is proven and the cell parks until
the owner writes.

### 2.4 Optional live tail (`CDCELL_TAIL=on`)

Off by default. It reads rows celld has not folded yet, from bundles under
`log/`.

**What it does.** Each run GETs the notified bundle keys still in the
bucket. A 404 means celld already folded and GC'd that bundle; its rows are
in per-cell objects, which are notified too. Bundle keys do not say which
cells they hold, so `plan` GETs bundles once, centrally, and splits their
rows into per-partition row files. A bundle row is confirmed as history if
its node is on the same generation with its log open at an epoch ≥ the
bundle's (node lease GET after the data GET). This is a celld behavior, not
a guarantee (C12).

**What it buys.** Quiet resident cells are fresh every run, and rows still
in bundles give per-capture versions (§4).

**What it costs.**
- bundle notifications under `log/` (suffix `.ltxb`, §9), one message and
  one GET per bundle (one per node per `CELLD_LOG_FLUSH_MS`, default 2 s),
  most of them 404 by run time;
- node lease GETs every run;
- a **tail journal** per cell (`<state>/snapshots/<urlenc(cell)>/journal/<epoch>-<from>-<to>.ltx`):
  the raw LTX bytes of confirmed bundle rows applied since the durable
  boundary, required whenever a cursor ends in tail rows (a journal PUT).
  State at the cursor = state at the boundary + journal. Small, because it
  only spans the not-yet-folded rows; superseded journals are deleted after
  the commit.

**Caveat and self-correction.** If a later paged marker proves a cut `c`
below a cursor that came from a confirmed bundle row, those rows were not
history: cdcell emitted versions that never existed. The cell is rebuilt to
`c` and resynced as one net change, with metric
`cdcell_tail_rollback_total`. This relies on C12 being true almost always;
it is not expected in a healthy fleet.

## 3. Batches: one committer, idempotent

A **batch** is one atomic commit: a set of cells moved from their cursors
to new proven points. Batch ids (`seq`) increase with every committed
batch (abandoned seqs leave gaps).

**Size.** A batch is capped by cells, bytes and rows (`CDCELL_BATCH_CELLS`,
`CDCELL_BATCH_BYTES`, `CDCELL_BATCH_ROWS`). `plan` caps cells and notified
object bytes; a stage task that reaches a cap stops each remaining cell at
its current proven point and leaves the rest pending. A run then loops:
several sequential batches, each with its own seq, `commit.json` and fence
advance, until no work is left (an outage backlog becomes several batches).
Memory is bounded by one global byte-weighted semaphore over in-flight cells
(`CDCELL_MEMORY_MB`), not per partition: a cell acquires its object and
state bytes before it GETs, so one large cell waits rather than overflowing.

Why one committer: concurrent commits to the same Iceberg table conflict,
because snapshot sequence numbers are per table and shared by all branches
(Lakekeeper 0.15.0: 409 `CommitFailedException`, "sequence number older
than last"). Schema-version assignment (the `cdcell.schemas` table
property, last writer wins) would race too.

### 3.1 Life of a run

1. **Lease and fence:** take the run lease (`_cdcell/lease.json`; never
   while another holder's lease is unexpired). It is renewed during stage
   and commit. A new holder commits an empty snapshot to the fence branch
   (§3.4) and remembers its id. That snapshot copies `cdcell.last-batch`
   from the head it replaces, so taking the fence never hides the last
   committed batch.
2. **Roll forward** any batch committed after the last checkpoint (§3.3).
3. **Evolve tables:** partition spec and sort-order changes (including the
   rev-5 migration: `add-spec`, `set-default-spec`, `add-sort-order`, `set-default-sort-order`, verified on Lakekeeper 0.15.0; take `source-id`s from the loaded metadata, since Lakekeeper renumbers field ids at create)
   happen only here, before any staging, because Iceberg rejects data files
   whose spec id is not the table's default. The run plan records each
   table's spec id.
4. **Allocate the seq and drain**, in this order: create
   `_cdcell/batches/<seq>.json` create-only with `seq = max(existing) + 1`
   (of two concurrent plans, one wins; the loser stops before draining);
   drain the broker (§2.1; with the tail on, split bundles, §2.4); write
   each partition's `_cdcell/batches/<seq>/p<k>.keys.json` create-only,
   merging the keys files of every uncommitted seq above the last committed
   batch (each merged once, then marked merged), and freezing each named
   cell's cursor from its checkpoint; then acknowledge the broker. The run
   plan holds `planned_at`, P, tasks, spec ids and the fence id to assert.
5. **Stage, partitions concurrently:** plan, GET, confirm, apply, decode;
   write snapshots (and journals, tail on); write Parquet create-only at
   deterministic paths (`…/data/<partition values>/b<seq>-t<i>-<n>.parquet`,
   task `i`), rows sorted by the table sort order. Each partition writes
   `_cdcell/batches/<seq>/p<k>.json` (cells, from, to, staged files with
   spec and schema id) create-only; the committer reads these (works across
   machines, §3.2). `__ingested_at` is `planned_at`, so a rerun writes
   identical rows. A stage task that meets a new shape adds the columns
   itself (§3.2).
6. **Commit** in the committer. First write `_cdcell/batches/<seq>/commit.json`
   create-only: the included partitions with the content hash of each
   `p<k>.json`, and the fixed new fence snapshot id for this seq. Then one
   catalog transaction (Lakekeeper `/transactions/commit`): every touched
   change-log table, the `_cdcell_progress` rows (new cursors and `batch`),
   snapshot summary `cdcell.batch=<seq>`, and the fence (§3.4, summary
   `cdcell.batch` and `cdcell.last-batch`). Registry visibility
   (`cdcell.schemas`) is updated here. A staged file whose spec id is not
   the table's current default abandons the batch; the next run restages.
7. **Finish**, from `commit.json` only: checkpoint shards and pending sets
   (keys of partitions not in `commit.json` go to their pending sets);
   delete each cell's previous snapshot key (and journals).

### 3.2 Scaling out: plan, stage, commit

Each Durable Object's work needs only its own objects: a 1:1 map, no joins
or shuffles. So a run splits into three steps any runner can execute:

```
cdcell plan                 lease, fence, roll forward, evolve specs, allocate seq, drain the
                            broker (tail on: split bundles); writes the run plan and keys files
cdcell stage --task <i>     N in parallel: decode the task's partitions, write cell state
                            and Parquet, write p<k>.json per partition (staged files)
cdcell commit               read every p<k>.json; one atomic commit with the fence
```

- **Only `plan` reads the broker.** It drains it with concurrent pollers,
  writes the keys per partition (`_cdcell/batches/<seq>/p<k>.keys.json`), and
  acknowledges the messages only after those files are written. With the
  tail on, it also GETs and splits bundles into per-partition row files
  (§2.4). Stage tasks read their
  partitions' files from the state prefix: no competing consumers, no
  visibility timeout or ack deadline spanning a run, no ordering needed.
  Keys files of a failed run stay; the next `plan` merges them with new
  messages (§3.1 step 4).
- **Customers do not partition their broker.** S3 notifications filter only
  by prefix/suffix (no hashing), so one ordinary broker receives everything
  and `plan` does the partitioning. The broker carries per-cell objects (and
  bundles with the tail on), not captures, so one drainer keeps up; if it
  ever does not, `plan` runs
  several pollers on the same broker (SQS and JetStream allow concurrent
  consumers). A Kafka/Kinesis adapter whose topic is keyed by cell id
  (customer-routed) could let stage tasks read their own partitions; later.
- One machine: `cdcell run` does all three, partitions as threads (default).
- Several machines: any fan-out runner (a Kubernetes Indexed Job, Cloud Run
  job tasks, N ECS/Fargate tasks, an Airflow/Argo/Dagster fan-out, or a Spark
  job that runs `stage` on executors) runs `stage` N times, then `commit`.
  No cluster framework is required.
- A failed stage task is rerun: same plan, same cursors (frozen in the keys
  files, never read from live checkpoints), same file names. Data files are
  create-only: if a file exists, its size and hash must match, else the task
  fails. A task exits as a no-op if its `p<k>.json` already exists or its
  seq is at or below the fence head's last committed batch, so a task
  retried after the commit writes nothing. Nothing is visible until the
  commit. `commit` waits for every task's files up to
  `CDCELL_COMMIT_WAIT`; a missing partition is left for the next run (its
  cursor does not move).
- Stage tasks never commit data. Schema is the only shared thing they
  touch: a task that meets a new shape adds the columns in a schema-only
  commit with `assert-current-schema-id`. On 409 it reloads, recomputes
  `plan_columns` against the fresh schema (a name taken with another type
  becomes `<col>__<type>`) and retries with only the still-missing columns;
  adding an existing name is an error in Iceberg, never a no-op. It then
  writes files with the new field ids. Schema versions (`fingerprint → __schema_version`) are assigned
  through a conditional-PUT counter in the state prefix
  (`_cdcell/schemas/<table>/`), not a last-writer-wins table property; the
  committer copies the registry into `cdcell.schemas` for readers.

### 3.3 Retry rules

| Failure | What happens |
|---|---|
| Before the commit (any error, crash) | The batch did not happen. The next plan merges its keys files and plans again from the checkpoint cursors with a new seq. Its staged files are orphans (`cdcell maintain` removes them, §6.3). |
| Commit ambiguous (timeout, 5xx), or any 409 | Reload the fence head. Equal to our fixed new id (from `commit.json`) → committed, finish. Still our expected id → the 409 was another table (sequence number, from `maintain` or a customer's compaction): reload the tables, rebuild only the snapshot and manifest list over the new parent, reusing the staged files and manifests; retry with jitter (after an ambiguous outcome, resend). Anything else → another run took the fence: stop. So an ambiguous commit that lands late, followed by a resend that gets 409, is still committed. |
| After the commit, before the checkpoint | On start, roll forward: the fence head summary (`cdcell.batch`, or `cdcell.last-batch` after a take) names the last committed seq; finish it from its `commit.json` only, never from table snapshot summaries (which `maintain` expires) or from `p<k>.json` files not listed there. |
| Stale finisher | Checkpoint shards are written with ETag CAS and carry the last seq; a shard is never overwritten with a lower seq. Pending sets are CAS too. |
| One cell fails (unreadable object, transient error) | The cell is left out with its cursor unchanged and none of its rows staged, logged and counted, and its keys go to the pending set for the next run. Other cells continue. |

Result: **each batch is committed exactly once**, batches commit in seq
order, and every cell's rows appear once. Duplicates (rev 5 rule 4) no
longer arise from crashes.

### 3.4 Fencing inside the commit

A lease alone cannot stop a paused old run from committing late. The
catalog has to reject it. A tiny table `cdcell.<script>._cdcell_fence` has
one branch, `run`. Every batch commit includes
`assert-ref-snapshot-id(run, <id this committer last wrote>)` and advances
the branch with a new snapshot whose id the committer picks and records in
`commit.json` first, so it knows the new head without a reload. A new run
holder first commits an empty snapshot to `run` (copying
`cdcell.last-batch`, §3.1), which makes every older holder's pending commit
fail as a whole.

Verified on Lakekeeper 0.15.0:
- first take: `assert-ref-snapshot-id` with `snapshot-id: null` (branch
  absent);
- `add-snapshot` whose manifest list need not exist (no data files);
- `set-snapshot-ref` with `type: branch`;
- success is 200 with an empty body;
- a stale assert rejects the whole multi-table request with 409
  `CatalogCommitConflicts` and no table changes;
- a sequence-number conflict on a data table is 409
  `CommitFailedException`.

## 4. History detail (customer choice)

By default a cell gets one version per fold (≥ 256 captures or 32 MiB)
while it stays active between runs, and one per drain (eviction, handoff,
recovery). Finer history comes from celld settings (§5); exact events need
an insert-only events table in the app. With `CDCELL_TAIL=on`, rows still
in bundles give per-capture versions (§2.4). Within a capture, SQLite
commits are already merged (§1.2).

**Restarts between runs.** Every activation advances the epoch (A6). A
database under `CELLD_LTX_PAGED_MIN_MB` (default 256 MiB) gets a snapshot
epoch: it opens with a TXID-1 full image that already holds the first new
writes, and nothing records which TXID of the old epoch was restored (D1).
Read hours later, after `own.json` moved on, the old epoch's last objects
cannot be proven. So those unproven objects and the new epoch's first
writes collapse into one net change at the new opener: across a restart
between runs, one version per activation for the unproven part. With
`CELLD_IDLE_EVICT_S` or frequent deploys this is common. Paged epochs carry
the cut (D2) and keep their grain. Shorter run intervals and the opt-in
live tail reduce the unproven part.

Later: on a versioned bucket, GC'd bundles stay readable as noncurrent
versions, which gives per-capture history without bucket mode. That needs
a fold check (not built): bundle rows must replay to the L1 that covers
them.

## 5. celld settings and what they mean for cdcell

These settings belong to the customer's celld deployment, not to cdcell.
cdcell's freshness, history detail and some guarantees depend on them.

**Recommended:** cdcell `CDCELL_TAIL=off` (the default); celld
`CELLD_IDLE_EVICT_S` set (e.g. `300`); celld's epoch GC off (its default) or
with a grace longer than cdcell's longest outage.

| Setting | celld default | Freshness | History detail | Guarantees | Cost / trade-off |
|---|---|---|---|---|---|
| `CELLD_IDLE_EVICT_S` | unset | an idle cell is evicted after N s; eviction writes its changes to a per-cell object, so the next run sees them. Bounds freshness for cells that go idle | each later activation starts a new epoch (A6); across a restart between runs, a snapshot epoch collapses the unproven part (D1, §4) | none on validity | more activations and epochs |
| `CELLD_LTX_COMPACTION_MIN_TXIDS`, `CELLD_LTX_COMPACTION_MIN_MB` | 256 captures, 32 MiB | lower values fold more often: fresher for busy cells and for slow cells that are never idle | finer: one version per that many captures | none on validity | more celld PUTs; more objects to read on takeover and restore |
| `CELLD_LTX_RETENTION_SECS` (epoch GC) | off | none | off: nothing cdcell needs is deleted. On, with a grace shorter than cdcell's longest outage: history in the gap is lost | fallback resync: current state stays exact, rows are restated as one net change (the only exactly-once exception) | storage, if left off |
| `CELLD_DURABILITY` | `fleet` | `bucket`: per-cell objects are written at once, so fresh every run | `bucket`: one L0 per upload pass, usually per capture (finest) | none on validity | `bucket`: write acks wait for S3 |
| `CELLD_LTX_PAGED_MIN_MB` | 256 | none | databases at or above it use paged epochs, which carry the cut (D2), so history is kept across restarts; below it, snapshot epochs collapse the unproven part | none on validity | a celld restore-path choice; nothing for cdcell |
| cdcell `CDCELL_TAIL=on` (for contrast, §2.4) | off | fresh every run for every cell | per capture for unfolded changes | validity has a rare, detected, self-correcting exception (C12) | bundle notifications and GETs, node lease reads, tail journals |

**Guaranteed regardless of settings:**
- validity (with the tail off);
- order by `(__epoch, __txid)`;
- consistent views per object;
- exactly once per batch (except the fallback resync);
- current state exact.

**Depends on settings:** freshness and history detail (table above).

**Known gap.** With the recommended defaults, a slow cell that is never idle
long enough to be evicted (e.g. one write every 10 min with
`CELLD_IDLE_EVICT_S=900`) appears only after its next fold: 256 captures,
about 42 h in that example. Levers: a lower
`CELLD_LTX_COMPACTION_MIN_TXIDS`, or the opt-in tail.

## 6. Iceberg layout

### 6.1 Partitioning and sort order (change-log tables)

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
  table's sort order, applied when writing each file; `__op` breaks ties so
  the order is total and reruns write identical rows). Per-object queries
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
- Files: zstd Parquet, target 256 MiB (`write.target-file-size-bytes`).
  Each stage task writes one file per (table, partition value) unless it is
  larger, named `b<seq>-t<i>-NNNNN.parquet`; in-process `run` is one task,
  so one file per (table, partition value). Format v2, append only.
  cdcell pins Parquet `created_by` (`set_created_by("cdcell")`), so a
  rerun writes identical bytes even across library upgrades; create-only
  writes (§3.2) check size and hash.

### 6.2 Other tables

- `_cdcell_progress`: `day(committed_at)`, sorted by `_cell_id`; one row per
  cell per batch. The newest row per cell is its cursor, and the checkpoint
  can be rebuilt from it.
- `_cdcell_kv`: like change-log tables.
- `_cdcell_fence`: no data, one branch.

### 6.3 Maintenance

`cdcell maintain` (scheduled, e.g. daily): expire snapshots (never the
fence branch head), remove orphan files of abandoned batches, delete
metadata after commit. An orphan is deleted only if its seq is below the
fence head's last batch, it is listed in no `commit.json`, and it is older
than an age floor (`CDCELL_ORPHAN_MIN_AGE`, default 24h); never a file whose
seq is at or above the newest `batches/<seq>.json`, so files of a run in
flight survive. It also bin-packs small files of past days within
each partition, keeping the sort order. Customers' engines can do the same
(`OPTIMIZE`, `rewrite_data_files`). These commits can conflict with a run's
commit; the run retries (§3.3).

## 7. Cost per run

| Item | Requests |
|---|---|
| Per dirty cell | 1 `own.json` GET; 1 GET per new celld object on the chain (by default one per fold); a snapshot GET unless cached; a snapshot PUT only per `CDCELL_SNAPSHOT_EVERY` (every changed run for paged cells, §2.3) |
| Per LISTing cell | gap: 3 LISTs (L0, L1, L9), plus a full L0 LIST if a straddling L0 kept the gap open; epoch change: 1 delimiter LIST (every epoch dir, paginated, since dirs are unpadded decimal) plus 3 per walked epoch. Epoch changes are routine (A6), so expect this for cells restarted since the last run |
| Per run | the broker drain; one lease write plus renewals; one catalog transaction per batch (several for a backlog, §3) |
| Per batch | one Parquet file per (table, partition value, task); `commit.json` and keys files; the commit reads manifests (`check_duplicate`, kept and measured) |
| Tail on only (§2.4) | per run: one GET per notified bundle key (most 404), node lease GETs; per cell: a journal PUT when the cursor ends in tail rows |

Compared with revision 5:
- no read per capture;
- no confirmation reads every second;
- no commit every 60 s: at the default 3 h interval, 180× fewer Iceberg
  snapshots and files;
- no always-on worker.

`docs/cost.md` will measure this on the e2e stack (MinIO request metrics),
as a function of cells, write rate, restart rate and run interval, and with
celld `CELLD_LTX_COMPACTION_MIN_TXIDS=16`.

## 8. Deployment

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
| Message broker | a small adapter | SQS, NATS JetStream; others (Kafka, Pub/Sub) are an adapter each |
| Consumers | Iceberg tables | Spark, Trino, ClickHouse, DuckDB, Snowflake, BigQuery (BigLake), Athena, Flink |

**Any message broker works** if notifications can get into it and cdcell has a reader
for it: cdcell only needs at-least-once delivery, retention longer than the
longest outage, and read-then-acknowledge. Customers choose; a forwarder
(a function, a webhook, a connector) can bridge any storage provider to any
broker. Recommended setups that need no forwarder:

| celld bucket on | Native notification targets | Recommended broker |
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

## 9. Operating requirements

1. celld's epoch GC off (default), or its grace above the longest gap
   between successful runs (§5). Otherwise the fallback resync applies:
   current state exact, history in the gap lost.
2. Read access to the celld bucket; read/write to the lake bucket, the
   catalog and `CDCELL_STATE_URL` (its own IAM and KMS, §2.3; the policy
   ships in `docs/deploy.md`).
3. A scheduler: cron, Kubernetes CronJob, ECS scheduled task (`cdcell run
   --once`); or one long-running `cdcell run` that repeats every
   `CDCELL_RUN_INTERVAL` (default `3h`; any duration, e.g. `15m` or `24h`).
4. Required: notifications of every object created under `cells/` (suffix
   `.ltx`) to a message broker (SQS or JetStream), at least once, as S3
   delivers (G2, G5). With `CDCELL_TAIL=on`, also under `log/` (suffix
   `.ltxb`). A lost notification is a setup fault (G3), not a cdcell bug.
5. Broker retention longer than cdcell's longest outage (SQS up to 14 days;
   JetStream `max_age`). Without it, a quiet cell's final objects may never
   be exported: no later write reveals the gap.

## 10. What carries over from the streaming code

| Kept | Changed or replaced |
|---|---|
| decoder (`cdcell-core`), emit, schema registry and evolution (now committer-only), `iceberg_out` staging and atomic commit, resync fallback, keys, metrics, IAM policy, maintain | `pipeline.rs` streaming state machine, `run.rs` intake loop, gap timers, in-memory parking, JetStream ack handling |
| | `resync::plan_chain`: not celld's `EpochChain`. celld skips an intermediate epoch whose clipped objects do not end at the cut and keeps walking down, and skips an empty newest epoch (`crates/ltx/src/client/epochs.rs:50-75`); `plan_chain` returns None. Port `EpochChain` exactly. |
| | Rev-5 tables use `bucket(16, __cell_id)`: adopting `day(__ts)` needs partition evolution in `plan` (no rewrite, §3.1). |
| | Rev-5 file names are random and rows unsorted: now deterministic names, create-only writes and sorted rows (§3.1). |

The test kit (fake celld with real SQLite and LTX, `MemSink`, paused time)
carries over: tests enqueue notifications, then call `run_once`.

## 11. To verify or build before shipping

1. Freshness of quiet resident cells with `CELLD_IDLE_EVICT_S` and without;
   tail on vs off (e2e).
2. celld with `CELLD_LTX_COMPACTION_MIN_TXIDS=16`: write-path and takeover
   cost (`docs/cost.md`).
3. The finest-chain planner against every overlap celld produces (merged
   drain L0s, L1s from the bundle overlay, recovery windows, straddles):
   property test in the test kit.
4. The `EpochChain` port, property-tested against celld's crate.
5. L0 straddles (§2.1): reproduce a drain L0 over earlier L1s in the test
   kit; the full-L0 fallback must close every such gap.
6. Marker identification by content (§2.2): zero pages and `wal_size == 0`
   on real celld markers; no bucket-mode `n-n` L0 matches.
7. `add-spec` / `set-default-spec` on a rev-5 table, and `DataFile` JSON
   across processes with an evolved schema.
8. Lakekeeper fencing (§3.4) is verified on 0.15.0; keep it as an e2e
   test.

## 12. Planned

- `cdcell backfill`: a one-time command the operator runs at install. It
  LISTs `cells/` once and exports cells that existed before cdcell and
  never write again. It is a manual command, not part of runs.
- The live tail (§2.4): opt-in, built after the default path.
- Multi-container workers with one committer per partition. Concurrent
  commits to shared tables conflict on sequence numbers (§3), so this needs
  the rebase-retry of §3.3 on every commit and serialized schema updates.
