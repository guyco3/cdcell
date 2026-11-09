# cdcell v1 design

cdcell turns the SQLite changes of celld Durable Objects into change events
(CloudEvents + Debezium envelope) in Apache Iceberg tables in the customer's
object storage. It reads only what celld already writes to its bucket.

Companion documents:
- [`invariants.md`](invariants.md): what celld, object storage and notifications
  guarantee. Referenced here by ID (A1, C9, …). Read it first.
- [`../e2e/README.md`](../e2e/README.md): the local fleet used to test all of this.

Status: draft for v1, revision 3, after three design reviews. Items marked
**[verify Mn]** are checked at milestone n.

---

## 1. Contract

**cdcell does**
- consume bucket notifications for the objects celld writes;
- turn LTX page diffs into row-level change events;
- write them as append-only Iceberg tables through an Iceberg REST catalog
  (Lakekeeper) into the customer's storage, and maintain those tables;
- guarantee, per Durable Object (cell):
  - **completeness and order:** every change that is part of the cell's
    restored history is emitted, in `(epoch, txid)` order, with net
    semantics per capture batch; the one exception is a loss celld itself
    declares (`*.loss.json`, §9.1), which cdcell records and flags rather
    than hides;
  - **no phantoms:** nothing outside that history is ever emitted;
  - **delivery:** at-least-once into Iceberg with deterministic event ids.
    In normal operation each event is written once; after a crash during a
    commit, duplicates are possible and consumers dedupe on
    `(ce_source, ce_id)`.

**cdcell does not**
- run or manage the customer's OLAP engine, CDC database or SCD jobs;
- change celld, or install anything inside a Durable Object;
- promise per-statement or per-transaction events: the unit is the capture
  batch (one LTX TXID or merged range, B3-B4), which never splits a SQLite
  transaction but can merge several. Granularity can be coarser still: in
  single-node mode an epoch's unconfirmed tail usually collapses into one
  epoch-boundary diff (§5.5), and when bundles are folded and deleted before
  cdcell reads them only the fold's net change remains (§7);
- promise real time: target lag is under 2 minutes at p99.

**Cost goal:** variable cost well below celld's own bucket cost for the same
writes (§13).

## 2. Terms

| Term | Meaning |
|---|---|
| cell | one Durable Object instance = one SQLite DB, scope `Class:<64 hex>` |
| epoch | ownership generation of a cell (`own.json`); in every LTX key `e<epoch>` |
| TXID | one capture pass of a cell's WAL; contiguous within an epoch |
| object | an LTX file (`cells/.../*.ltx`) or a bundle (`log/.../*.ltxb`) |
| row | one entry in a bundle footer: `(cell, cell_epoch, txid, L0 bytes)` |
| unit | what the per-cell pipeline orders: one LTX file, or one bundle row |
| shadow | cdcell's copy of a cell's pages at its **confirmed point** |
| confirmed point | `(epoch, txid)` up to which units are proven history and applied |
| committed point | `(epoch, txid)` up to which events are committed to Iceberg |
| suspect | a unit visible but not provable yet; parked, never emitted unresolved |
| opener | a unit that fixes an epoch's base: any L0 or L1 with `min_txid == 1`, or a paged marker (§5.5) |
| full image | a unit containing every page `1..commit`: an opener with `min_txid == 1`, an L9 `1-max`, or a mid-epoch boundary snapshot (B5) |
| predecessor of E' | the epoch whose history E' continues, proven by LIST (§5.5) |
| shard | `hash(cell) mod S`; exactly one owner applies a shard's cells |

## 3. Architecture

```
celld bucket ──notifications──► broker (SQS std | NATS JetStream)
     ▲                                │
     │ GET objects, own.json,         ▼
     │ nodes/*.json, rare LIST   ┌──────────┐  units by cell  ┌──────────────────┐
     └───────────────────────────│  intake  │────────────────►│ shard owner(s)   │
                                 └──────────┘                 │ order, confirm,  │
                                                              │ apply, decode,   │
                                                              │ emit, park       │
                                                              └────────┬─────────┘
                                                                       ▼
                       customer bucket ◄── committer: Parquet + batch manifest + Iceberg
                       (warehouse + _cdcell/ state)   commit via Lakekeeper + checkpoints
```

- **Intake** is stateless: decode the key, drop what the key alone allows,
  GET the object, split bundle rows by cell, route each cell's units to its
  shard owner. Bundles hold many cells (C2), so routing happens after the
  GET, not in the broker.
- **Shard owner**: exactly one active owner per shard. v1 runs **one process**
  with all S shards in-process (default S = 64), one intake pool and one
  committer.
- **Committer**: one per process; owns Iceberg commits and checkpoints.
- **Process lease (v1 fencing).** Before doing any work the process takes the
  global lease `_cdcell/lease.json` with a conditional PUT
  (`If-None-Match: *` to create, `If-Match` to take over an expired one):
  `{"version":1,"holder":"<host>/<pid>/<uuid>","epoch":N,"expires_ms":…}`,
  TTL 30 s, renewed every 10 s by CAS. Its `epoch` is a fencing token written
  into every batch manifest and checkpoint; a process that fails to renew
  stops committing at its expiry and exits. Two processes can never commit
  at once (fixes rolling deploys). Per-shard leases for multi-process
  operation are deferred (§15).
- **Durable state**: unacknowledged broker messages, plus `_cdcell/` in the
  customer bucket (lease, checkpoints, batch manifests, parked units, shadow
  checkpoints, registry caches). RAM and local disk are caches.

### 3.1 Configuration
- `source.bucket`, `source.prefix` (celld's optional fleet key prefix, B1;
  applied to every key cdcell reads, lists or filters), endpoint, credentials.
- **Class → script map.** Cell keys carry only `Class:<hex>`, but `ce_source`,
  namespaces and `id-from-name` need the script. cdcell reads
  `deploy/current.json` and, for each script pointer `deploy/<script>/current.json`,
  that version's `deploy/<script>/<version>/manifest.json`, whose
  `do_classes` lists the classes the script defines (`protocol.rs:23`).
  Refreshed every 5 min. A class claimed by two scripts, or found in none, is
  an error for that class (metric + alert; its cells are held) unless
  `config.class_scripts` overrides it.
- `catalog` (Lakekeeper URL, warehouse), `state.prefix` (default `_cdcell/`
  in the warehouse bucket), `ce_type_prefix`, shard count, timeouts.

## 4. Per-object pipeline (intake)

1. **Decode the key.** Strip `source.prefix`; URL-decode with form rules
   (`:` arrives as `%3A`, G4). Recognize:
   `cells/<cellpath>/ltx/e<E>/<level:04x>/<min:016x>-<max:016x>.ltx`
   (cellpath may contain `/facets/`), `cells/<cell>/own.json`,
   `log/<node>/<gen>/bundle/e<LE>-<seq:08>.ltxb`,
   `log/<node>/<gen>/recovered/<sha>.json`,
   `log/<node>/<gen>.e<N>.loss.json`, `log/<node>/<gen>.bundle-<name>.loss.json`
   (§9.1). Ignore everything else.
2. **Drop from the key alone (no GET):** facets (v1, §15); an epoch lower than
   the cell's predecessor-resolved floor (§5.5); `max ≤ committed txid` in that
   epoch (the committed point, not the confirmed point, so a restart never
   drops redelivered units it has not committed); L1 whose range is covered
   and an L9 re-take of a covered range. Remember L1/L9 keys as candidates
   for gap repair, rule 3 and bootstrap.
3. **GET immediately** (bundles can be GC'd ~30-40 s after coverage, C8).
   A 404 means covered elsewhere (C8, F4): record it, rely on the covering
   per-cell object's notification or on gap repair (§7), and acknowledge.
4. **Split and route.** A bundle becomes one unit per footer row (each is a
   verbatim single-TXID L0 file, C4); an LTX file is one unit. Each unit
   carries `(source key, ETag, GET time)`.
5. **Track the message.** A per-message pending counter (in memory) counts
   its units; a bundle's units can span shards. The message is acknowledged
   when the counter reaches zero (§9). If the process crashes, unacked
   messages are redelivered and their units are dropped or reprocessed by
   the committed points: nothing is lost, work may repeat.

## 5. Per-cell processing (shard owner)

State per cell (§11.2 checkpoint): epoch view, confirmed point, committed
point, ordered buffer of units keyed by `(epoch, min_txid)`, shadow, parked
flag and parking index, last record-read start time per record.

### 5.1 The shadow advances only on confirmed units
- Units are buffered in arrival order and processed in `(epoch, txid)` order.
- A unit is **applied to the shadow only after it is confirmed** (§5.2), and
  only when it is next: `min ≤ confirmed_txid + 1 ≤ max + 1` in the current
  epoch. Applying computes and emits its rows (§6) and moves the confirmed
  point to `(epoch, max)`.
- The shadow is therefore exactly the state at the confirmed point. Nothing
  is ever undone; phantoms are never applied.
- Merged or overlapping units carry the final page images at their max TXID
  (B4); applying one at `confirmed_txid ∈ [min-1, max]` yields the net change
  to `max`. Events are stamped with `_txid = max`.
- A mid-epoch full image needs no special case (B5).
- Confirmation reads are batched about every 10 s, so emission lags arrival
  by up to ~10 s, within the lag target.
- The shadow keeps, per page, the version as of the last **committed** batch
  as well as the current version (copy-on-write, tagged with batch seq), so a
  shadow checkpoint can always be written at the committed point (§11.1).

### 5.2 Confirmation (the proof rule)
A unit is history once every future restore must contain it. The proof is
ordering: a successor must CAS a record before it reads the data (D5, D6).
If cdcell sees the data and afterwards reads that record unchanged, the
successor's later read includes the data.

| Unit from | After the GET completed, read | CONFIRMED if |
|---|---|---|
| bundle `log/<node>/<gen>/bundle/e<LE>-…` | `nodes/<node>.json` | `ownership_index_generation` (or `probe_public_key` when absent, `ownership_store.rs:49-53`) equals `<gen>`, **and** `log.state == "open"` with `log.epoch ≥ LE` (C12) |
| per-cell `cells/<cell>/ltx/e<E>/…` | `cells/<cell>/own.json` | `epoch == E` (any node, or released `""`) |

- **"After"** means the record read started after the unit's GET completed.
  The shard owner keeps, per node and per cell, the start time of its last
  read; a unit is decided by the first read that started after its GET.
- Reads are batched: one `nodes/<node>.json` read per node and one
  `own.json` read per cell per ~10 s round, covering every unit waiting on it.
- Lease JSON fields used: `ownership_index_generation`, `probe_public_key`,
  `log.state` (`open` / `recovering` / `sealed`), `log.epoch`
  (`ownership_store.rs:34-63, 218-243`).
- A bundle row whose `cell_epoch` is lower than the cell's current epoch is
  still confirmed by the lease alone: a graceful handoff drains the cell into
  per-cell history before ownership moves (C13), and an ungraceful one seals
  the session (SUSPECT).
- celld's own ack credit also requires `log.epoch == LE` (C9); that gates
  acknowledgements, not history. A healthy session drains every row it
  bundled, and recovery folds bundles of every log epoch (C12, D6).
- **SUSPECT** results: lease 404; generation mismatch; no `log` field
  (C10-C11); `log.state` `recovering` (with or without a claimant) or
  `sealed`; `log.epoch < LE`; `own.json` epoch > E. SUSPECT means
  unconfirmed, not phantom (D10).
- `own.json` 404 (cell never claimed): not suspect; wait, and let gap repair
  look (§7).
- **`own.json` notifications** can only make units SUSPECT (epoch > E). They
  never confirm: a notification proves a CAS happened, not the state after a
  GET (D14). CONFIRMED always needs a read that started after the GET.

### 5.3 Parking
When a unit becomes SUSPECT, the cell enters **parked** mode:
- the suspect unit and **every later unit buffered for the cell** (any epoch)
  are appended to the cell's parking area, then their messages are
  acknowledged (broker retention and DLQ redrive must never decide history);
- later units for the cell keep going to the parking area until resolved;
- units are stored as their L0 bytes, not keys: sealed sessions' bundles are
  deleted after recovery (C8).

Layout, append-only, one segment per parked unit:
`_cdcell/parked/<cell-id-hex>/<epoch>/<min:016x>-<max:016x>-<etag8>.ltx`
(the verbatim L0 file; `etag8` = first 8 hex of the source ETag, so a crash
between parking and ack that re-parks the same unit rewrites the same key:
deduped by `(key, ETag)`). Metadata per segment (source key, ETag, reason,
time) goes in S3 user metadata. The checkpoint records only the parked flag;
**the parking area is authoritative**: on startup cdcell LISTs
`_cdcell/parked/` and re-parks every cell found there. Segments are deleted
when the cell resolves. Alert on parked cells by age.

### 5.4 Resolving a parked cell (first rule that applies, each round)
0. **Per-cell units confirmed by `own.json`:** a parked per-cell unit of
   epoch E is history if an `own.json` read that started after its GET shows
   epoch == E (D5). This holds even when E's bundle writer is sealed, and it
   covers recovery's merged L0 uploads, which land before the `own.json` CAS.
   Apply such units in order from the parking area.
1. **Recovery progress records (fast path):** only when the writer's log is
   **sealed** and the record's `epoch` equals the unit's cell epoch. Records
   `log/<node>/<gen>/recovered/*.json`
   (`{"version":1,"cells":[{cell,epoch,through}]}`) are hints, written once
   per 32 cells with no final flush (invariants OQ 15). Units with
   `max ≤ through` are history. Nothing above `through` is decided.
2. **A later epoch opens with a paged marker** `e<E'>/0000/(c+1)-(c+1)`.
   First prove its predecessor P by LIST (§5.5). If P == E: units of E with
   `max ≤ c` are history (apply in order); units with `max > c`, including
   ones straddling the cut, are phantoms: drop. If `c > confirmed_txid` and
   units in `(confirmed, c]` are missing, fetch them (§7) and apply. If
   `c < confirmed_txid`: the proof rule was violated; alert and resync the
   cell (§10). If P > E (an intermediate epoch has objects): resolve E against
   P's history first (P's opener or a full image of P, rule 3), then apply the
   cut to P.
3. **Any full image of a later epoch E'** (an opener with `min_txid == 1`, an
   L1 `1-…`, or an L9 `1-max`) at TXID t: drop the remaining parked units of
   E; diff the image against the shadow (= confirmed point); emit the net
   changes as synthetic events stamped `(E', t)`; the confirmed point becomes
   `(E', t)`; skip E' units with `max ≤ t`. Correct whether or not the parked
   units were history, and independent of how E' opened; only per-TXID detail
   is lost.
4. **Nothing yet:** stay parked. A phantom is final only once a later epoch
   has an opener (D9). A snapshot epoch's TXID-1 file appears only at the
   successor's first durable write (D1), so this can last; alert, never guess.

Raw per-cell prefix contiguity is never evidence by itself: a degraded zombie
shipper or a fenced compactor can upload per-cell objects after a seal (D10).
Per-cell units count only with the `own.json` read of rule 0.

### 5.5 Epoch transitions, openers and predecessors
- Units of higher epochs are **GETted, inspected and buffered** while a lower
  epoch is unresolved; they are applied only after it resolves.
- **Opener detection** (needs the header, so after the GET):
  - `min_txid == 1` at level 0 or level 1: a snapshot opener and a full image
    (D1). In fleet mode the first per-cell object is often an L1 `1-256`,
    because TXID 1 went into a bundle (D15).
  - **Paged marker:** `min == max`, zero page frames, `wal_size == 0` (D2).
    The cut is `min - 1`; TXIDs continue at `min + 1`.
  - An L9 `1-max` is **not** an opener: handoff snapshots are always `1..max`,
    also in paged epochs (invariants OQ 16). It is a full image, usable by
    rule 3, bootstrap and resync.
  - A first unit of an epoch with `min > 1` that is not a marker: wait for the
    opener (gap repair may find it).
- **Predecessor proof.** Before applying a paged cut, LIST
  `cells/<cell>/ltx/` with delimiter `/` to get the cell's epoch prefixes, and
  walk down from E' exactly as `celld_ltx::client::epochs::EpochChain::build`
  does (`crates/ltx/src/client/epochs.rs:39-80`, D3): the predecessor is the
  highest epoch below E' that has objects whose clipped range ends exactly at
  the cut. Epochs with no objects are skipped. This LIST is rare and cheap.
- **Normal (non-parked) transition from E to E'** goes through E''s opener:
  - snapshot opener or other full image: diff against the shadow, emit
    synthetic events stamped `(E', t)`;
  - paged marker: prove the predecessor; fetch and apply E up to the cut if
    `c > confirmed` (these units are history even if their notifications were
    lost); `c < confirmed` → alert + resync.
- An epoch is **resolved** once the cell's confirmed point has moved into a
  later epoch. Units of resolved epochs are dropped by key (§4.2).
- Every activation advances the epoch (A6): expect frequent transitions. In
  single-node mode `own.json` usually moves before the next 10 s confirmation
  round, so an epoch's last units often resolve by rule 3: correct, but one
  coarser diff per activation. Fleet mode does not have this cost, because
  bundles confirm by lease regardless of `own.json`.

### 5.6 Duplicates and resets
- Skip units with `max ≤ confirmed txid` in their epoch (and, at intake,
  `max ≤ committed txid`, §4.2).
- **Reset detection** compares ETags only for keys in the active window: the
  checkpoint's `seen` map holds `key → ETag` for units of the current epoch
  above the committed point. The same key with a different ETag there is a
  reset (e.g. a facet recreated, F2): resync the cell (§10). L9 keys are
  excluded (a re-taken L9 differs only in its timestamp, F2). Below the
  committed point keys are dropped without comparison.
- Deterministic event ids (§8.5) let consumers dedupe anything that slips
  through after a crash.

## 6. Page → row decoder

Exact algorithm, validated on ~1,700 transactions and live data (E1-E6).

1. `dirty = changed pages ∪ pages beyond commit`.
2. Re-read `sqlite_schema` only when the schema cookie (page 1, bytes 40-44)
   changes. Column metadata (names, declared types, defaults, hidden and
   generated columns) via a read-only SQLite connection over the shadow image.
3. Re-walk a table's interior pages only if its root or interior pages are
   dirty.
4. **Map dirty overflow pages to the leaf owning the cell** (required: a
   same-size overwrite can change only overflow pages, E3).
5. Decode rows from changed entry pages before (shadow) and after
   (shadow + changed pages). Key: the declared PK; tables without a declared
   PK use the rowid (`INTEGER PRIMARY KEY` is the rowid); WITHOUT ROWID tables
   use their PK fields. Compare **decoded values by column name**, not bytes:
   before only = `d`, after only = `c`, both with any value difference = `u`,
   no difference = nothing. So delete + re-insert of the same PK is a `u`, and
   DROP COLUMN, ADD COLUMN and table rebuilds emit only rows whose surviving
   values changed.
6. Apply the changed pages to the shadow; drop pages beyond `commit`.

**PK changes.** Keyed by PK, a PK change is a `d` of the old key and a `c` of
the new key. For rowid tables with a declared non-INTEGER PK, when the same
rowid carries the old and the new PK in one batch, both events carry
`_old_key` / `_new_key`. WITHOUT ROWID tables cannot tell a PK change from an
unrelated delete + insert: plain `d` + `c`. Within one batch a key appears at
most once per table.

**Table rebuilds.** SQLite's rebuild pattern (`CREATE new_t; INSERT … SELECT;
DROP t; ALTER TABLE new_t RENAME TO t`) can span batches. Rule: a table that
is renamed onto the name of a table dropped earlier (in the same or an
earlier batch, tracked per cell in the schema state) is redirected to the
dropped table's Iceberg table. Its rows are diffed by PK against the last
known rows of `t` (kept in the shadow's schema state until the redirect
resolves) and emitted there; the transient `new_t` gets a `schema_changes`
row of kind `rebuild_artifact`, and rows already emitted under `new_t` stay
there (documented as artifacts). Within one batch no artifact rows appear.

**Implementation.** Rust, reusing `celld_ltx` (Apache-2.0) for LTX and bundle
decoding, epoch chains and `build_database_image`; a read-only b-tree/record
module (~700 lines) modelled on turso's MIT `sqlite3_ondisk.rs`; rusqlite for
schema metadata only; a pure-Rust V8 deserializer for KV values.

**Tables.** User tables (unprefixed, same DB, E4) and `_cf_KV` (KV table,
§8.7). Skipped: `_litestream_*`, `_cf_WAKE`, `_cf_FACETS`, `_cf_METADATA`,
`_cf_ALARM` (v1), `sqlite_*`, virtual tables and their shadow tables.

## 7. Gaps, lost notifications, 404s

Sources for a missing unit, in order:
1. **Per-cell:** LIST `<prefix>cells/<c>/ltx/e<E>/0000/`, `0001/` and `0009/`
   from the start of the epoch prefix and keep objects with
   `max > confirmed txid`. Listing from the start (not `start-after` the
   confirmed TXID) catches merged and overlapping files with
   `min ≤ confirmed < max` (B4). Per-cell prefixes are small within an epoch;
   paginate if needed.
2. **Live bundles (fleet mode, where per-cell prefixes can stay empty, C7):**
   enumerate sessions with `LIST <prefix>log/` delimiter `/` and then each
   node's generations; keep sessions whose lease (`nodes/<node>.json`) is
   Open for that generation; `LIST log/<node>/<gen>/bundle/` and GET bundles
   not yet processed; filter rows by cell. Bundles of live sessions are not
   GC'd until per-cell objects cover them (C8). Between the two sources,
   **every TXID's final state is always available**: before a fold, the
   bundles hold each TXID; after a fold, the fold holds the net state
   (finer granularity may be gone, data is not).
3. `own.json`, for epoch changes.

Triggers:
- **Gap timeout** T_gap = 5 min (configurable) on a cell whose next unit is
  missing.
- **Tail reconciliation** every 5 min: LIST each live session's bundle
  prefix. Do not infer loss from seq gaps (C5); compare against processed
  keys.
- An `own.json` event for a cell with buffered or parked units: LIST that
  cell's prefixes.
- **Shared scans:** one live-bundle scan per round serves every cell that
  needs source 2 (gap repair, bootstrap, replay); bundles are GET once and
  their rows routed to all interested cells.

Each LIST costs ~$0.000005.

## 8. Output contract (frozen for v1)

### 8.1 Tables and names
- Iceberg **format-version 2**, append-only, no delete files.
- Namespace `cdcell.<script>.<class>`; one table per SQLite table. Reserved
  tables per class namespace: `_cdcell_kv`, `_cdcell_schema_changes`; per
  script namespace `cdcell.<script>`: `_cdcell_progress`, `_cdcell_cells`,
  `_cdcell_losses`.
- Identifier rule: lowercase; every character outside `[a-z0-9_]` becomes `_`;
  a user table whose result starts with `_cdcell_` gets a `u_` prefix.
  Collisions (e.g. classes `Shop` and `shop`) get `_2`, `_3`, … in order of
  first creation.
- **The catalog is the source of truth.** Each table carries Iceberg table
  properties `cdcell.original-name` (class or table name before the
  identifier rule) and `cdcell.schema-versions` (§8.6); field ids come from
  the Iceberg schema. Namespaces carry `cdcell.original-name` too. The
  registry objects under `_cdcell/registry/` are caches rebuilt from the
  catalog on start. Creating a table and its properties is one catalog
  operation; schema and property changes go through Iceberg commit
  requirements (`assert-current-schema-id`, `assert-table-uuid`), so a failed
  commit never leaves the cache ahead of the catalog: the cache is updated
  only after the commit succeeds.
- Partitioned by `day(ce_time)`, `bucket(16, _cell_id)`; write order
  `_cell_id, _key_json, _epoch, _txid`.
- Created lazily on first event; committed only when they have rows.

### 8.2 Columns (every change table)

| Column | Type | Meaning |
|---|---|---|
| `ce_id` | string NOT NULL | CloudEvents id (§8.5) |
| `ce_source` | string NOT NULL | `/cdcell/sqlite/<script>/<class>/<cell>`; percent-encode `/` and `%` |
| `ce_specversion` | string NOT NULL | `1.0` |
| `ce_type` | string NOT NULL | `<ce_type_prefix>.cdcell.sqlite.DataChangeEvent` (default prefix `dev`) |
| `ce_subject` | string | `<table>/<percent-encoded pk_json>` |
| `ce_time` | timestamptz NOT NULL | LTX header capture time (owner wall clock; display only, B9) |
| `ce_datacontenttype` | string | `application/json` |
| `ce_dataschema` | string | `urn:cdcell:schema:<script>:<class>:<table>:<schema_version>` |
| `ce_sequence` | string | zero-padded `epoch(20) ‖ txid(20) ‖ total_order(10)` |
| `ce_partitionkey` | string | `<script>/<class>/<cell>` |
| `ce_recordedtime` | timestamptz | when cdcell produced the event |
| `_cell_id` | string NOT NULL | the Durable Object, `Class:<64 hex>` |
| `_epoch`, `_txid` | long NOT NULL | ordering key; compare as the pair |
| `_key_json` | string NOT NULL | **canonical key** (§8.5), on every op; comparable across cells and schema versions |
| `_key` | struct (nullable) | typed key columns of the table's first key schema (rowid tables: `struct<rowid: long>`); null for cells whose key schema differs |
| `_old_key`, `_new_key` | struct | set on the `d`/`c` pair of a detected PK change (§6) |
| `op` | string NOT NULL | `c` / `u` / `d` / `r` |
| `before`, `after` | struct<all cols> | full row images (§8.3) |
| `_raw` | map<string,string> | values that do not fit their column type, JSON-encoded |
| `source` | struct | `version, connector="cdcell", name, ts_ms (capture time), ts_us, snapshot, db=<cell>, sequence, table, change_id=<txid>, script, cell_class, cell_id, epoch, txid, synthetic` |
| `transaction` | struct | `id="<epoch>:<txid>", total_order, data_collection_order` |
| `ts_ms`, `ts_us` | long | processing time (Debezium's top-level semantics) |

No packed `__seq`: Iceberg has only signed 64-bit integers and epochs churn,
so any bit-packing can overflow. Order by `(_epoch, _txid)` (H5).

`transaction.id` is `"<epoch>:<txid>"`, unique within the cell (the cell is
already in `ce_source` and `_cell_id`), and identical to the extension
`iodebeziumtxid`. `transaction.total_order`: 1-based position of the event
within its batch, ordering events by table name (bytes), then `pk_json`
(bytes). `data_collection_order`: the same, restricted to one table.
`source.ts_ms` and the extension `iodebeziumtsms` carry the capture time;
top-level `ts_ms` is processing time.

### 8.3 Type mapping

| SQLite declared affinity | Iceberg | Notes |
|---|---|---|
| INTEGER (incl. INTEGER PRIMARY KEY) | long | |
| REAL | double | ±Inf as JSON strings; SQLite stores NaN as NULL |
| TEXT | string | |
| BLOB (declared) | binary | base64 in JSON |
| NUMERIC | double | values that would lose precision go to `_raw` |
| untyped (`CREATE TABLE t(a,b)`) | string | canonical JSON scalar: number, string, or `{"$b64":…}` |

A value whose storage class does not fit (text in an INTEGER column, an
integer above 2^53 in NUMERIC) leaves the typed column null and records the
original in `_raw`. Recommend STRICT tables to users.

### 8.4 Ops
- `c` / `u` / `d`: streamed changes (plain ids). This includes a brand-new
  cell: its first opener is diffed against an empty shadow, which yields `c`
  rows with plain ids (`source.synthetic = false`); a new cell is not a
  bootstrap.
- `r`: emitted only by bootstrap (§10) of a cell with **no committed history**
  that is first seen mid-life (cdcell started after the cell existed, or the
  objects needed to stream it from TXID 1 are gone), with `source.snapshot` =
  `first` / `true` / `last`; and by the fallback of §10 step 3.
- Synthetic `c`/`u`/`d` (`source.synthetic = true`, `source.snapshot =
  "false"`, `s:` ids): epoch-boundary full-image diffs (§5.4 rule 3, §5.5)
  and resync diffs against committed history (§10).
- Within one batch a key appears at most once per table. `DELETE FROM t`
  emits one `d` per row (no truncate op). No Kafka tombstones.

### 8.5 Identity
- `pk_json` = canonical JSON array of key values (BLOB as `{"$b64":…}`).
  `_key_json` = `pk_json`.
- `ce_id`:
  - streamed: `<epoch>:<txid>:<table>:<first 16 hex of sha256(pk_json)>`;
  - synthetic: `s:<epoch>:<txid>:<table>:<hash>`;
  - bootstrap: `r:<epoch>:<txid>:<table>:<hash>`;
  - snapshot marker (§10): `m:<epoch>:<txid>:<table>`.
  Separate namespaces keep a correction from being deduplicated away against
  an earlier event at the same `(epoch, txid)`. Unique per `ce_source`,
  deterministic.

### 8.6 Schema evolution
Schema versions per `(class, table)` are numbered 1, 2, … in order of first
sight and stored in the table property `cdcell.schema-versions` (JSON: version
→ CREATE statement, column list with Iceberg field ids, key schema). Each
event's `ce_dataschema` names the version of the cell that produced it.

| SQLite change | cdcell |
|---|---|
| ADD COLUMN | add a nullable Iceberg field; old records decode with the SQL default |
| DROP COLUMN | keep the Iceberg field, write null going forward, `schema_changes` row |
| RENAME COLUMN | Iceberg rename (same field id) when: the schema SQL changed, the column count is equal, positions and declared types match, exactly one name differs, and no data pages of the table changed in the batch. Anything else is DROP + ADD with a `schema_changes` row. |
| RENAME TABLE | same root page, new name: `schema_changes` row; events keep flowing into the existing Iceberg table; the alias is recorded in `cdcell.original-name` history. Exception: the rebuild pattern (§6) |
| type change (SQLite table rebuild) | decoded values are compared by name (§6), so only real changes are emitted; a column whose declared affinity changes gets a new field `<col>__<affinity>` |
| CREATE / DROP TABLE | `schema_changes` row; no per-row deletes on drop |
| cells of one class with different schemas (lazy migrations) | union schema: a column keeps the Iceberg type of the first affinity seen; a different affinity for the same name adds `<col>__<affinity>` (Iceberg v2 widening is only int→long, float→double, so no in-place widening). `_key_json` stays comparable; `_key` is null for cells whose key schema differs |

### 8.7 KV table (`_cdcell_kv`)
`_cf_KV` has primary key `(scope, k)`, but each DB holds exactly one cell and
`scope` is constant (a leftover of the single-DB era, `storage.rs:10-14`), so
the key is `k`: `_key` = `struct<k: string>`. A second scope value in one DB
is an error (metric; the cell is held). `before` / `after` =
`struct<value_type, value_json, value_raw>`: `value_type` is `json` / `v8` /
`stub` / `legacy_text`; `value_raw` is always set; `value_json` is tagged JSON
for V8 values: `{"$date":iso}`, `{"$bigint":"…"}`, `{"$map":[[k,v],…]}`,
`{"$set":[…]}`, `{"$bytes":b64,"$kind":"Uint8Array"}`, `{"$undefined":true}`,
`{"$ref":n}` for shared or cyclic references; null for stub rows. Values over
1 MiB stored in `kv/blobs-v2` are emitted as their reference (v1).

### 8.8 Supporting tables
- `_cdcell_progress`: one row per **touched** cell per commit
  `(_cell_id, _epoch, _txid, ce_time, committed_at, has_loss boolean)`:
  "complete up to here" (`has_loss` true if a celld loss declaration covers
  this cell epoch, §9.1).
- `_cdcell_cells`: `(_cell_id, class, script, first_seen, last_epoch, last_seen)`.
- `_cdcell_schema_changes`: `(_cell_id, table, kind, old_sql, new_sql, schema_version, _epoch, _txid)`.
- `_cdcell_losses`: one row per celld loss declaration
  `(source_key, kind, leader, epoch, declared_at, declared_by, ensemble, note, cells)`.

### 8.9 JSON CloudEvent form
Each row maps 1:1 to a structured-mode JSON CloudEvent: `ce_*` columns are
the attributes; extensions `iodebeziumop`, `iodebeziumtable`,
`iodebeziumtsms`, `iodebeziumtxid`, `iodebeziumsnapshot`, `cdcellepoch`,
`cdcelltxid`, `cdcellcell` (lowercase, ≤ 20 chars; 64-bit values as strings
because CloudEvents Integer is 32-bit, H2); `data` =
`{"before":…,"after":…}` with `_raw` values restored. Example:

```json
{"specversion":"1.0","id":"9:4182:orders:3f1c2a9b7e0d4c11",
 "source":"/cdcell/sqlite/shop/Shop/Shop:474ecadb…","type":"dev.cdcell.sqlite.DataChangeEvent",
 "subject":"orders/%5B42%5D","time":"2026-11-07T22:18:07.174Z","datacontenttype":"application/json",
 "dataschema":"urn:cdcell:schema:shop:Shop:orders:3",
 "sequence":"00000000000000000009000000000000000041820000000003",
 "partitionkey":"shop/Shop/Shop:474ecadb…","recordedtime":"2026-11-07T22:18:41.002Z",
 "iodebeziumop":"u","iodebeziumtable":"orders","iodebeziumtsms":"1794089887174",
 "iodebeziumtxid":"9:4182","iodebeziumsnapshot":"false",
 "cdcellepoch":"9","cdcelltxid":"4182","cdcellcell":"Shop:474ecadb…",
 "data":{"before":{"id":42,"customer":"ada","status":"paid","total":40.0,"updated_at":1794089880000},
         "after":{"id":42,"customer":"ada","status":"shipped","total":40.0,"updated_at":1794089887170}}}
```

Deviations from the Debezium SQLite connector, documented: `connector` is
`cdcell`; `db` is the cell id; `change_id` is the batch TXID, not per row;
`transaction.total_order` is deterministic, not commit order; no BEGIN/END
topic; no `t` op.

## 9. Notifications and broker

Filters (ObjectCreated only):

| Prefix | Suffix | Why |
|---|---|---|
| `cells/` | `.ltx` | per-cell L0/L1/L9 |
| `cells/` | `own.json` | epoch changes |
| `log/` | `.ltxb` | bundles |
| `log/` | `.json` | recovery progress hints and loss declarations |

(Prefixes include `source.prefix` when set.) Excluded by construction:
`nodes/` lease churn, `fleet/`, `deploy/`, `wake/`, `kv/`, `r2/`.
Production: S3 → SQS standard (batch 10, long polling). e2e: MinIO → NATS
JetStream with `queue_dir`.

**Acknowledgement.** A message is acknowledged when every unit from it is:
- committed (its events are in a committed batch);
- parked (§5.3);
- dropped as a phantom;
- dropped as a duplicate **whose covering unit is committed** (a duplicate
  is attached to its covering unit's batch, so a crash before that commit
  redelivers it);
- or the GET returned 404.

**Message lifetime while waiting.** Units can wait without being parked: on
a gap (T_gap), on an opener, or behind an unresolved lower epoch. While a
unit waits, cdcell extends its message's lease (SQS
`ChangeMessageVisibility`, JetStream `InProgress`) every 2 min. A unit that
has waited 10 min is **parked** (§5.3, reason `wait-bound`) and its message
acknowledged, so broker limits never decide history.

Settings: commit every 60 s; JetStream `max_ack_pending ≥ 50k`,
`ack_wait = 5 min`; SQS visibility 5 min, DLQ after 10 receives. A DLQ'd
message triggers a resync of its cells (§10), never silent loss.

Doorbell mode (acknowledge as soon as the key is journaled) is deferred
(§15); revisit above ~300 objects/s.

### 9.1 Loss declarations
celld writes `log/<node>/<gen>.e<N>.loss.json` when recovery finds no
complete witness for an active log ("acked writes within the final flush
window may be unrecovered", `node_log.rs:5242-5254`) and
`log/<node>/<gen>.bundle-<name>.loss.json` for an unreadable bundle
(`node_log.rs:6156-6178`). On such an event cdcell:
- writes a row to `_cdcell_losses`;
- marks every cell epoch with units from that session after the last
  confirmed unit: `has_loss = true` in its next `_cdcell_progress` row;
- raises a metric and an alert.
The completeness guarantee (§1) excludes exactly these declared windows.

## 10. Bootstrap and resync

One procedure for: first start on an existing fleet, an outage longer than
broker retention or bundle GC, a malformed page, a reset (§5.6), a DLQ'd
message, `c < confirmed` (§5.4 rule 2).

1. **Discover cells.** First start only: paginated LIST of `cells/` with
   delimiter.
2. **Rebuild the current image** of each cell at its current epoch:
   - base: the newest full image in the current epoch chain (an L9 `1-max`,
     an L1 from TXID 1, or the epoch's TXID-1 L0; for a paged epoch follow the
     epoch chain to its snapshot-opened ancestor, D3);
   - tail: contiguous per-cell objects above the base, then units from live
     bundles (§7 source 2, shared scan) above that;
   - image via `celld_ltx` `build_database_image`.
   Units are confirmed as usual (§5.2) before they enter the image.
3. **Emit**, depending on what cdcell already committed for the cell:
   - **No committed history:** `r` rows for every row of the current image
     (`snapshot` first…last).
   - **Committed history and a shadow checkpoint at the committed point:**
     diff the current image against it → synthetic `c`/`u`/`d`.
   - **Committed history, no usable shadow checkpoint:** rebuild the image
     **at the committed point** `(E, committed_txid)` from E's chain (celld
     never deletes L0s inside an epoch, B13, and epoch GC is off by
     default), then diff the current image against it → synthetic
     `c`/`u`/`d`. Deleted keys produce `d`.
   - **Fallback, only when E's objects are gone** (epoch GC enabled): emit
     `r` rows for the current image with `source.synthetic = true`, plus one
     **complete-snapshot marker** row per table (`op = "r"`, `_key_json =
     "[]"`, `ce_id = m:<epoch>:<txid>:<table>`, `before`/`after` null). A key
     with committed history that is absent from the newest complete snapshot
     counts as deleted (§14).
4. **Resume** streaming from the rebuilt point (confirmed = committed =
   rebuilt point).

Bundles of sealed sessions are not read (their rows are in per-cell objects
after recovery, D6).

## 11. State, commits and crash recovery

### 11.1 Shadows
- Tiers: RAM LRU (hot) → local disk page file per cell (warm) → S3 shadow
  checkpoint `_cdcell/shadows/<cell-id-hex>/<epoch>-<txid>.ltx` (an LTX
  snapshot) when a cell is evicted from the local tiers.
- **Invariant: a shadow checkpoint is never ahead of the committed point.**
  It is written from the committed page versions (§5.1), so busy cells get
  checkpoints too: after each commit, for cells evicted or idle for 10 min.
- **Restore** (process start, or a cell loaded from S3): take the newest
  shadow checkpoint at or below the committed point; if there is none, or it
  is ahead, rebuild the image at the committed point (§10 step 3, third case)
  without emitting; if it is behind, replay units (§7 sources) up to the
  committed point **without emitting**. Then confirmed := committed.
- Large cells live in the disk tier; there is no key-only mode in v1. A
  per-cell size alert fires above a configurable threshold (default 1 GB).

### 11.2 Commit protocol (every 60 s or 128 MB buffered)
Single committer per process; batches are numbered `seq = 1, 2, …`.

1. Write Parquet data files for every touched table under
   `<table location>/data/cdcell-<lease epoch>-<seq>-<n>.parquet`.
2. Create the batch manifest `_cdcell/batches/<seq:020>.json` with
   `If-None-Match: *` (a second process cannot overwrite it):
   ```json
   {"version":1, "seq":1042, "lease_epoch":7, "created":"…",
    "tables":{"cdcell.shop.shop.orders":["s3://…/data/cdcell-7-1042-0.parquet"]},
    "cells":{"Shop:474e…":{"epoch":9,"committed_txid":4182}}}
   ```
3. Commit all touched tables, each snapshot summary carrying
   `cdcell.batch=<seq>` and `cdcell.lease-epoch=<epoch>`. Use the Iceberg
   REST multi-table transaction (`POST /v1/{prefix}/transactions/commit`).
   **Verified M0 (Lakekeeper v0.15.0):** atomic — a failing requirement on one
   table (HTTP 409 `CatalogCommitConflicts`) leaves every table untouched
   (`e2e/m0/verify_iceberg.py`). Fallback, for catalogs without it: commit table by table in
   name order. In the fallback, readers can briefly see a batch in some
   tables and not others (rows of one TXID in `orders` but not yet in
   `items`); `_cdcell_progress` is committed **last**, so "complete up to
   here" is never claimed for a partially visible batch.
4. CAS (If-Match) each touched shard checkpoint
   `_cdcell/shards/<n>/checkpoint.json`:
   ```json
   {"version":1, "shard":17, "lease_epoch":7, "last_batch":1042,
    "cells":{"Shop:474e…":{"epoch":9, "committed_txid":4182, "parked":false,
                           "shadow":"_cdcell/shadows/…/9-4182.ltx",
                           "seen":{"…key…":"etag"}}}}
   ```
   Confirmed points are not persisted: on load, confirmed := committed.
   Size is bounded by cells per shard; raise S for large fleets.
5. Acknowledge the covered messages; write shadow checkpoints (§11.1).

**Idle shards.** Every shard checkpoint is rewritten (with its current
`last_batch`) at least every 6 h, so the global floor `L` below advances.

**Restart.** Take the process lease (§3). Read every shard checkpoint; let
`L` = the lowest `last_batch`. LIST `_cdcell/batches/` from `L+1`. For each
manifest in order: for each table, check whether a snapshot with
`cdcell.batch=<seq>` exists; commit the missing tables from the manifest's
data files (idempotent); then advance the shard checkpoints for the
manifest's cells. LIST `_cdcell/parked/` and re-park. Resume consuming;
redelivered messages are dropped by the committed points. Snapshot expiry
(§11.3) keeps snapshots for at least 24 h, beyond the restart window.

**Orphan data files.** Parquet files written before a manifest that was never
created (crash between step 1 and 2) are not referenced by any snapshot.
Daily maintenance deletes `cdcell-*` data files older than 24 h that no
snapshot or manifest references (Iceberg `remove_orphan_files` semantics).

### 11.3 Table maintenance (cdcell's own tables)
Daily: expire snapshots older than 7 days, rewrite manifests, compact small
files to ~256 MB, remove orphan files (§11.2);
`write.metadata.delete-after-commit.enabled=true`. Can be disabled for
customers who run their own maintenance. Batch manifests below every shard's
`last_batch` are deleted.

## 12. Operations

**Metrics:** watermark age per cell (p99, max); open gaps and gap age;
waiting units and wait age; parked cells and age; phantoms dropped;
duplicates dropped; 404s by cause; loss declarations; decoder errors and
resyncs; class→script map errors; commit latency, retries and conflicts;
shadow cache hit rate, rebuild count and bytes; broker depth and oldest
message age; DLQ size; process lease renewals.

**Alerts:** gap older than T_gap; parked cell older than 30 min; lag p99
over 5 min; resync rate; DLQ non-empty; any loss declaration; process lease
lost.

**IAM, celld bucket (read):** `s3:GetObject` on `cells/*/ltx/*`,
`cells/*/own.json`, `log/*`, `nodes/*`, `deploy/current.json`,
`deploy/*/current.json`, `deploy/*/*/manifest.json`; `s3:ListBucket`
restricted by prefix to `cells/` and `log/` (the latter also with delimiter,
to enumerate sessions, §7); explicit deny on `fleet/*` (peer HMAC secret),
`r2/*`, other `deploy/*` keys (Worker code and assets), `kv/*`; `kms:Decrypt`
if SSE-KMS. This role can read every Durable Object's data: treat it as
fleet-wide data access.
**IAM, customer bucket:** warehouse prefix via Lakekeeper-vended
credentials; `_cdcell/` read/write for state.

## 13. Cost model (AWS list prices, us-east-1; re-check)

Variable, per 1M celld objects: SQS ~$0.48 (send + batched receive/delete) +
GET ~$0.40 + confirmation reads ~$0.04 + Iceberg ~$0.01 ≈ **$0.93**, versus
celld's own ~$5.86 for the same objects (~16%). Fleet mode: bundles are one
object per node per 2 s regardless of write rate, so ~10% at 3 nodes.

Fixed floor: worker compute (one small instance), Lakekeeper + Postgres
(~$15-40/month, or shared), broker baseline. Publish cost as "variable per
1M objects + fixed floor"; at low volume the floor dominates. Measured in M6
against bucket request counters.

## 14. Consumer recipes

**SCD type 1** (ClickHouse 26.9 + Lakekeeper REST catalog, verified M0; send `allow_database_iceberg=1` as a setting — the HTTP interface takes one statement per request):
```sql
SET allow_database_iceberg = 1;
CREATE DATABASE lake ENGINE = DataLakeCatalog('http://lakekeeper:8181/catalog')
  SETTINGS catalog_type = 'rest', warehouse = 'cdc', vended_credentials = true;

SELECT _cell_id, _key_json, argMax(after, (_epoch, _txid)) AS row
FROM (SELECT * FROM lake.`cdcell.shop.shop.orders` LIMIT 1 BY ce_source, ce_id)
WHERE _key_json != '[]'                       -- skip snapshot markers
GROUP BY _cell_id, _key_json
HAVING argMax(op, (_epoch, _txid)) != 'd';
```
If a cell has a complete-snapshot marker (§10 fallback), keys of that cell
and table whose latest event is older than the newest marker are deleted:
add `AND (_epoch, _txid) >= <newest marker for the cell>` to the outer query.

**SCD type 2:** partition by `(_cell_id, _key_json)`, order by
`(_epoch, _txid)`; first collapse consecutive identical `after` images
(bootstrap `r` and synthetic no-ops), then `valid_from = ce_time`,
`valid_to = lead(ce_time)`; a `d` closes the last version. Order by
`(_epoch, _txid)`, never by `ce_time` (clock skew).

**ReplacingMergeTree** materialization: version
`bitShiftLeft(toUInt128(_epoch), 64) + _txid` (verified M0 on ClickHouse
26.9; a `2^64` literal parses as Float64 and fails),
`is_deleted = op = 'd'`.

**Finding a Durable Object by name.** `idFromName` is a one-way HMAC; celld
stores only the id. cdcell ships `cdcell id-from-name <script> <Class> <name>`
and a matching ClickHouse function. Construction (`engine_api.rs:433-441,
478-491, 548`):
- `K = SHA256("cells:v1:" ‖ len(script) ‖ ":" ‖ script ‖ ":" ‖ Class)`;
- `h1 = HMAC-SHA256(K, name)`, `id[0..16] = h1[0..16]`;
- `h2 = HMAC-SHA256(K, id[0..16])`, `id[16..32] = h2[0..16]`;
- cell id = `Class + ":" + lowercase hex(id)`.
Shared classes (D1, KV, Queue) use fixed keys (`engine_api.rs:507-520`) and
are deferred with those cell types. Objects created with `newUniqueId()` have
no name: recommend storing a natural key in a one-row `meta` table.

## 15. v1 scope

**In:** AWS S3 and MinIO sources; NATS JetStream (e2e) and SQS (prod); one
process (intake + in-process shards + committer) with a global process
lease; bundles and per-cell L0; L1/L9 for gap repair, rule 3 and bootstrap;
confirmation, parking and resolution (rules 0-4, predecessor proof); snapshot
and paged epochs; full decoder (rowid, WITHOUT ROWID, overflow map, schema
cookie, value comparison by name, rebuild redirect); user tables and KV;
shadow tiers with committed page versions and S3 shadow checkpoints;
bootstrap/resync including live bundles and rebuild at the committed point;
loss declarations; Iceberg via Lakekeeper with the frozen schema and
catalog-backed registries; own-table maintenance incl. orphan files;
metrics; `id-from-name`; class→script map from deploy manifests.

**Deferred:** GCS and Azure; multi-process shard leases; doorbell ack mode;
mirror (type-1) mode; facets; `_cf_ALARM`; D1 / KV-namespace / Queue /
Workflow cells (D1 is next: plain user tables); KV blob fetch; key-only
shadows; a JSON CloudEvents export stream; multi-tenancy.

## 16. Milestones

| # | Deliverable | Gate (e2e) |
|---|---|---|
| M0 | Stack: Lakekeeper + Postgres, `minio-cdc`, ClickHouse; verify multi-table commits, DataLakeCatalog with Lakekeeper, UInt128 RMT version | ClickHouse reads a hand-written Iceberg table through Lakekeeper |
| M1 | Decoder library: LTX + bundle parsing, page → row, overflow map, schema cookie, type mapping, KV decode | Property tests on seeded SQLite workloads + captured `shop` LTX match a full decode exactly, incl. `retouch` |
| M2 | Single-node streaming end to end: notifications → GET → confirm → shadow → events → Iceberg, commit protocol, checkpoints, process lease | Single celld node + load: ClickHouse type-1 view equals every shop's `?op=dump` |
| M3 | Fleet: bundles, lease confirmation, routing by cell, live-bundle gap source, duplicates | 3-node stack + load: type-1 = dump; no duplicate `(ce_source, ce_id)` |
| M4 | Epochs and phantoms: openers, predecessor proof, full-image diff, paged marker, parking, resolution rules 0-4 | `zombie-test.sh pause\|stall\|zombie`, `docker kill`, small `CELLD_LTX_PAGED_MIN_MB`; each ends with type-1 = dump and zero rule violations |
| M5 | Robustness: gap repair, tail reconciliation, bootstrap/resync incl. rebuild at the committed point, cdcell crash mid-commit, two processes started at once, outage past bundle GC | Kill cdcell mid-commit; start a second cdcell; drop 5% of notifications; stop cdcell 10 min under load; restart: type-1 = dump, no duplicates |
| M6 | Operations: metrics, maintenance, IAM policy, loss declarations, cost check | 1-hour soak at 10 objects/s: lag p99 < 2 min; request counts within ±20% of §13 |

## 17. Open questions

1. ~~Lakekeeper multi-table transactions; ClickHouse `DataLakeCatalog`
   against Lakekeeper~~ — **resolved M0**: both work (§11.2, §14).
2. ~~ClickHouse `ReplacingMergeTree` with a UInt128 version~~ — **resolved
   M0**: works with `bitShiftLeft(toUInt128(_epoch), 64) + _txid` (§14).
3. How often resolution needs rule 3 vs rules 0-2 in practice (paged epochs
   need ≥ 256 MB chains) — M4.
4. Turn-level implicit transactions in celld beyond microtask batching
   (affects batch granularity only) — invariants OQ 6.
5. Fidelity of the chosen pure-Rust V8 deserializer on celld's values
   (version byte forced to 15) — M1.
6. MinIO notification key encoding and loss behaviour (dev only) — M2.
7. Phantom rows that reached no follower: reproduce and confirm they are
   dropped by rules 2/3 — invariants OQ 10, M4.
8. Loss declaration contents: which cells a session-level loss affects is
   inferred from units after the last confirmed point; celld's record names
   only the session and epoch — M4.
