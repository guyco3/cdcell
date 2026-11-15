# Using cdcell's change log

cdcell writes one **append-only change log** per Durable Object class and
SQLite table into your object storage, as Apache Iceberg (format v2, Parquet
data files) registered in your Iceberg REST catalog. You turn it into current
state (SCD type 1) or history (SCD type 2) with any Iceberg engine.

## Where the data is

```
catalog namespace   cdcell.<script>.<class>
table               <sqlite table>           e.g. cdcell.shop.shop.orders
                    _cdcell_kv               the Durable Object KV API (ctx.storage.put/get)
cdcell.<script>._cdcell_progress             per cell: committed up to (epoch, txid), when
```

Partitioning: `day(__ts)`, `bucket(16, __cell_id)`.

## Columns

| Column | Meaning |
|---|---|
| `__cell_id` | which Durable Object (`Class:<64 hex>`) |
| `__epoch`, `__txid` | **the order of changes within a Durable Object; compare as the pair** |
| `__op` | `c` insert · `u` update · `d` delete · `r` initial state |
| `__key` | the row's primary key as a JSON array (`[42]`, `["a",1]`) |
| `__ts` | when the change was captured (wall clock: for display, not ordering) |
| `__schema_version` | which shape of the table produced the row |
| `__ingested_at` | when cdcell wrote the row |
| *your columns* | the row **after** the change; on `d` only the key columns |
| `__raw` | values that did not fit their column type, as JSON |

`_cdcell_kv` has `k`, `value_type`, `value_json` (JSON; dates, BigInts, Maps,
Sets and binary are tagged: `{"$date": …}`, `{"$bigint": "…"}`, `{"$map": […]}`, …),
`value_raw`.

## Rules that keep results correct

1. **Order by `(__epoch, __txid)` only.** Not by `__ts`, `__ingested_at`, file
   order, or Iceberg snapshot sequence numbers: those record when cdcell
   committed, not when your Durable Object wrote.
2. **Key = `(__cell_id, __key)`.** Primary keys repeat across Durable Objects
   (every shop has an order 1).
3. **Deletes are rows** with `__op = 'd'`. A key whose latest row is a delete
   does not exist. `DROP TABLE` emits a delete for every row of the table, and
   `ALTER TABLE ... RENAME TO` a delete under the old name plus a create
   under the new one, so per-table current state stays exact.
4. **Duplicates can appear after a cdcell crash.** They have the same
   `(__cell_id, __key, __epoch, __txid)`; merge with a version guard (below)
   or deduplicate on those four columns.
5. **History is per transaction while cdcell keeps up.** If cdcell falls
   behind celld's compaction window (an outage, a backlog), it emits one net
   change (updates plus deletes) per compacted range. Current state stays
   exact; versions inside that range are missing from type 2. Operators can
   widen the window with celld's `CELLD_LTX_COMPACTION_MIN_TXIDS` /
   `_MIN_MB` (design §7, operating requirements).
6. **NULLs are values.** A row re-created with NULL columns must not pick up
   older non-NULL values. In ClickHouse, `argMax(col, v)` skips NULLs: use
   `argMax(tuple(col), v).1` (or `argMax(tuple(*), v)`).
7. **Schemas differ between Durable Objects of one class** until each runs
   its migration (celld migrates lazily). New columns appear in the table for
   everyone; rows from objects that have not migrated have them NULL.
   `__schema_version` tells you which shape a row came from.

## Current state (type 1)

**Merge-on-read view, ClickHouse:**
```sql
SET allow_database_iceberg = 1;
CREATE DATABASE lake ENGINE = DataLakeCatalog('http://<catalog>/catalog')
  SETTINGS catalog_type = 'rest', warehouse = '<warehouse>', vended_credentials = true;

SELECT __cell_id, __key,
       argMax(tuple(id, customer, status, total), (__epoch, __txid)) AS row
FROM lake.`cdcell.shop.shop.orders`
GROUP BY __cell_id, __key
HAVING argMax(__op, (__epoch, __txid)) != 'd';
```

**Trino:**
```sql
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY __cell_id, __key ORDER BY __epoch DESC, __txid DESC) AS rn
  FROM iceberg.cdcell_shop_shop.orders)          -- or "cdcell.shop.shop"."orders" depending on catalog config
WHERE rn = 1 AND __op <> 'd';
```

**Copy-on-write / merge-on-read table, Spark or Trino `MERGE`:** keep a
current-state table and merge each new slice of the log into it. The version
guard makes the merge safe to re-run and immune to duplicates:
```sql
MERGE INTO lake.orders_current t
USING (SELECT * FROM (
         SELECT *, row_number() OVER (PARTITION BY __cell_id, __key ORDER BY __epoch DESC, __txid DESC) AS rn
         FROM cdcell.shop.shop.orders WHERE __ingested_at > :last_run)
       WHERE rn = 1) s
ON t.__cell_id = s.__cell_id AND t.__key = s.__key
WHEN MATCHED AND (s.__epoch > t.__epoch OR (s.__epoch = t.__epoch AND s.__txid > t.__txid)) AND s.__op = 'd' THEN DELETE
WHEN MATCHED AND (s.__epoch > t.__epoch OR (s.__epoch = t.__epoch AND s.__txid > t.__txid)) THEN UPDATE SET *
WHEN NOT MATCHED AND s.__op <> 'd' THEN INSERT *;
```
Whether that table applies deletes and updates as **merge-on-read** (delete
files / deletion vectors) or **copy-on-write** (rewritten data files) is a
property of *your* table: `write.delete.mode`, `write.update.mode`,
`write.merge.mode` = `merge-on-read` | `copy-on-write`. cdcell's change log
itself never needs delete files.

**ClickHouse ReplacingMergeTree** (materialized copy):
```sql
CREATE TABLE orders_current (cell String, key String, id Int64, customer String, status String,
                             ver UInt128, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY (cell, key);
INSERT INTO orders_current
SELECT __cell_id, __key, id, customer, status,
       bitShiftLeft(toUInt128(__epoch), 64) + __txid, __op = 'd'
FROM lake.`cdcell.shop.shop.orders`;
SELECT * FROM orders_current FINAL WHERE NOT is_deleted;
```

## History (type 2)

Every row is a version. Validity runs until the next version of the same
key; a delete ends it:
```sql
SELECT __cell_id, __key, __op, id, customer, status,
       __ts AS valid_from,
       lead(__ts) OVER w AS valid_to,            -- NULL = current
       __epoch, __txid
FROM cdcell.shop.shop.orders
WINDOW w AS (PARTITION BY __cell_id, __key ORDER BY __epoch, __txid)
-- keep __op <> 'd' rows as versions; a 'd' row only closes the previous one
```
Rows with `__op = 'r'` (initial state) or repeated identical images (after a
crash re-emission) can be collapsed by comparing each row to the previous one
in the window.

## Finding a Durable Object by name

`__cell_id` is derived from the name the Worker passed to `idFromName`:
key = SHA-256(`"cells:v1:" + len(script) + ":" + script + ":" + Class`),
`a` = HMAC-SHA256(key, name)[0:16], `b` = HMAC-SHA256(key, a)[0:16],
`__cell_id` = `Class + ":" + hex(a ‖ b)`. Objects created with `newUniqueId()`
have no name; store a natural key in a table (e.g. a one-row `meta`) and join
on it.

## Freshness

`_cdcell_progress` tells you how far each Durable Object is committed. Lag is
typically seconds to a couple of minutes; cdcell exposes it as
`cdcell_watermark_age_seconds_p99`.
