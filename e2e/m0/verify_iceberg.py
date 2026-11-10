"""M0 verification against Lakekeeper (run inside the e2e docker network).

1. nested namespace cdcell.shop.shop + a change table shaped like design §8.2
   (subset), partitioned by day(ce_time), bucket(16, _cell_id)
2. append sample change events for two cells: inserts, updates, a delete,
   and one duplicate event (same ce_source + ce_id)
3. multi-table transaction (POST /v1/{prefix}/transactions/commit):
   a) both tables updated atomically
   b) a failing requirement on the second table leaves the first untouched
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.types import (DoubleType, LongType, NestedField, StringType,
                             StructType, TimestamptzType)

URI = "http://lakekeeper:8181/catalog"
cat = load_catalog("lk", type="rest", uri=URI, warehouse="cdc")
NS = ("m0test", "shop", "shop")
for i in range(1, len(NS) + 1):
    try:
        cat.create_namespace(NS[:i])
    except Exception:  # noqa: BLE001 - exists
        pass

row_t = StructType(
    NestedField(101, "id", LongType()), NestedField(102, "customer", StringType()),
    NestedField(103, "status", StringType()), NestedField(104, "total", DoubleType()))
schema = Schema(
    NestedField(1, "ce_id", StringType(), required=True),
    NestedField(2, "ce_source", StringType(), required=True),
    NestedField(3, "ce_time", TimestamptzType(), required=True),
    NestedField(4, "_cell_id", StringType(), required=True),
    NestedField(5, "_epoch", LongType(), required=True),
    NestedField(6, "_txid", LongType(), required=True),
    NestedField(7, "_key_json", StringType(), required=True),
    NestedField(8, "op", StringType(), required=True),
    NestedField(9, "before", StructType(*[NestedField(f.field_id + 100, f.name, f.field_type) for f in row_t.fields])),
    NestedField(10, "after", row_t),
)
spec = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="ce_time_day"),
    PartitionField(source_id=4, field_id=1001, transform=BucketTransform(16), name="cell_bucket"))

for name in ("orders", "items"):
    try:
        cat.drop_table(NS + (name,))
    except Exception:  # noqa: BLE001 - first run
        pass
orders = cat.create_table(NS + ("orders",), schema=schema, partition_spec=spec,
                          properties={"format-version": "2"})
items = cat.create_table(NS + ("items",), schema=schema, partition_spec=spec,
                         properties={"format-version": "2"})

A, B = "Shop:aaaa", "Shop:bbbb"
t = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
def r(id_, cust, status, total):
    return {"id": id_, "customer": cust, "status": status, "total": total}
events = [  # cell, epoch, txid, key, op, before, after
    (A, 1, 1, 1, "c", None, r(1, "ada", "new", 10.0)),
    (A, 1, 2, 1, "u", r(1, "ada", "new", 10.0), r(1, "ada", "paid", 10.0)),
    (A, 1, 2, 2, "c", None, r(2, "bo", "new", 5.0)),
    (A, 2, 1, 2, "d", r(2, "bo", "new", 5.0), None),          # new epoch, delete
    (B, 1, 1, 1, "c", None, r(1, "cy", "new", 7.0)),           # same pk, other cell
    (B, 1, 3, 1, "u", r(1, "cy", "new", 7.0), r(1, "cy", "shipped", 7.0)),
]
rows = []
for cell, ep, tx, key, op, before, after in events:
    rows.append({"ce_id": f"{ep}:{tx}:orders:{key}", "ce_source": f"/cdcell/sqlite/shop/Shop/{cell}",
                 "ce_time": t, "_cell_id": cell, "_epoch": ep, "_txid": tx,
                 "_key_json": json.dumps([key]), "op": op, "before": before, "after": after})
rows.append(dict(rows[1]))  # duplicate event: same ce_source + ce_id
orders.append(pa.Table.from_pylist(rows, schema=orders.schema().as_arrow()))
print(f"orders: appended {len(rows)} rows, snapshot {orders.current_snapshot().snapshot_id}")

# ---- multi-table transaction ------------------------------------------------
prefix = json.load(urllib.request.urlopen(f"{URI}/v1/config?warehouse=cdc"))["defaults"]["prefix"]
def table_uuid(tbl):
    return str(cat.load_table(tbl).metadata.table_uuid)
def props(tbl):
    return cat.load_table(tbl).properties
def commit(changes):
    req = urllib.request.Request(f"{URI}/v1/{prefix}/transactions/commit", method="POST",
                                 data=json.dumps({"table-changes": changes}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        return urllib.request.urlopen(req).status
    except urllib.error.HTTPError as e:
        return f"{e.code} {e.read().decode()[:160]}"
def change(name, uuid, batch):
    return {"identifier": {"namespace": list(NS), "name": name},
            "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
            "updates": [{"action": "set-properties", "updates": {"cdcell.batch": batch}}]}

o_uuid, i_uuid = table_uuid(NS + ("orders",)), table_uuid(NS + ("items",))
st = commit([change("orders", o_uuid, "1"), change("items", i_uuid, "1")])
ok_a = props(NS + ("orders",)).get("cdcell.batch") == "1" and props(NS + ("items",)).get("cdcell.batch") == "1"
print(f"multi-table commit (both valid): status={st} both_updated={ok_a}")

bogus = "00000000-0000-0000-0000-000000000000"
st = commit([change("orders", o_uuid, "2"), change("items", bogus, "2")])
untouched = props(NS + ("orders",)).get("cdcell.batch") == "1"
print(f"multi-table commit (second requirement fails): status={st} first_table_untouched={untouched}")
print("RESULT", "PASS" if ok_a and untouched else "FAIL")
