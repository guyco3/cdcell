#!/usr/bin/env python3
"""Gate check: ClickHouse SCD type-1 views == each shop's real state.

  verify_type1.py --shops shop-01 shop-02 ...      live dumps through the LB
  verify_type1.py --fixture DIR                    a captured dump.json

For each shop: cell id from the name (celld idFromName), type-1 view per
table filtered to that cell (latest (_epoch,_txid) per key, deletes removed,
duplicates removed by (ce_source, ce_id)), compared field by field.
Exit status 1 on any mismatch.
"""
import argparse
import hashlib
import hmac
import json
import sys
import urllib.parse
import urllib.request

CH = "http://127.0.0.1:8123/"
LB = "http://127.0.0.1:8180"
NS = "cdcell.shop.shop"


def id_from_name(script, cls, name):
    key = hashlib.sha256(f"cells:v1:{len(script)}:{script}:{cls}".encode()).digest()
    a = hmac.new(key, name.encode(), hashlib.sha256).digest()[:16]
    b = hmac.new(key, a, hashlib.sha256).digest()[:16]
    return f"{cls}:{(a + b).hex()}"


def ch(sql):
    req = urllib.request.Request(CH + "?allow_database_iceberg=1&default_format=JSONEachRow", data=sql.encode())
    req.add_header("Authorization", "Basic ZGVmYXVsdDpjbGlja2hvdXNl")  # default:clickhouse
    with urllib.request.urlopen(req, timeout=60) as r:
        return [json.loads(l) for l in r.read().decode().splitlines() if l]


def ensure_db():
    ch("DROP DATABASE IF EXISTS lake")
    ch("CREATE DATABASE lake ENGINE = DataLakeCatalog('http://lakekeeper:8181/catalog') "
       "SETTINGS catalog_type = 'rest', warehouse = 'cdc', vended_credentials = true")


def type1(table, cell, exprs):
    cols = ", ".join(f"argMax({e}, (_epoch, _txid)) AS {a}" for a, e in exprs.items())
    return ch(f"""SELECT {cols} FROM (SELECT * FROM lake.`{NS}.{table}` WHERE _cell_id = '{cell}'
                LIMIT 1 BY ce_source, ce_id) GROUP BY _key_json HAVING argMax(op, (_epoch, _txid)) != 'd'""")


def close(a, b):
    return abs(float(a) - float(b)) < 1e-9


def check_shop(name, dump):
    cell = id_from_name("shop", "Shop", name)
    problems = []
    got = {r["id"]: r for r in type1("orders", cell, {"id": "after.id", "customer": "after.customer", "status": "after.status",
                                                      "total": "after.total", "updated_at": "after.updated_at"})}
    want = {o["id"]: o for o in dump["orders"]}
    if set(got) != set(want):
        problems.append(f"orders keys: missing {sorted(set(want) - set(got))[:5]} extra {sorted(set(got) - set(want))[:5]}")
    for k in set(got) & set(want):
        g, w = got[k], want[k]
        if g["customer"] != w["customer"] or g["status"] != w["status"] or not close(g["total"], w["total"]) or int(g["updated_at"]) != w["updated_at"]:
            problems.append(f"orders {k}: got {g} want {w}")
    got = {(r["order_id"], r["sku"]): r["qty"] for r in type1("items", cell, {"order_id": "after.order_id", "sku": "after.sku", "qty": "after.qty"})}
    want = {(i["order_id"], i["sku"]): i["qty"] for i in dump["items"]}
    if {k: int(v) for k, v in got.items()} != want:
        problems.append(f"items differ: {len(got)} vs {len(want)}")
    got = {r["id"]: (int(r["bytes"]), r["tail"]) for r in type1("attachments", cell, {
        "id": "after.id", "bytes": "length(after.data)", "tail": "hex(substring(after.data, -4))"})}
    want = {a["id"]: (a["bytes"], a["tail"]) for a in dump["attachments"]}
    if got != want:
        bad = [k for k in set(got) | set(want) if got.get(k) != want.get(k)]
        problems.append(f"attachments differ at {bad[:5]}")
    got = {r["k"] for r in type1("_cf_kv", cell, {"k": "after.k"}) if r["k"].startswith("pref:")}
    if got != set(dump["kv"]):
        problems.append(f"kv keys differ: {sorted(got ^ set(dump['kv']))[:5]}")
    counts = f"orders={len(dump['orders'])} items={len(dump['items'])} attachments={len(dump['attachments'])} kv={len(dump['kv'])}"
    print(f"{'PASS' if not problems else 'FAIL'} {name} ({cell[:13]}…) {counts}")
    for p in problems:
        print("   ", p)
    return not problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shops", nargs="*", default=[])
    ap.add_argument("--fixture")
    a = ap.parse_args()
    ensure_db()
    ok = True
    if a.fixture:
        meta = json.load(open(f"{a.fixture}/dump.json"))
        ok &= check_shop(meta["name"], meta["dump"])
    for s in a.shops:
        dump = json.load(urllib.request.urlopen(f"{LB}/?shop={urllib.parse.quote(s)}&op=dump", timeout=30))
        ok &= check_shop(s, dump)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
