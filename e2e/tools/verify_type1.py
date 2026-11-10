#!/usr/bin/env python3
"""Gate check: ClickHouse SCD type-1 views of cdcell's change log == each
shop's real state (design rev 5 contract).

  verify_type1.py --shops shop-01 shop-02 ...      live dumps through the LB
  verify_type1.py --fixture DIR                    a captured dump.json

Change-log contract: one append-only table per (script, class, table) with
meta columns __cell_id, __epoch, __txid, __op (c|u|d|r), __key, __ts,
__schema_version, __ingested_at, then the table's own columns (after values;
on deletes only the key). Order by (__epoch, __txid). Type 1 = latest row per
(__cell_id, __key) unless it is a delete.

Also checked: no duplicate (__cell_id, __key, __epoch, __txid) (pass
--allow-dups after crash tests), and optional columns from schema migrations
(orders.note, tags) when the dump has them.
"""
import argparse
import hashlib
import hmac
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

CH = "http://127.0.0.1:8123/"
LB = "http://127.0.0.1:8180"
NS = "cdcell.shop.shop"
ALLOW_DUPS = False


def id_from_name(script, cls, name):
    key = hashlib.sha256(f"cells:v1:{len(script)}:{script}:{cls}".encode()).digest()
    a = hmac.new(key, name.encode(), hashlib.sha256).digest()[:16]
    b = hmac.new(key, a, hashlib.sha256).digest()[:16]
    return f"{cls}:{(a + b).hex()}"


def ch(sql):
    req = urllib.request.Request(CH + "?allow_database_iceberg=1&default_format=JSONEachRow", data=sql.encode())
    req.add_header("Authorization", "Basic ZGVmYXVsdDpjbGlja2hvdXNl")  # default:clickhouse
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return [json.loads(l) for l in r.read().decode().splitlines() if l]
    except urllib.error.HTTPError as e:
        print(f"FAIL query: {e.read().decode()[:200]}")
        sys.exit(1)


def ensure_db():
    ch("DROP DATABASE IF EXISTS lake")
    ch("CREATE DATABASE lake ENGINE = DataLakeCatalog('http://lakekeeper:8181/catalog') "
       "SETTINGS catalog_type = 'rest', warehouse = 'cdc', vended_credentials = true")


def tables():
    return {r["name"] for r in ch("SHOW TABLES FROM lake")}


def type1(table, cell, cols):
    """Latest row per key for one cell, deletes removed (merge-on-read view)."""
    # argMax skips NULLs; wrapping in a tuple keeps a NULL latest value (a
    # re-created row with NULL columns must not resurrect an older value).
    sel = ", ".join(f"argMax(tuple({c}), (__epoch, __txid)).1 AS {a}" for a, c in cols.items())
    return ch(f"""SELECT {sel} FROM lake.`{NS}.{table}` WHERE __cell_id = '{cell}'
                 GROUP BY __key HAVING argMax(__op, (__epoch, __txid)) != 'd'""")


def close(a, b):
    return abs(float(a) - float(b)) < 1e-9


def check_shop(name, dump):
    cell = id_from_name("shop", "Shop", name)
    have = tables()
    problems = []
    with_note = any("note" in o for o in dump["orders"])
    cols = {"id": "id", "customer": "customer", "status": "status", "total": "total", "updated_at": "updated_at"}
    if with_note:
        cols["note"] = "note"
    got = {r["id"]: r for r in type1("orders", cell, cols)}
    want = {o["id"]: o for o in dump["orders"]}
    if set(got) != set(want):
        problems.append(f"orders keys: missing {sorted(set(want) - set(got))[:5]} extra {sorted(set(got) - set(want))[:5]}")
    for k in set(got) & set(want):
        g, w = got[k], want[k]
        if (g["customer"], g["status"], int(g["updated_at"])) != (w["customer"], w["status"], w["updated_at"]) or not close(g["total"], w["total"]) \
                or (with_note and g.get("note") != w.get("note")):
            problems.append(f"orders {k}: got {g} want {w}")
    got = {(r["order_id"], r["sku"]): int(r["qty"]) for r in type1("items", cell, {"order_id": "order_id", "sku": "sku", "qty": "qty"})}
    want = {(i["order_id"], i["sku"]): i["qty"] for i in dump["items"]}
    if got != want:
        problems.append(f"items differ: {len(got)} vs {len(want)}")
    if "tags" in dump:
        got = {(r["order_id"], r["tag"]) for r in type1("tags", cell, {"order_id": "order_id", "tag": "tag"})} if f"{NS}.tags" in have else set()
        want = {(t["order_id"], t["tag"]) for t in dump["tags"]}
        if got != want:
            problems.append(f"tags differ: {len(got)} vs {len(want)}")
    got = {r["id"]: (int(r["bytes"]), r["tail"]) for r in type1("attachments", cell, {
        "id": "id", "bytes": "length(data)", "tail": "hex(substring(data, -4))"})}
    want = {a["id"]: (a["bytes"], a["tail"]) for a in dump["attachments"]}
    if got != want:
        bad = [k for k in set(got) | set(want) if got.get(k) != want.get(k)]
        problems.append(f"attachments differ at {bad[:5]}")
    got = {r["k"]: json.loads(r["v"]) for r in type1("_cdcell_kv", cell, {"k": "k", "v": "value_json"}) if r["k"].startswith("pref:")} \
        if f"{NS}._cdcell_kv" in have else {}
    if got != dump["kv"]:
        bad = sorted(k for k in set(got) | set(dump["kv"]) if got.get(k) != dump["kv"].get(k))
        problems.append(f"kv differs at {bad[:5]}")
    for t in ("orders", "items", "attachments", "_cdcell_kv", "tags"):
        if f"{NS}.{t}" not in have:
            continue
        d = ch(f"SELECT count() - uniqExact(__cell_id, __key, __epoch, __txid) AS d FROM lake.`{NS}.{t}` WHERE __cell_id = '{cell}'")[0]["d"]
        if int(d) and not ALLOW_DUPS:
            problems.append(f"{t}: {d} duplicate change rows")
    counts = f"orders={len(dump['orders'])} items={len(dump['items'])} attachments={len(dump['attachments'])} kv={len(dump['kv'])}"
    print(f"{'PASS' if not problems else 'FAIL'} {name} ({cell[:13]}…) {counts}")
    for p in problems:
        print("   ", p)
    return not problems


def schema_versions(name, table="orders"):
    """Distinct __schema_version values for one cell's rows of a table."""
    cell = id_from_name("shop", "Shop", name)
    return sorted(r["v"] for r in ch(f"SELECT DISTINCT __schema_version AS v FROM lake.`{NS}.{table}` WHERE __cell_id = '{cell}'"))


def main():
    global ALLOW_DUPS
    ap = argparse.ArgumentParser()
    ap.add_argument("--shops", nargs="*", default=[])
    ap.add_argument("--fixture")
    ap.add_argument("--allow-dups", action="store_true", help="after crashes, duplicates are allowed (dedupe by key+version)")
    ap.add_argument("--schema-versions", nargs="*", default=[], help="print __schema_version sets for these shops")
    a = ap.parse_args()
    ALLOW_DUPS = a.allow_dups
    ensure_db()
    ok = True
    if a.fixture:
        meta = json.load(open(f"{a.fixture}/dump.json"))
        ok &= check_shop(meta["name"], meta["dump"])
    for s in a.shops:
        dump = json.load(urllib.request.urlopen(f"{LB}/?shop={urllib.parse.quote(s)}&op=dump", timeout=30))
        ok &= check_shop(s, dump)
    for s in a.schema_versions:
        print(f"SCHEMA {s} {schema_versions(s)}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
