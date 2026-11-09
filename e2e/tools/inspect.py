#!/usr/bin/env python3
"""Inspect what a celld fleet has written to its bucket.

Read-only. Talks to the bucket through the `mc` CLI (alias given by --alias,
default `e2e`), so it needs no Python dependencies.

  inspect.py leases            node leases and their node-log state
  inspect.py bundles [-n N]    fleet bundles: writer, size, rows per cell
  inspect.py cells             per cell: owner, epoch, LTX objects per epoch/level
  inspect.py watch -o FILE     poll leases + new bundles, append JSONL with timestamps

Formats (celld crates/ltx/src/bundle.rs, ltx.rs):
  bundle  = concatenated raw L0 LTX files | footer rows | u32 LE footer len | "CLB1"
  row     = u16 LE cell_len | cell utf-8 | u64 LE cell_epoch | txid | offset | len
  LTX hdr = big-endian; magic "LTX1", flags, page_size, commit, min_txid, max_txid, timestamp ms
"""
import argparse
import json
import struct
import subprocess
import sys
import time
from collections import defaultdict
from urllib.parse import unquote


def mc(alias, *args, binary=False):
    out = subprocess.run(["mc", *args], capture_output=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.decode().strip() or f"mc {' '.join(args)} failed")
    return out.stdout if binary else out.stdout.decode()


def ls(alias, bucket, prefix):
    """All objects under a prefix: list of (key, size, last_modified)."""
    try:
        raw = mc(alias, "ls", "--recursive", "--json", f"{alias}/{bucket}/{prefix}")
    except RuntimeError:
        return []
    items = []
    for line in raw.splitlines():
        o = json.loads(line)
        if o.get("type") == "file":
            items.append((prefix + o["key"], o["size"], o["lastModified"]))
    return items


def cat(alias, bucket, key):
    return mc(alias, "cat", f"{alias}/{bucket}/{key}", binary=True)


def ltx_header(b):
    if b[:4] != b"LTX1":
        raise ValueError("not an LTX file")
    flags, page_size, commit, min_tx, max_tx, ts = struct.unpack(">IIIQQQ", b[4:40])
    return {"flags": flags, "page_size": page_size, "commit": commit,
            "min_txid": min_tx, "max_txid": max_tx, "timestamp_ms": ts}


def bundle_rows(b):
    if b[-4:] != b"CLB1":
        raise ValueError("bad bundle magic")
    (flen,) = struct.unpack("<I", b[-8:-4])
    footer = b[-8 - flen:-8]
    rows, i = [], 0
    while i < len(footer):
        (clen,) = struct.unpack_from("<H", footer, i)
        i += 2
        cell = footer[i:i + clen].decode()
        i += clen
        epoch, txid, off, ln = struct.unpack_from("<QQQQ", footer, i)
        i += 32
        hdr = ltx_header(b[off:off + ln])
        rows.append({"cell": cell, "cell_epoch": epoch, "txid": txid, "len": ln,
                     "ltx_min": hdr["min_txid"], "ltx_max": hdr["max_txid"],
                     "commit": hdr["commit"], "ts": hdr["timestamp_ms"]})
    return rows


def parse_bundle_key(key):
    # log/<session...>/bundle/e<log_epoch>-<seq:08>.ltxb
    head, name = key.rsplit("/bundle/", 1)
    log_epoch, seq = name[1:-len(".ltxb")].split("-")
    return {"session": head[len("log/"):], "log_epoch": int(log_epoch), "seq": int(seq)}


def short(cell):
    cls, _, h = cell.partition(":")
    return f"{cls}:{h[:8]}" if h else cell


def leases(args):
    out = []
    for key, _, _ in ls(args.alias, args.bucket, "nodes/"):
        try:
            rec = json.loads(cat(args.alias, args.bucket, key))
        except Exception as e:  # noqa: BLE001 - a racing GC delete is fine here
            out.append({"key": key, "error": str(e)})
            continue
        log = rec.get("log") or {}
        out.append({
            "node": rec.get("node"),
            "expires_in_ms": rec.get("expires_ms", 0) - int(time.time() * 1000),
            "generation": (rec.get("ownership_index_generation") or "")[:16],
            "log_state": log.get("state"), "log_epoch": log.get("epoch"),
            "ensemble": log.get("ensemble"), "claimant": log.get("claimant"),
            "owned_cells": (rec.get("load") or {}).get("owned_cells"),
        })
    return out


def cmd_leases(args):
    for l in leases(args):
        print(json.dumps(l))


def cmd_bundles(args):
    items = sorted(ls(args.alias, args.bucket, "log/"), key=lambda x: x[2])
    items = [i for i in items if i[0].endswith(".ltxb")]
    if args.n:
        items = items[-args.n:]
    for key, size, modified in items:
        meta = parse_bundle_key(key)
        try:
            rows = bundle_rows(cat(args.alias, args.bucket, key))
        except RuntimeError:
            print(f"{modified}  {key}  (gone: deleted by bundle GC)")
            continue
        per_cell = defaultdict(list)
        for r in rows:
            per_cell[(r["cell"], r["cell_epoch"])].append(r["txid"])
        cells = ", ".join(f"{short(c)} e{e} tx{min(t)}-{max(t)}" for (c, e), t in sorted(per_cell.items()))
        print(f"{modified}  {meta['session']}  e{meta['log_epoch']}-{meta['seq']:08d}  {size:>7} B  {len(rows)} rows  [{cells}]")


def cmd_cells(args):
    objs = ls(args.alias, args.bucket, "cells/")
    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    owners = {}
    for key, size, _ in objs:
        cell, _, rest = key[len("cells/"):].partition("/")
        cell = unquote(cell)
        if rest == "own.json":
            owners[cell] = json.loads(cat(args.alias, args.bucket, key))
        elif rest.startswith("ltx/e") and rest.endswith(".ltx"):
            _, epoch, level, name = rest.split("/")
            lo, hi = name[:-4].split("-")
            tree[cell][epoch][level].append((int(lo, 16), int(hi, 16)))
    for cell in sorted(set(owners) | set(tree)):
        own = owners.get(cell, {})
        print(f"{short(cell)}  owner={own.get('node') or '(released)'} epoch={own.get('epoch')}")
        for epoch in sorted(tree[cell], key=lambda e: int(e[1:])):
            parts = []
            for level, ranges in sorted(tree[cell][epoch].items()):
                ranges.sort()
                parts.append(f"L{int(level, 16)}: {len(ranges)} obj tx{ranges[0][0]}-{max(r[1] for r in ranges)}")
            print(f"    {epoch}: " + "; ".join(parts))


def cmd_watch(args):
    seen = set()
    last_leases = {}
    with open(args.output, "a") as out:
        def emit(event):
            event["t"] = time.time()
            out.write(json.dumps(event) + "\n")
            out.flush()
            print(json.dumps(event), file=sys.stderr)
        while True:
            for l in leases(args):
                sig = (l.get("log_state"), l.get("log_epoch"), l.get("generation"), l.get("claimant"))
                if last_leases.get(l.get("node")) != sig:
                    last_leases[l.get("node")] = sig
                    emit({"kind": "lease", **l})
            for key, size, modified in ls(args.alias, args.bucket, "log/"):
                if key.endswith(".ltxb") and key not in seen:
                    seen.add(key)
                    # GET immediately, as cdcell would: bundle GC can delete
                    # it within ~30 s once per-cell prefixes cover its rows.
                    try:
                        rows = [{"cell": r["cell"], "cell_epoch": r["cell_epoch"], "txid": r["txid"]}
                                for r in bundle_rows(cat(args.alias, args.bucket, key))]
                    except RuntimeError:
                        rows = None
                    emit({"kind": "bundle", "key": key, "size": size, "last_modified": modified,
                          "rows": rows, **parse_bundle_key(key)})
            time.sleep(args.interval)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--alias", default="e2e")
    p.add_argument("--bucket", default="celld")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("leases").set_defaults(fn=cmd_leases)
    b = sub.add_parser("bundles")
    b.add_argument("-n", type=int, default=0, help="only the newest N bundles")
    b.set_defaults(fn=cmd_bundles)
    sub.add_parser("cells").set_defaults(fn=cmd_cells)
    w = sub.add_parser("watch")
    w.add_argument("-o", "--output", required=True)
    w.add_argument("--interval", type=float, default=0.5)
    w.set_defaults(fn=cmd_watch)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
