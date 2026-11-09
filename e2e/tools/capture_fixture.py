#!/usr/bin/env python3
"""Capture a real celld fixture for cdcell-core's tests (M1 gate, part 2).

Runs mutations against a brand-new shop on the e2e fleet, then saves every
bundle row celld wrote for that cell (TXID-1 snapshot onward) plus the shop's
own `?op=dump` as ground truth.

  capture_fixture.py OUT_DIR [--ops 150] [--name fx-<time>]

The cell id is computed from the name the way celld does (design §14,
celld engine_api.rs:433-491): key = SHA256("cells:v1:<len(script)>:<script>:<Class>"),
id[0:16] = HMAC(key, name)[0:16], id[16:32] = HMAC(key, id[0:16])[0:16].
"""
import argparse
import hashlib
import hmac
import json
import os
import struct
import subprocess
import sys
import time
import urllib.request

URL = "http://127.0.0.1:8180"


def id_from_name(script, cls, name):
    key = hashlib.sha256(f"cells:v1:{len(script)}:{script}:{cls}".encode()).digest()
    a = hmac.new(key, name.encode(), hashlib.sha256).digest()[:16]
    b = hmac.new(key, a, hashlib.sha256).digest()[:16]
    return f"{cls}:{(a + b).hex()}"


def mc(*args):
    return subprocess.run(["mc", *args], capture_output=True, check=True).stdout


def bundle_rows(b):
    assert b[-4:] == b"CLB1"
    (flen,) = struct.unpack("<I", b[-8:-4])
    footer, i, out = b[-8 - flen:-8], 0, []
    while i < len(footer):
        (clen,) = struct.unpack_from("<H", footer, i)
        cell = footer[i + 2:i + 2 + clen].decode()
        i += 2 + clen
        epoch, txid, off, ln = struct.unpack_from("<QQQQ", footer, i)
        i += 32
        out.append((cell, epoch, txid, b[off:off + ln]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--ops", type=int, default=150)
    ap.add_argument("--name", default=f"fx-{int(time.time())}")
    a = ap.parse_args()
    cell = id_from_name("shop", "Shop", a.name)
    print(f"shop {a.name} -> {cell}")
    ops = {}
    for i in range(a.ops):
        op = json.load(urllib.request.urlopen(f"{URL}/?shop={a.name}&op=random", timeout=30))["op"]
        ops[op] = ops.get(op, 0) + 1
    print("ops:", ops)
    time.sleep(4)  # bundles flush every 2 s
    dump = json.load(urllib.request.urlopen(f"{URL}/?shop={a.name}&op=dump", timeout=30))
    time.sleep(4)  # the dump itself is a read; nothing new to flush, but be safe

    rows = {}
    listing = mc("ls", "--recursive", "--json", "e2e/celld/log/").decode().splitlines()
    for line in listing:
        key = json.loads(line)["key"]
        if not key.endswith(".ltxb"):
            continue
        try:
            body = mc("cat", f"e2e/celld/log/{key}")
        except subprocess.CalledProcessError:
            continue  # GC'd between list and get
        for c, epoch, txid, l0 in bundle_rows(body):
            if c == cell:
                rows[(epoch, txid)] = l0
    if not rows:
        sys.exit("no bundle rows for that cell (fleet in bucket mode, or already folded?)")
    keys = sorted(rows)
    print(f"captured {len(rows)} rows: epochs {sorted({e for e, _ in keys})}, txids {keys[0][1]}..{keys[-1][1]}")
    d = os.path.join(a.out, a.name)
    os.makedirs(d, exist_ok=True)
    for (epoch, txid), l0 in rows.items():
        with open(os.path.join(d, f"e{epoch:04d}-{txid:016x}.ltx"), "wb") as f:
            f.write(l0)
    with open(os.path.join(d, "dump.json"), "w") as f:
        json.dump({"cell": cell, "name": a.name, "ops": ops, "dump": dump}, f, indent=1)
    print("wrote", d)


if __name__ == "__main__":
    main()
