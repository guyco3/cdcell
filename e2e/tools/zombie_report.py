#!/usr/bin/env python3
"""Check cdcell's bundle-confirmation rule against what celld actually kept.

Input: the JSONL written by `inspect.py watch` during a pause/kill test, plus
the bucket (via mc). For every bundle written by the frozen node's old
generation:

  rule verdict   the first lease observation strictly after the bundle became
                 visible: log "open" with the same generation => CONFIRMED,
                 anything else => SUSPECT
  ground truth   a row (cell, epoch, txid) is history iff the cell's per-cell
                 prefix e<epoch> covers txid contiguously from 1 (that prefix is
                 what the next owner restores after recovery folded bundles)

A CONFIRMED bundle with any row beyond coverage is a RULE VIOLATION.
A SUSPECT bundle's rows are resolved by coverage: history or phantom.

  zombie_report.py EVENTS.jsonl NODE [--alias e2e --bucket celld]
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import importlib.util

_spec = importlib.util.spec_from_file_location("cdcell_inspect", Path(__file__).resolve().parent / "inspect.py")
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)

SLACK_S = 0.6  # judge with the first lease poll after the bundle GET succeeded


def coverage(alias, bucket):
    """(cell, epoch) -> highest txid covered contiguously from 1 by per-cell objects."""
    ranges = defaultdict(list)
    for key, _, _ in tool.ls(alias, bucket, "cells/"):
        cell, _, rest = key[len("cells/"):].partition("/")
        if rest.startswith("ltx/e") and rest.endswith(".ltx"):
            _, epoch, _level, name = rest.split("/")
            lo, hi = (int(x, 16) for x in name[:-4].split("-"))
            ranges[(unquote(cell), int(epoch[1:]))].append((lo, hi))
    out = {}
    for k, rs in ranges.items():
        top = 0
        for lo, hi in sorted(rs):
            if lo <= top + 1:
                top = max(top, hi)
        out[k] = top
    return out


def ts(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("events")
    p.add_argument("node")
    p.add_argument("--alias", default="e2e")
    p.add_argument("--bucket", default="celld")
    a = p.parse_args()

    events = [json.loads(l) for l in open(a.events)]
    leases = [e for e in events if e["kind"] == "lease" and e.get("node") == a.node]
    print(f"== lease timeline for {a.node}")
    t0 = events[0]["t"]
    for e in leases:
        print(f"  +{e['t'] - t0:6.1f}s  state={e['log_state']} gen={e['generation']} claimant={e['claimant']}")

    gens = [e["generation"] for e in leases if e["generation"]]
    if not gens:
        sys.exit("no lease observations for that node")
    old_gen = gens[0]
    cov = coverage(a.alias, a.bucket)

    def lease_state_at(t):
        """The watcher logs a lease only when it changes, so the state in
        force at t is the last change observed at or before t."""
        state = leases[0]
        for e in leases:
            if e["t"] <= t:
                state = e
        return state

    bundles = [e for e in events if e["kind"] == "bundle"
               and e["session"].startswith(f"{a.node}/{old_gen}")]
    print(f"\n== {len(bundles)} bundles from {a.node} generation {old_gen}")
    totals = defaultdict(int)
    for b in sorted(bundles, key=lambda e: (e["log_epoch"], e["seq"])):
        # Visibility = when the watcher's GET first succeeded. S3 LastModified
        # is not a visibility signal (a slow upload can be stamped earlier).
        visible = b["t"]
        # cdcell's rule: after the bundle is visible, the writer's lease must
        # carry this generation and an Open log at any log epoch >= the
        # bundle's (recovery folds bundles of every log epoch).
        observed = lease_state_at(visible + SLACK_S)
        confirmed = (observed["log_state"] == "open" and observed["generation"] == old_gen
                     and (observed.get("log_epoch") or 0) >= b["log_epoch"])
        rows = b.get("rows")
        if rows is None:
            print(f"  e{b['log_epoch']}-{b['seq']:08d} +{visible - t0:6.1f}s  (deleted before the watcher could GET it)")
            continue
        beyond = [r for r in rows if r["txid"] > cov.get((r["cell"], r["cell_epoch"]), 0)]
        verdict = "CONFIRMED" if confirmed else "SUSPECT"
        if confirmed and beyond:
            outcome = f"RULE VIOLATION: {len(beyond)} rows not in history"
            totals["violation"] += 1
        elif confirmed:
            outcome = "ok (all rows in history)"
        elif beyond:
            outcome = f"{len(rows) - len(beyond)} rows history, {len(beyond)} PHANTOM rows dropped"
            totals["phantom_rows"] += len(beyond)
        else:
            outcome = "all rows turned out to be history"
        totals[verdict] += 1
        print(f"  e{b['log_epoch']}-{b['seq']:08d} +{visible - t0:6.1f}s  {len(rows):3d} rows  {verdict:9s} {outcome}")
    print(f"\n== summary: {dict(totals)}")
    print("   (no RULE VIOLATION means every bundle the rule confirmed really is history)")


if __name__ == "__main__":
    main()
