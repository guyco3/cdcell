// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Property tests of the cell planner and proof over generated celld
//! histories: a small executor answers LISTs and GETs from the bucket's
//! final contents, with `own.json` at the newest epoch.
//!
//! - **No phantom:** every proven step is an object of celld's restore
//!   chain ([`epoch_chain`] over everything in the bucket), the steps form a
//!   valid path from the cursor, and a rebuild lands on the cursor.
//! - **Convergence:** after runs that saw random subsets of the keys, a run
//!   that sees every key reaches the chain's head (or parks, if the owner's
//!   epoch is empty).

use std::collections::{BTreeMap, BTreeSet};

use cdcell::batch::chain::{epoch_chain, Grain};
use cdcell::batch::obj::{Obj, Pos};
use cdcell::batch::plan::{
    plan_cell, prove, Base, CellView, Decision, Fetched, GapMemo, ListReq, Listed, ParkReason,
    PlanSettings, StateAvail, Step,
};
use proptest::prelude::*;

fn o(epoch: u64, level: u32, min: u64, max: u64) -> Obj {
    Obj {
        epoch,
        level,
        min,
        max,
    }
}

#[derive(Clone, Debug)]
enum Kind {
    /// A snapshot epoch: TXID-1 opener (an L9 when `l9`).
    Snapshot { l9: bool },
    /// A paged epoch: a zero-page marker at the chain's end + 1.
    Paged,
    /// No objects.
    Empty,
    /// A fenced owner's late snapshot, older than the chain's end: celld
    /// skips it.
    Late,
}

#[derive(Clone, Debug)]
struct EpochGen {
    kind: Kind,
    /// Contiguous L0 run lengths after the opener or marker.
    lens: Vec<u64>,
    /// A fold (L1) over run objects `i..=j`.
    fold: Option<(usize, usize)>,
    /// A drain L0 from run object `i` (not a marker) to the end.
    drain: Option<usize>,
    /// A handoff L9 `1-end` (snapshot epochs).
    handoff: bool,
    /// Objects a fenced owner wrote past the cut in the previous epoch.
    fenced: u64,
}

fn arb_epoch() -> impl Strategy<Value = EpochGen> {
    let kind = prop_oneof![
        2 => any::<bool>().prop_map(|l9| Kind::Snapshot { l9 }),
        4 => Just(Kind::Paged),
        1 => Just(Kind::Empty),
        1 => Just(Kind::Late),
    ];
    (
        kind,
        prop::collection::vec(1u64..5, 0..5),
        prop::option::of((0usize..6, 0usize..6)),
        prop::option::of(0usize..6),
        prop::bool::weighted(0.2),
        prop_oneof![1 => Just(0u64), 1 => 1u64..4],
    )
        .prop_map(|(kind, lens, fold, drain, handoff, fenced)| EpochGen {
            kind,
            lens,
            fold,
            drain,
            handoff,
            fenced,
        })
}

struct Truth {
    objs: Vec<Obj>,
    markers: BTreeSet<Obj>,
    own: u64,
}

fn build(gens: &[EpochGen]) -> Truth {
    let mut by: BTreeMap<u64, Vec<Obj>> = BTreeMap::new();
    let mut markers = BTreeSet::new();
    // (epoch, last TXID) of the chain so far.
    let mut head: Option<(u64, u64)> = None;
    let n = gens.len() as u64;
    for (i, g) in gens.iter().enumerate() {
        let e = i as u64 + 1;
        let kind = match (&g.kind, head) {
            (Kind::Paged, None) => Kind::Snapshot { l9: false },
            (Kind::Late, None) => Kind::Empty,
            // own.json's epoch is never a late snapshot.
            (Kind::Late, _) if e == n => Kind::Empty,
            (k, _) => k.clone(),
        };
        let mut objs = Vec::new();
        match kind {
            Kind::Empty => {}
            Kind::Late => {
                let (_, end) = head.expect("head");
                if end > 1 {
                    objs.push(o(e, 9, 1, end - 1));
                }
            }
            Kind::Snapshot { .. } | Kind::Paged => {
                if let (Some((he, end)), true) = (head, g.fenced > 0) {
                    by.entry(he)
                        .or_default()
                        .push(o(he, 0, end + 1, end + g.fenced));
                }
                let mut run = Vec::new();
                match kind {
                    Kind::Paged => {
                        let (_, end) = head.expect("head");
                        let m = o(e, 0, end + 1, end + 1);
                        markers.insert(m);
                        run.push(m);
                    }
                    Kind::Snapshot { l9: true } => {
                        run.push(o(e, 9, 1, 1 + g.lens.first().copied().unwrap_or(0)))
                    }
                    _ => run.push(o(e, 0, 1, 1)),
                }
                for len in &g.lens {
                    let min = run.last().expect("opener").max + 1;
                    run.push(o(e, 0, min, min + len - 1));
                }
                let end = run.last().expect("opener").max;
                if let Some((a, b)) = g.fold {
                    let (a, b) = (a % run.len(), b % run.len());
                    let (a, b) = (a.min(b), a.max(b));
                    if run[a].min < run[b].max {
                        objs.push(o(e, 1, run[a].min, run[b].max));
                    }
                }
                if let (Some(d), true) = (g.drain, run.len() > 2) {
                    let d = 1 + d % (run.len() - 1);
                    objs.push(o(e, 0, run[d].min, end));
                }
                if g.handoff && run[0].min == 1 {
                    objs.push(o(e, 9, 1, end));
                }
                objs.extend(run);
                head = Some((e, end));
            }
        }
        by.entry(e).or_default().extend(objs);
    }
    let mut objs: Vec<Obj> = by.into_values().flatten().collect();
    objs.sort();
    objs.dedup();
    Truth {
        objs,
        markers,
        own: n,
    }
}

impl Truth {
    fn by_epoch(&self) -> BTreeMap<u64, Vec<Obj>> {
        let mut by: BTreeMap<u64, Vec<Obj>> = (1..=self.own).map(|e| (e, Vec::new())).collect();
        for o in &self.objs {
            by.entry(o.epoch).or_default().push(*o);
        }
        by
    }

    /// celld's restore chain's objects, and its head when the owner's
    /// epoch has objects (otherwise the cell must park).
    fn chain(&self) -> (BTreeSet<Obj>, Option<Pos>) {
        let Some(chain) = epoch_chain(&self.by_epoch()) else {
            return (BTreeSet::new(), None);
        };
        let set = chain.iter().flat_map(|c| c.objs.iter().copied()).collect();
        let top = chain.last().expect("non-empty");
        let head = (top.epoch == self.own).then(|| Pos {
            epoch: top.epoch,
            txid: top.objs.iter().map(|o| o.max).max().expect("objs"),
        });
        (set, head)
    }

    fn answer(&self, listed: &mut Listed, req: &ListReq) {
        let epochs: Vec<u64> = self.objs.iter().map(|o| o.epoch).collect();
        listed.record(req, &epochs, &self.objs);
    }
}

#[derive(Clone, Debug, Default)]
struct Cell {
    cursor: Option<Pos>,
    last_l0: Option<Pos>,
    gap: Option<GapMemo>,
    parked: Option<ParkReason>,
}

fn check_valid(cursor: Option<Pos>, plan_base: &Base, steps: &[Step], chain: &BTreeSet<Obj>) {
    let mut pos = match plan_base {
        Base::Bootstrap | Base::Resync => None,
        Base::Cursor => cursor,
        Base::Rebuild(objs) => {
            let mut at: Option<Pos> = None;
            for (i, o) in objs.iter().enumerate() {
                assert!(chain.contains(o), "rebuild from a phantom {o:?}");
                match at {
                    None => assert!(i == 0 && o.min <= 1, "rebuild starts at a full image"),
                    Some(p) => assert!(o.min <= p.txid + 1 && p.txid < o.max && o.epoch >= p.epoch),
                }
                at = Some(Pos {
                    epoch: o.epoch,
                    txid: o.max,
                });
            }
            assert_eq!(at, cursor, "rebuild lands on the cursor");
            cursor
        }
    };
    for s in steps {
        let o = s.obj();
        assert!(chain.contains(&o), "phantom {s:?}");
        match s {
            Step::Opener(_) => {
                assert!(o.min <= 1);
                assert!(pos.is_none_or(|p| o.epoch > p.epoch), "{s:?} after {pos:?}");
            }
            _ => {
                let p = pos.expect("first step is an opener");
                assert!(o.epoch >= p.epoch, "{s:?} after {p:?}");
                assert!(o.min <= p.txid + 1 && p.txid < o.max, "{s:?} after {p:?}");
            }
        }
        pos = Some(Pos {
            epoch: o.epoch,
            txid: o.max,
        });
    }
}

/// One run of the executor for one cell.
fn run(cell: &mut Cell, truth: &Truth, notified: &[Obj], grain: Grain, state_missing: bool) {
    let s = PlanSettings {
        grain,
        gap_lists: 3,
    };
    let (chain, _) = truth.chain();
    let mut listed = Listed::default();
    let mut own_hint = None;
    let mut parked = cell.parked;
    let mut resync = false;
    let state = if state_missing {
        StateAvail::Missing
    } else {
        StateAvail::Present
    };
    for _ in 0..64 {
        let view = CellView {
            cursor: cell.cursor,
            state,
            notified,
            listed: &listed,
            gap: cell.gap,
            parked,
            last_l0: cell.last_l0,
            own_hint,
            resync,
        };
        match plan_cell(&view, &s) {
            Decision::Idle => return,
            Decision::Park(r) => {
                cell.parked = Some(r);
                return;
            }
            Decision::List(reqs) => {
                assert!(!reqs.is_empty());
                for r in &reqs {
                    truth.answer(&mut listed, r);
                }
            }
            Decision::Fetch(plan) => {
                let fetched: Vec<Fetched> = plan
                    .steps
                    .iter()
                    .map(Step::obj)
                    .chain(plan.probes.iter().copied())
                    .map(|obj| {
                        assert!(truth.objs.contains(&obj), "GET of a missing {obj:?}");
                        Fetched {
                            obj,
                            is_marker: truth.markers.contains(&obj),
                        }
                    })
                    .collect();
                let got = prove(&plan, &fetched, &listed, Some(truth.own), &s);
                if got.resync {
                    assert!(!resync, "resync twice in a run");
                    resync = true;
                    continue;
                }
                if got.parked == Some(ParkReason::Unproven) && own_hint.is_none() {
                    own_hint = Some(truth.own);
                    parked = got.parked;
                    continue;
                }
                check_valid(cell.cursor, &plan.base, &got.steps, &chain);
                cell.cursor = got.head;
                cell.last_l0 = got.last_l0;
                cell.gap = got.gap;
                cell.parked = got.parked;
                return;
            }
        }
    }
    panic!("no decision after 64 rounds");
}

fn arb_case() -> impl Strategy<Value = (Vec<EpochGen>, Vec<u8>, usize, Vec<bool>, bool)> {
    (
        prop::collection::vec(arb_epoch(), 1..7),
        prop::collection::vec(any::<u8>(), 1..40),
        0usize..4,
        prop::collection::vec(any::<bool>(), 5),
        any::<bool>(),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn prop_plan_prove_no_phantom_and_converges(
        (gens, masks, partial, missing, fold) in arb_case()
    ) {
        let truth = build(&gens);
        let grain = if fold { Grain::Fold } else { Grain::Finest };
        let mut cell = Cell::default();
        // Runs that see random subsets of the keys (the pending set keeps
        // what earlier runs were told).
        let mut seen: Vec<Obj> = Vec::new();
        for (r, &lose_state) in missing.iter().enumerate().take(partial) {
            seen.extend(
                truth
                    .objs
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| masks[i % masks.len()] & (1 << r) != 0)
                    .map(|(_, o)| *o),
            );
            let state_missing = lose_state && cell.cursor.is_some();
            run(&mut cell, &truth, &seen, grain, state_missing);
        }
        // Every key delivered.
        let last = missing[4] && cell.cursor.is_some();
        run(&mut cell, &truth, &truth.objs, grain, last);
        let (_, head) = truth.chain();
        match head {
            Some(h) => prop_assert_eq!(cell.cursor, Some(h), "{:?}", cell),
            None => prop_assert_eq!(cell.cursor, None),
        }
    }
}

/// A naive path over every object in the bucket, ignoring cuts, fenced
/// tails and late snapshots: each step applies at the running position
/// (any epoch at or above it), or is a full image of a higher epoch.
fn naive_steps(truth: &Truth, from: Option<Pos>, picks: &[u16]) -> Vec<Step> {
    let mut pos = from;
    let mut steps = Vec::new();
    for &pick in picks {
        let next: Vec<Step> = truth
            .objs
            .iter()
            .filter_map(|&o| match pos {
                None => (o.min <= 1).then_some(Step::Opener(o)),
                Some(p) if o.epoch > p.epoch && o.min <= 1 && pick % 3 == 0 => {
                    Some(Step::Opener(o))
                }
                Some(p) => (o.epoch >= p.epoch && o.min <= p.txid + 1 && p.txid < o.max)
                    .then_some(Step::Apply(o)),
            })
            .collect();
        if next.is_empty() {
            break;
        }
        let s = next[pick as usize % next.len()];
        let o = s.obj();
        pos = Some(Pos {
            epoch: o.epoch,
            txid: o.max,
        });
        steps.push(s);
    }
    steps
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// Whatever plan it is given, the proof confirms only chain objects,
    /// along a valid path, and never sends a cursor on the chain to resync.
    #[test]
    fn prop_prove_rejects_phantoms_in_any_plan(
        gens in prop::collection::vec(arb_epoch(), 1..7),
        start in prop::option::of(any::<u16>()),
        picks in prop::collection::vec(any::<u16>(), 0..12),
        list_mask in any::<u16>(),
        dirs in any::<bool>(),
    ) {
        let truth = build(&gens);
        let (chain, _) = truth.chain();
        // A cursor at the end of a chain object, or a bootstrap.
        let ends: Vec<Obj> = chain.iter().copied().collect();
        let from = match start {
            Some(i) if !ends.is_empty() => {
                let o = ends[i as usize % ends.len()];
                Some(Pos { epoch: o.epoch, txid: o.max })
            }
            _ => None,
        };
        let mut listed = Listed::default();
        if dirs {
            truth.answer(&mut listed, &ListReq::EpochDirs);
        }
        for e in 1..=truth.own {
            if list_mask & (1 << (e % 16)) != 0 {
                for level in cdcell::batch::plan::LEVELS {
                    truth.answer(
                        &mut listed,
                        &ListReq::Level { epoch: e, level, start_after: None },
                    );
                }
            }
        }
        let steps = naive_steps(&truth, from, &picks);
        let fetched: Vec<Fetched> = steps
            .iter()
            .map(|s| Fetched { obj: s.obj(), is_marker: truth.markers.contains(&s.obj()) })
            .collect();
        let base = if from.is_some() { Base::Cursor } else { Base::Bootstrap };
        let plan = cdcell::batch::plan::Plan {
            from,
            base: base.clone(),
            steps,
            probes: Vec::new(),
            dropped: Vec::new(),
            gap: None,
            last_l0: None,
            known: truth.objs.iter().copied().filter(|o| picks.len() % 2 == 0 || o.level != 1).collect(),
        };
        let got = prove(&plan, &fetched, &listed, Some(truth.own), &PlanSettings::default());
        prop_assert!(!got.resync, "cursor {:?} on the chain sent to resync", from);
        check_valid(from, &base, &got.steps, &chain);
    }
}
