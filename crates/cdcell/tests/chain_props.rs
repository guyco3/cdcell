// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Property tests of the pure chain core against celld itself: the same
//! listings go through cdcell's [`epoch_chain`] / [`best_path`] and through
//! celld-ltx's `EpochChain::build` / `calc_restore_plan` over an in-memory
//! [`ReplicaClient`].
//!
//! Generated histories use a small TXID range so links, overlaps and
//! straddles (folds over L0 runs, drain L0s from an old seed, fenced tails
//! past a cut, late snapshots) happen often.

use std::collections::BTreeMap;

use async_trait::async_trait;
use cdcell::batch::chain::{best_path, epoch_chain, Grain, Path};
use cdcell::batch::obj::Obj;
use celld_ltx::client::epochs::EpochChain;
use celld_ltx::replica::calc_restore_plan;
use celld_ltx::{Error, FileInfo, ReplicaClient, Result, TXID};
use proptest::prelude::*;

/// One epoch's listing. The epoch rides in `FileInfo::size` so objects
/// coming back through `EpochChain` can be attributed to their epoch.
#[derive(Clone)]
struct MemEpoch {
    epoch: u64,
    objs: Vec<Obj>,
}

#[async_trait]
impl ReplicaClient for MemEpoch {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        let mut files: Vec<FileInfo> = self
            .objs
            .iter()
            .filter(|o| o.level as i32 == level && o.min >= seek.0)
            .map(|o| FileInfo {
                level,
                min_txid: TXID(o.min),
                max_txid: TXID(o.max),
                pre_apply_checksum: 0,
                post_apply_checksum: 0,
                size: self.epoch as i64,
                created_at: None,
            })
            .collect();
        // The object store's order: keys sort by min, then max.
        files.sort_by_key(|f| (f.min_txid, f.max_txid));
        files.dedup_by_key(|f| (f.min_txid, f.max_txid));
        Ok(files)
    }

    async fn open_ltx_file(&self, _: i32, _: TXID, _: TXID) -> Result<Vec<u8>> {
        unimplemented!("listings only")
    }

    async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> Result<FileInfo> {
        unimplemented!("listings only")
    }

    async fn delete_ltx_files(&self, _: &[FileInfo]) -> Result<()> {
        unimplemented!("listings only")
    }

    async fn delete_all(&self) -> Result<()> {
        unimplemented!("listings only")
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(f)
}

async fn celld_chain(by_epoch: &BTreeMap<u64, Vec<Obj>>) -> Result<EpochChain<MemEpoch>> {
    let epochs = by_epoch
        .iter()
        .map(|(&epoch, objs)| {
            (
                epoch,
                MemEpoch {
                    epoch,
                    objs: objs.clone(),
                },
            )
        })
        .collect();
    EpochChain::build(epochs).await
}

/// Every object a celld chain serves, as cdcell objects.
async fn chain_objs(chain: &EpochChain<MemEpoch>) -> Vec<Obj> {
    let mut out = Vec::new();
    for level in 0..=9 {
        for f in chain.ltx_files(level, TXID(0)).await.expect("list") {
            out.push(Obj {
                epoch: f.size as u64,
                level: level as u32,
                min: f.min_txid.0,
                max: f.max_txid.0,
            });
        }
    }
    out.sort();
    out
}

/// How an epoch's history starts.
#[derive(Clone, Debug)]
enum Start {
    /// A snapshot opener at TXID 1 (as an L0, or an L9 when `l9`).
    Opener { l9: bool },
    /// A paged continuation after one of the previous epoch's object ends
    /// (picked by index): the previous epoch may run past it (fenced tail).
    AfterPrev(usize),
    /// A continuation after an end of the epoch two below: the epoch in
    /// between (often a late snapshot, or a fenced owner's tail) is not the
    /// link and must be skipped.
    AfterOlder(usize),
    /// Anywhere: usually breaks the link.
    At(u64),
}

#[derive(Clone, Debug)]
struct EpochGen {
    start: Start,
    /// Contiguous L0 run lengths from the start.
    lens: Vec<u64>,
    /// A fold over L0s `i..=j` of the run (straddles a cursor inside it).
    fold: Option<(usize, usize)>,
    /// A drain L0 from the start of L0 `i` to `extra` past the run's end.
    drain: Option<(usize, u64)>,
    /// Arbitrary objects `(level, min, len)`.
    noise: Vec<(u32, u64, u64)>,
    /// No objects at all.
    empty: bool,
}

fn arb_epoch() -> impl Strategy<Value = EpochGen> {
    let start = prop_oneof![
        2 => any::<bool>().prop_map(|l9| Start::Opener { l9 }),
        4 => (0usize..16).prop_map(Start::AfterPrev),
        2 => (0usize..16).prop_map(Start::AfterOlder),
        1 => (1u64..40).prop_map(Start::At),
    ];
    let level = prop_oneof![4 => Just(0u32), 2 => Just(1u32), 1 => Just(2u32), 1 => Just(9u32)];
    (
        start,
        prop::collection::vec(1u64..6, 0..7),
        prop::option::of((0usize..7, 0usize..7)),
        prop::option::of((0usize..7, 0u64..4)),
        prop::collection::vec((level, 1u64..45, 0u64..12), 0..3),
        prop::bool::weighted(0.15),
    )
        .prop_map(|(start, lens, fold, drain, noise, empty)| EpochGen {
            start,
            lens,
            fold,
            drain,
            noise,
            empty,
        })
}

/// Builds the listings epoch by epoch (epoch numbers `1..=n`).
fn build(gens: &[EpochGen]) -> BTreeMap<u64, Vec<Obj>> {
    let mut out: BTreeMap<u64, Vec<Obj>> = BTreeMap::new();
    // Distinct object ends of each epoch built so far.
    let mut ends: Vec<Vec<u64>> = Vec::new();
    for (i, g) in gens.iter().enumerate() {
        let epoch = i as u64 + 1;
        let o = |level, min, max| Obj {
            epoch,
            level,
            min,
            max,
        };
        let mut objs = Vec::new();
        if !g.empty {
            let after = |back: usize, k: usize| {
                let e = ends.len().checked_sub(back).map(|i| &ends[i])?;
                (!e.is_empty()).then(|| e[k % e.len()] + 1)
            };
            let lo = match g.start {
                Start::Opener { .. } => 1,
                Start::AfterPrev(k) => after(1, k).unwrap_or(1),
                Start::AfterOlder(k) => after(2, k).unwrap_or(1),
                Start::At(t) => t,
            };
            let mut run = Vec::new();
            let mut next = lo;
            for (j, len) in g.lens.iter().enumerate() {
                let (min, max) = (next, next + len - 1);
                if j == 0 && matches!(g.start, Start::Opener { l9: true }) {
                    run.push(o(9, 1, max));
                } else {
                    run.push(o(0, min, max));
                }
                next = max + 1;
            }
            if let (Some((a, b)), false) = (g.fold, run.is_empty()) {
                let (a, b) = (a % run.len(), b % run.len());
                let (a, b) = (a.min(b), a.max(b));
                objs.push(o(1, run[a].min, run[b].max));
            }
            if let (Some((a, extra)), false) = (g.drain, run.is_empty()) {
                objs.push(o(0, run[a % run.len()].min, next - 1 + extra));
            }
            objs.extend(run);
            for &(level, min, len) in &g.noise {
                let min = if level == 9 { 1 } else { min };
                objs.push(o(level, min, min + len));
            }
        }
        let mut e: Vec<u64> = objs.iter().map(|o| o.max).collect();
        e.sort();
        e.dedup();
        ends.push(e);
        out.insert(epoch, objs);
    }
    out
}

fn arb_history() -> impl Strategy<Value = BTreeMap<u64, Vec<Obj>>> {
    prop::collection::vec(arb_epoch(), 1..5).prop_map(|g| build(&g))
}

/// Is `steps` a valid path from `from` (each step applies at the running
/// position, never past `limit`), and where does it end?
fn walk(steps: &[Obj], from: u64, limit: Option<u64>) -> Option<u64> {
    let mut p = from;
    for s in steps {
        if !(s.min <= p + 1 && p < s.max) || limit.is_some_and(|l| s.max > l) {
            return None;
        }
        p = s.max;
    }
    Some(p)
}

/// Exhaustive search: `(reach, steps)` of the best path by the same order
/// [`best_path`] promises.
fn brute(objs: &[Obj], from: u64, limit: Option<u64>, grain: Grain) -> (u64, usize) {
    fn go(
        objs: &[Obj],
        p: u64,
        n: usize,
        limit: Option<u64>,
        grain: Grain,
        best: &mut (u64, usize),
    ) {
        let better = p > best.0
            || (p == best.0
                && match grain {
                    Grain::Finest => n > best.1,
                    Grain::Fold => n < best.1,
                });
        if better {
            *best = (p, n);
        }
        for s in objs {
            if s.min <= p + 1 && p < s.max && limit.is_none_or(|l| s.max <= l) {
                go(objs, s.max, n + 1, limit, grain, best);
            }
        }
    }
    let mut best = (from, 0);
    go(objs, from, 0, limit, grain, &mut best);
    best
}

fn arb_small_objs() -> impl Strategy<Value = Vec<Obj>> {
    prop::collection::vec((0u32..3, 1u64..16, 0u64..6), 0..=8).prop_map(|v| {
        v.into_iter()
            .map(|(level, min, len)| Obj {
                epoch: 1,
                level,
                min,
                max: min + len,
            })
            .collect()
    })
}

fn arb_grain() -> impl Strategy<Value = Grain> {
    prop_oneof![Just(Grain::Finest), Just(Grain::Fold)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn prop_epoch_chain_matches_celld(by_epoch in arb_history()) {
        let ours = epoch_chain(&by_epoch);
        match block_on(celld_chain(&by_epoch)) {
            Err(Error::TxNotAvailable) => prop_assert_eq!(ours, None),
            Err(e) => panic!("celld: {e:?}"),
            Ok(theirs) => {
                let ours = ours.expect("celld found a chain");
                let spans: Vec<(u64, TXID)> =
                    ours.iter().map(|c| (c.epoch, TXID(c.lo))).collect();
                prop_assert_eq!(spans, theirs.spans());
                let mut objs: Vec<Obj> =
                    ours.iter().flat_map(|c| c.objs.iter().copied()).collect();
                objs.sort();
                objs.dedup();
                prop_assert_eq!(objs, block_on(chain_objs(&theirs)));
            }
        }
    }

    #[test]
    fn prop_best_path_valid_and_maximal(
        objs in arb_small_objs(),
        from in 0u64..12,
        limit in prop::option::of(0u64..24),
        grain in arb_grain(),
    ) {
        let got = best_path(&objs, from, limit, grain);
        prop_assert_eq!(walk(&got.steps, from, limit), Some(got.reach));
        prop_assert_eq!((got.reach, got.steps.len()), brute(&objs, from, limit, grain));
        let mut rev = objs.clone();
        rev.reverse();
        prop_assert_eq!(best_path(&rev, from, limit, grain), got);
    }

    #[test]
    fn prop_chain_path_reach_equals_celld_restore_plan(by_epoch in arb_history()) {
        let Ok(theirs) = block_on(celld_chain(&by_epoch)) else {
            return Ok(());
        };
        let Ok(plan) = block_on(calc_restore_plan(&theirs, TXID(0))) else {
            return Ok(());
        };
        let celld_reach = plan.iter().map(|f| f.max_txid.0).max().unwrap_or(0);
        let objs: Vec<Obj> = epoch_chain(&by_epoch)
            .expect("celld found a chain")
            .into_iter()
            .flat_map(|c| c.objs)
            .collect();
        for grain in [Grain::Finest, Grain::Fold] {
            let Path { steps, reach } = best_path(&objs, 0, None, grain);
            prop_assert_eq!(reach, celld_reach);
            prop_assert_eq!(walk(&steps, 0, None), Some(reach));
            let bounded = match grain {
                Grain::Finest => steps.len() >= plan.len(),
                Grain::Fold => steps.len() <= plan.len(),
            };
            prop_assert!(bounded, "{:?}: {} steps vs celld {}", grain, steps.len(), plan.len());
        }
    }
}

/// Guards the properties against vacuity: the history generator must
/// produce multi-epoch links, skipped epochs, fenced tails clipped away and
/// chains celld can restore.
#[test]
fn generators_cover_links_skips_and_restores() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let (mut links, mut skips, mut clipped, mut restores) = (0, 0, 0, 0);
    for _ in 0..256 {
        let by_epoch = arb_history().new_tree(&mut runner).unwrap().current();
        let Some(chain) = epoch_chain(&by_epoch) else {
            continue;
        };
        links += usize::from(chain.len() >= 2);
        let first = chain[0].epoch;
        let visited = by_epoch
            .range(first..)
            .filter(|(_, objs)| !objs.is_empty())
            .count();
        skips += usize::from(visited > chain.len());
        let kept: usize = chain.iter().map(|c| c.objs.len()).sum();
        let listed: usize = chain.iter().map(|c| by_epoch[&c.epoch].len()).sum();
        clipped += usize::from(kept < listed);
        let restored = block_on(async {
            let c = celld_chain(&by_epoch).await.ok()?;
            calc_restore_plan(&c, TXID(0)).await.ok()
        });
        restores += usize::from(restored.is_some_and(|p| p.len() >= 2));
    }
    assert!(
        links >= 20 && skips >= 10 && clipped >= 10 && restores >= 20,
        "links {links} skips {skips} clipped {clipped} restores {restores}"
    );
}
