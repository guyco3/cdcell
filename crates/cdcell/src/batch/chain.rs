// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Which objects a cell's history is made of. PURE.
//!
//! [`epoch_chain`] picks the epochs (celld's restore view across paged
//! activations); [`best_path`] picks the objects inside them, from a
//! position to the furthest point reachable.

use std::collections::BTreeMap;

use super::obj::Obj;

/// celld's snapshot level (`compaction_level::SNAPSHOT_LEVEL`): a full
/// image `1-<max>` written at handoff. celld lists levels `0..=9` only.
pub const SNAPSHOT_LEVEL: u32 = 9;

/// One epoch of a chain: the first TXID it serves and its objects, clipped
/// to below the next epoch's `lo`, sorted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainEpoch {
    pub epoch: u64,
    pub lo: u64,
    pub objs: Vec<Obj>,
}

/// celld's `EpochChain::build` (`ltx/src/client/epochs.rs`), over listings
/// already in hand. Walks down from the newest epoch until one whose lowest
/// TXID is 1 (a snapshot opener or an L9). Below a continued epoch, an epoch
/// is clipped to TXIDs below that epoch's `lo` and is the link only if the
/// clipped objects end exactly at `lo - 1`; one that ends elsewhere (a
/// fenced owner's late snapshot) is skipped, as is an epoch with no objects
/// (the newest included). Returns the chain oldest first, or `None` when no
/// epoch has objects (celld's `TxNotAvailable`).
pub fn epoch_chain(by_epoch: &BTreeMap<u64, Vec<Obj>>) -> Option<Vec<ChainEpoch>> {
    let mut chain: Vec<ChainEpoch> = Vec::new();
    for (&epoch, objs) in by_epoch.iter().rev() {
        let mut objs: Vec<Obj> = objs
            .iter()
            .filter(|o| o.level <= SNAPSHOT_LEVEL)
            .copied()
            .collect();
        if let Some(above) = chain.last() {
            // Clip to the TXIDs before the continuation's first; the link
            // must then end at the cut.
            objs.retain(|o| o.max < above.lo);
            let ends = objs.iter().map(|o| o.max).max();
            if ends.is_some() && ends != above.lo.checked_sub(1) {
                continue;
            }
        }
        // Empty (or clipped to nothing): contributes nothing.
        let Some(lo) = objs.iter().map(|o| o.min).min() else {
            continue;
        };
        objs.sort();
        objs.dedup();
        chain.push(ChainEpoch { epoch, lo, objs });
        if lo == 1 {
            break;
        }
    }
    if chain.is_empty() {
        return None;
    }
    chain.reverse();
    Some(chain)
}

/// How to choose among paths of equal reach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grain {
    /// The most steps: every capture the objects still distinguish (design
    /// §2.2, the default).
    Finest,
    /// The fewest steps: fewest GETs (`CDCELL_GRAIN=fold`).
    Fold,
}

/// The objects to apply in order, and the TXID reached after them (the
/// starting position when there are none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Path {
    pub steps: Vec<Obj>,
    pub reach: u64,
}

/// The path from TXID `from` that reaches furthest, at most `limit`. An
/// object `a-b` applies at position `p` iff `a <= p+1 <= b`: LTX pages are
/// full page images, so an object that straddles `p` still lands exactly on
/// `b` (design §2.2 step 3). Among paths of maximal reach, `Finest` takes
/// the most steps and `Fold` the fewest. Ties go to the object first in
/// [`Obj`] order (lower epoch, then lower level, then lower min), then to the
/// lower predecessor position, so the result depends only on the set of
/// objects, not their order.
pub fn best_path(objs: &[Obj], from: u64, limit: Option<u64>, grain: Grain) -> Path {
    // Objects that can move past `from` within `limit`, by end then Obj
    // order: every way to reach a position is settled before it is used.
    let mut cand: Vec<Obj> = objs
        .iter()
        .filter(|o| o.min <= o.max && o.max > from && limit.is_none_or(|l| o.max <= l))
        .copied()
        .collect();
    cand.sort_by_key(|o| (o.max, *o));
    cand.dedup();
    // Positions a path can stand on: `from` and every end, ascending.
    let mut pos: Vec<u64> = std::iter::once(from)
        .chain(cand.iter().map(|o| o.max))
        .collect();
    pos.dedup();
    // Per position: (steps, object index, predecessor position index).
    let mut best: Vec<Option<(usize, usize, usize)>> = vec![None; pos.len()];
    best[0] = Some((0, usize::MAX, usize::MAX));
    for (k, o) in cand.iter().enumerate() {
        let to = pos.partition_point(|&q| q < o.max);
        // Applies at q iff min <= q+1 (and q < max): q >= min-1.
        let first = pos.partition_point(|&q| q.saturating_add(1) < o.min);
        for q in first..to {
            let Some((steps, ..)) = best[q] else {
                continue;
            };
            let better = match best[to] {
                None => true,
                Some((b, ..)) => match grain {
                    Grain::Finest => steps + 1 > b,
                    Grain::Fold => steps + 1 < b,
                },
            };
            if better {
                best[to] = Some((steps + 1, k, q));
            }
        }
    }
    let end = best.iter().rposition(Option::is_some).unwrap_or(0);
    let mut steps = Vec::new();
    let mut at = end;
    while at != 0 {
        let (_, k, q) = best[at].expect("reachable");
        steps.push(cand[k]);
        at = q;
    }
    steps.reverse();
    Path {
        steps,
        reach: pos[end],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(epoch: u64, level: u32, min: u64, max: u64) -> Obj {
        Obj {
            epoch,
            level,
            min,
            max,
        }
    }

    fn chain(epochs: &[(u64, &[Obj])]) -> Option<Vec<ChainEpoch>> {
        let map = epochs
            .iter()
            .map(|(e, objs)| (*e, objs.to_vec()))
            .collect::<BTreeMap<_, _>>();
        epoch_chain(&map)
    }

    fn ce(epoch: u64, lo: u64, objs: &[Obj]) -> ChainEpoch {
        ChainEpoch {
            epoch,
            lo,
            objs: objs.to_vec(),
        }
    }

    #[test]
    fn epoch_chain_single_snapshot_epoch() {
        let e1 = [o(1, 0, 1, 1), o(1, 0, 2, 5), o(1, 1, 1, 5)];
        assert_eq!(
            chain(&[(1, &e1)]),
            Some(vec![ce(
                1,
                1,
                &[o(1, 0, 1, 1), o(1, 0, 2, 5), o(1, 1, 1, 5)]
            )])
        );
    }

    #[test]
    fn epoch_chain_paged_link_clips_fenced_tail() {
        // e1's owner was fenced after the cut at 20 but kept writing 21-30
        // and a fold over 1-30: neither is history.
        let e1 = [
            o(1, 0, 21, 30),
            o(1, 0, 1, 10),
            o(1, 1, 1, 30),
            o(1, 0, 11, 20),
        ];
        let e2 = [o(2, 0, 21, 25)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2)]),
            Some(vec![
                ce(1, 1, &[o(1, 0, 1, 10), o(1, 0, 11, 20)]),
                ce(2, 21, &e2),
            ])
        );
    }

    #[test]
    fn epoch_chain_skips_intermediate_epoch_not_ending_at_cut() {
        // e2 holds a fenced owner's late snapshot: whole, but older than the
        // cut at 20 (CelldPersistencePaged.tla BreakLinkByOrder).
        let e1 = [o(1, 0, 1, 20)];
        let e2 = [o(2, 9, 1, 15)];
        let e3 = [o(3, 0, 21, 30)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2), (3, &e3)]),
            Some(vec![ce(1, 1, &e1), ce(3, 21, &e3)])
        );
    }

    #[test]
    fn epoch_chain_skips_empty_epochs() {
        let e1 = [o(1, 0, 1, 20)];
        let e3 = [o(3, 0, 21, 30)];
        assert_eq!(
            chain(&[(1, &e1), (2, &[]), (3, &e3)]),
            Some(vec![ce(1, 1, &e1), ce(3, 21, &e3)])
        );
    }

    #[test]
    fn epoch_chain_skips_empty_newest_epoch() {
        // celld epochs.rs:64: an empty newest epoch is skipped and the epoch
        // below becomes the top, unclipped (its tail is not cut by anything).
        let e1 = [o(1, 0, 1, 20)];
        let e2 = [o(2, 0, 21, 30), o(2, 0, 31, 40)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2), (3, &[])]),
            Some(vec![ce(1, 1, &e1), ce(2, 21, &e2)])
        );
    }

    #[test]
    fn epoch_chain_stops_at_first_lo_1() {
        // e2 opens with a TXID-1 snapshot: e1 is never visited.
        let e1 = [o(1, 0, 1, 10)];
        let e2 = [o(2, 0, 1, 5)];
        let e3 = [o(3, 0, 6, 8)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2), (3, &e3)]),
            Some(vec![ce(2, 1, &e2), ce(3, 6, &e3)])
        );
    }

    #[test]
    fn epoch_chain_l9_counts_as_lo_1() {
        let e1 = [o(1, 0, 1, 10)];
        let e2 = [o(2, 0, 11, 12), o(2, 9, 1, 10)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2)]),
            Some(vec![ce(2, 1, &[o(2, 0, 11, 12), o(2, 9, 1, 10)])])
        );
    }

    #[test]
    fn epoch_chain_none_without_objects() {
        assert_eq!(chain(&[]), None);
        assert_eq!(chain(&[(1, &[]), (2, &[])]), None);
    }

    #[test]
    fn epoch_chain_keeps_top_when_no_link_reaches_cut() {
        // No epoch below e2 ends at its cut (14): celld's chain is e2 alone,
        // whose lo != 1 tells the planner it has no base.
        let e1 = [o(1, 0, 1, 10)];
        let e2 = [o(2, 0, 15, 20)];
        assert_eq!(chain(&[(1, &e1), (2, &e2)]), Some(vec![ce(2, 15, &e2)]));
    }

    #[test]
    fn epoch_chain_ignores_levels_celld_does_not_list() {
        let e1 = [o(1, 0, 1, 5), o(1, 10, 1, 9)];
        assert_eq!(chain(&[(1, &e1)]), Some(vec![ce(1, 1, &e1[..1])]));
    }

    fn path(steps: &[Obj], reach: u64) -> Path {
        Path {
            steps: steps.to_vec(),
            reach,
        }
    }

    #[test]
    fn best_path_prefers_l0s_over_covering_l1() {
        let mut objs: Vec<Obj> = (1..=256).map(|t| o(1, 0, t, t)).collect();
        objs.push(o(1, 1, 1, 256));
        let finest = best_path(&objs, 0, None, Grain::Finest);
        assert_eq!(finest.reach, 256);
        assert_eq!(finest.steps.len(), 256);
        assert!(finest.steps.iter().all(|s| s.level == 0));
        assert_eq!(
            best_path(&objs, 0, None, Grain::Fold),
            path(&[o(1, 1, 1, 256)], 256)
        );
    }

    #[test]
    fn best_path_overlapping_merged_l0s() {
        let objs = [o(1, 0, 1, 5), o(1, 0, 3, 8), o(1, 0, 6, 10)];
        assert_eq!(best_path(&objs, 0, None, Grain::Finest), path(&objs, 10));
        assert_eq!(
            best_path(&objs, 0, None, Grain::Fold),
            path(&[o(1, 0, 1, 5), o(1, 0, 6, 10)], 10)
        );
    }

    #[test]
    fn best_path_straddling_object_applies_from_cursor() {
        let objs = [o(1, 1, 1, 256), o(1, 1, 257, 512)];
        assert_eq!(best_path(&objs, 100, None, Grain::Finest), path(&objs, 512));
        assert_eq!(
            best_path(&objs, 256, None, Grain::Finest),
            path(&objs[1..], 512)
        );
    }

    #[test]
    fn best_path_drain_l0_over_l1s() {
        // An eviction drains one L0 from the replica's last uploaded
        // position (1), over the folds 1-256 and 257-512.
        let objs = [o(1, 0, 1, 600), o(1, 1, 1, 256), o(1, 1, 257, 512)];
        assert_eq!(
            best_path(&objs, 0, None, Grain::Finest),
            path(&[o(1, 1, 1, 256), o(1, 1, 257, 512), o(1, 0, 1, 600)], 600)
        );
        assert_eq!(
            best_path(&objs, 300, None, Grain::Finest),
            path(&[o(1, 1, 257, 512), o(1, 0, 1, 600)], 600)
        );
        assert_eq!(
            best_path(&objs, 0, None, Grain::Fold),
            path(&[o(1, 0, 1, 600)], 600)
        );
    }

    #[test]
    fn best_path_stops_at_gap_reports_reach() {
        let objs = [o(1, 0, 1, 5), o(1, 0, 6, 10), o(1, 0, 15, 20)];
        assert_eq!(
            best_path(&objs, 0, None, Grain::Finest),
            path(&objs[..2], 10)
        );
        assert_eq!(best_path(&objs, 20, None, Grain::Finest), path(&[], 20));
        assert_eq!(best_path(&[], 7, None, Grain::Fold), path(&[], 7));
    }

    #[test]
    fn best_path_respects_limit() {
        let objs = [
            o(1, 0, 1, 5),
            o(1, 0, 6, 10),
            o(1, 1, 1, 20),
            o(1, 0, 11, 20),
        ];
        assert_eq!(
            best_path(&objs, 0, Some(15), Grain::Finest),
            path(&objs[..2], 10)
        );
        assert_eq!(
            best_path(&objs, 0, Some(10), Grain::Fold),
            path(&objs[..2], 10)
        );
        assert_eq!(best_path(&objs, 0, Some(4), Grain::Finest), path(&[], 0));
    }

    #[test]
    fn best_path_deterministic_tie_break() {
        // Same range at L0 and L1: the lower level wins, whatever the order.
        let a = [
            o(1, 0, 1, 10),
            o(1, 1, 1, 10),
            o(1, 0, 11, 12),
            o(1, 1, 11, 12),
        ];
        let mut b = a;
        b.reverse();
        for grain in [Grain::Finest, Grain::Fold] {
            let want = path(&[o(1, 0, 1, 10), o(1, 0, 11, 12)], 12);
            assert_eq!(best_path(&a, 0, None, grain), want);
            assert_eq!(best_path(&b, 0, None, grain), want);
        }
        // Equal step counts through different predecessors: 1-4,5-10 vs
        // 1-6,3-10 (both 2 steps). The lower predecessor (4) wins.
        let c = [o(1, 0, 3, 10), o(1, 0, 1, 6), o(1, 0, 5, 10), o(1, 0, 1, 4)];
        let want = path(&[o(1, 0, 1, 4), o(1, 0, 3, 10)], 10);
        assert_eq!(best_path(&c, 0, None, Grain::Fold), want);
        let mut d = c;
        d.reverse();
        assert_eq!(best_path(&d, 0, None, Grain::Fold), want);
    }

    // ---- Adversarial cases (PR #11 review) ----

    #[test]
    fn epoch_chain_l9_only_top_epoch_is_the_whole_chain() {
        let e1 = [o(1, 0, 1, 50)];
        let e2 = [o(2, 9, 1, 30)];
        assert_eq!(chain(&[(1, &e1), (2, &e2)]), Some(vec![ce(2, 1, &e2)]));
    }

    #[test]
    fn epoch_chain_marker_only_epochs() {
        // A paged activation that wrote only its marker `m-m` and was
        // evicted before any write, twice over.
        let e1 = [o(1, 0, 1, 20)];
        let e2 = [o(2, 0, 21, 21)];
        let e3 = [o(3, 0, 22, 22)];
        let e4 = [o(4, 0, 23, 23), o(4, 0, 24, 30), o(4, 1, 23, 30)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2), (3, &e3), (4, &e4)]),
            Some(vec![
                ce(1, 1, &e1),
                ce(2, 21, &e2),
                ce(3, 22, &e3),
                ce(4, 23, &[o(4, 0, 23, 23), o(4, 0, 24, 30), o(4, 1, 23, 30)]),
            ])
        );
        // A marker-only newest epoch.
        assert_eq!(
            chain(&[(1, &e1), (2, &e2)]),
            Some(vec![ce(1, 1, &e1), ce(2, 21, &e2)])
        );
    }

    #[test]
    fn epoch_chain_link_with_inner_gap_still_links() {
        // celld only checks the clipped end: a hole inside the link is the
        // planner's problem (best_path stops at it), not the chain's.
        let e1 = [o(1, 0, 1, 5), o(1, 0, 10, 20)];
        let e2 = [o(2, 0, 21, 25)];
        let got = chain(&[(1, &e1), (2, &e2)]).unwrap();
        assert_eq!(got, vec![ce(1, 1, &e1), ce(2, 21, &e2)]);
        let objs: Vec<Obj> = got.into_iter().flat_map(|c| c.objs).collect();
        assert_eq!(best_path(&objs, 0, None, Grain::Finest).reach, 5);
    }

    #[test]
    fn epoch_chain_object_straddling_cut_breaks_link() {
        // e1's only object reaching the cut runs past it: clipped away, the
        // rest ends at 10, not 20, so e1 is not the link (celld: same).
        let e1 = [o(1, 0, 1, 10), o(1, 1, 11, 30)];
        let e2 = [o(2, 0, 21, 25)];
        assert_eq!(chain(&[(1, &e1), (2, &e2)]), Some(vec![ce(2, 21, &e2)]));
        // The clipped set ends one short of the cut: not the link either.
        let e1 = [o(1, 0, 1, 19)];
        assert_eq!(chain(&[(1, &e1), (2, &e2)]), Some(vec![ce(2, 21, &e2)]));
        // Duplicate range at another level, ending exactly at the cut.
        let e1 = [o(1, 0, 1, 20), o(1, 1, 1, 20), o(1, 9, 1, 20)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2)]),
            Some(vec![ce(1, 1, &e1), ce(2, 21, &e2)])
        );
    }

    #[test]
    fn epoch_chain_duplicates_and_input_order() {
        let e1 = [
            o(1, 1, 1, 20),
            o(1, 0, 11, 20),
            o(1, 0, 1, 10),
            o(1, 0, 1, 10),
        ];
        assert_eq!(
            chain(&[(1, &e1)]),
            Some(vec![ce(
                1,
                1,
                &[o(1, 0, 1, 10), o(1, 0, 11, 20), o(1, 1, 1, 20)]
            )])
        );
    }

    #[test]
    fn epoch_chain_many_epochs_walk_back() {
        // 200 paged epochs of one TXID each over a snapshot epoch, with a
        // fenced late snapshot in every tenth epoch (skipped).
        let mut map = BTreeMap::new();
        map.insert(1, vec![o(1, 9, 1, 100)]);
        let mut want = vec![ce(1, 1, &[o(1, 9, 1, 100)])];
        let mut t = 100;
        let mut epoch = 1;
        for i in 0..200 {
            epoch += 1;
            if i % 10 == 5 {
                map.insert(epoch, vec![o(epoch, 9, 1, t - 1)]);
                epoch += 1;
            }
            t += 1;
            map.insert(epoch, vec![o(epoch, 0, t, t)]);
            want.push(ce(epoch, t, &[o(epoch, 0, t, t)]));
        }
        assert_eq!(epoch_chain(&map), Some(want));
    }

    #[test]
    fn epoch_chain_txid_extremes_do_not_panic() {
        // A link ending at u64::MAX - 1, continued at u64::MAX.
        let e1 = [o(1, 0, 1, u64::MAX - 1)];
        let e2 = [o(2, 0, u64::MAX, u64::MAX)];
        assert_eq!(
            chain(&[(1, &e1), (2, &e2)]),
            Some(vec![ce(1, 1, &e1), ce(2, u64::MAX, &e2)])
        );
        // An object starting at TXID 0 (celld never writes one): lo 0 has
        // no cut below it, so nothing below can link; no underflow.
        let e1 = [o(1, 0, 1, 5)];
        let e2 = [o(2, 0, 0, 7)];
        assert_eq!(chain(&[(1, &e1), (2, &e2)]), Some(vec![ce(2, 0, &e2)]));
        // Epoch numbers at both ends.
        let lo = [o(0, 0, 1, 5)];
        let hi = [o(u64::MAX, 0, 6, 9)];
        assert_eq!(
            chain(&[(0, &lo), (u64::MAX, &hi)]),
            Some(vec![ce(0, 1, &lo), ce(u64::MAX, 6, &hi)])
        );
    }

    #[test]
    fn best_path_finest_prefers_reach_over_steps() {
        // Three steps reach 6; four steps (the same three, then the fold)
        // reach 10. Reach always wins, then steps.
        let objs = [o(1, 0, 1, 2), o(1, 0, 3, 4), o(1, 0, 5, 6), o(1, 1, 1, 10)];
        assert_eq!(best_path(&objs, 0, None, Grain::Finest), path(&objs, 10));
        assert_eq!(best_path(&objs, 0, None, Grain::Fold), path(&objs[3..], 10));
        // A limit below the fold: the L0s only.
        assert_eq!(
            best_path(&objs, 0, Some(9), Grain::Finest),
            path(&objs[..3], 6)
        );
    }

    #[test]
    fn best_path_from_beyond_and_inside() {
        let objs = [o(1, 0, 1, 5), o(1, 1, 1, 256), o(1, 0, 6, 300)];
        // Beyond every object.
        assert_eq!(best_path(&objs, 1000, None, Grain::Finest), path(&[], 1000));
        // At an end: nothing ending there is reused.
        assert_eq!(
            best_path(&objs, 256, None, Grain::Finest),
            path(&objs[2..], 300)
        );
        // Inside the straddling L1 and the L0 6-300 alike: Finest takes both.
        assert_eq!(
            best_path(&objs, 100, None, Grain::Finest),
            path(&objs[1..], 300)
        );
        assert_eq!(
            best_path(&objs, 100, None, Grain::Fold),
            path(&objs[2..], 300)
        );
        // A limit at or below `from`: nothing to do.
        for limit in [0, 99, 100] {
            assert_eq!(
                best_path(&objs, 100, Some(limit), Grain::Finest),
                path(&[], 100)
            );
        }
    }

    #[test]
    fn best_path_txid_extremes() {
        let top = u64::MAX;
        let objs = [
            o(1, 0, 0, top),
            o(1, 0, top - 1, top - 1),
            o(1, 0, top, top),
        ];
        assert_eq!(best_path(&objs, top, None, Grain::Finest), path(&[], top));
        assert_eq!(
            best_path(&objs, top - 2, None, Grain::Finest),
            // Two steps either way from top-1; `0-top` is first in Obj order.
            path(&[o(1, 0, top - 1, top - 1), o(1, 0, 0, top)], top)
        );
        assert_eq!(
            best_path(&objs, top - 2, None, Grain::Fold),
            path(&[o(1, 0, 0, top)], top)
        );
        assert_eq!(
            best_path(&objs, 0, Some(top - 1), Grain::Fold),
            path(&[], 0)
        );
    }

    #[test]
    fn best_path_ignores_inverted_and_duplicate_objects() {
        let objs = [o(1, 0, 5, 1), o(1, 0, 1, 5), o(1, 0, 1, 5), o(1, 0, 6, 6)];
        assert_eq!(
            best_path(&objs, 0, None, Grain::Finest),
            path(&[o(1, 0, 1, 5), o(1, 0, 6, 6)], 6)
        );
    }

    #[test]
    fn best_path_equal_reach_across_epochs_and_levels() {
        // The same range in two epochs and two levels (only possible
        // outside a chain): the lowest epoch, then level, wins.
        let objs = [o(2, 0, 1, 9), o(1, 1, 1, 9), o(1, 2, 1, 9), o(2, 1, 1, 9)];
        for grain in [Grain::Finest, Grain::Fold] {
            assert_eq!(best_path(&objs, 0, None, grain), path(&[o(1, 1, 1, 9)], 9));
        }
    }

    #[test]
    fn best_path_large_history_is_fast_and_finest() {
        // 20 000 single-TXID L0s, an L1 per 256, a drain L0 over all of
        // them from 1, and recovery L0s duplicating every tenth L0.
        let n = 20_000u64;
        let mut objs: Vec<Obj> = (1..=n).map(|t| o(1, 0, t, t)).collect();
        objs.extend((0..n / 256).map(|i| o(1, 1, i * 256 + 1, (i + 1) * 256)));
        objs.push(o(1, 0, 1, n + 1));
        objs.extend((1..=n).step_by(10).map(|t| o(1, 2, t, t)));
        let t0 = std::time::Instant::now();
        let finest = best_path(&objs, 0, None, Grain::Finest);
        let fold = best_path(&objs, 0, None, Grain::Fold);
        assert!(t0.elapsed() < std::time::Duration::from_secs(10));
        assert_eq!(finest.reach, n + 1);
        assert_eq!(finest.steps.len() as u64, n + 1);
        assert_eq!(fold, path(&[o(1, 0, 1, n + 1)], n + 1));
    }
}
