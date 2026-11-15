// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Planning one cell (design §2.1-2.3): what to LIST, what to GET, and,
//! after the GETs and the `own.json` read, which objects are proven
//! history. PURE: no async, no I/O, no clocks.
//!
//! The executor loops: [`plan_cell`] → [`Decision::List`] (it lists and
//! records the results in [`Listed`]) → [`plan_cell`] … → [`Decision::Fetch`]
//! → GET the plan's objects → read `own.json` → [`prove`]. Every rule of
//! when and how to LIST lives here, not in the executor.

use std::collections::{BTreeMap, BTreeSet};

use super::chain::{best_path, epoch_chain, Grain, SNAPSHOT_LEVEL};
use super::obj::{Obj, Pos};
use crate::keys;

/// The levels celld writes in an epoch: L0 (uploads, drains, markers), L1
/// (folds) and L9 (handoff images). A **full listing** of an epoch is all
/// three from TXID 0; levels 2-8 are never written by celld.
pub const LEVELS: [u32; 3] = [0, 1, SNAPSHOT_LEVEL];

/// Whether the cell's pages at its cursor can be restored without celld.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateAvail {
    /// From the checkpoint: a snapshot plus its replay objects (or an
    /// in-memory state). The planner does not need the details.
    Present,
    /// Lost or never written: rebuild from celld's objects (§2.3).
    Missing,
}

/// One level of one epoch as listed this run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LevelList {
    /// `None`: listed from TXID 0 (full). `Some(s)`: only objects whose
    /// min is above `s` ([`start_after_key`]).
    pub start_after: Option<u64>,
    pub objs: Vec<Obj>,
}

/// The LIST results of this run for one cell, as the executor records them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Listed {
    /// The delimiter listing of `cells/<cell>/ltx/`: every epoch that has
    /// objects. `None` until listed.
    pub epochs: Option<BTreeSet<u64>>,
    pub levels: BTreeMap<(u64, u32), LevelList>,
}

impl Listed {
    /// Records a delimiter listing.
    pub fn add_epochs(&mut self, epochs: impl IntoIterator<Item = u64>) {
        self.epochs.get_or_insert_with(BTreeSet::new).extend(epochs);
    }

    /// Records a level listing. Objects of other epochs or levels, or at or
    /// below the start, are ignored; listings of the same level merge (the
    /// union covers from the lower start).
    pub fn add_level(
        &mut self,
        epoch: u64,
        level: u32,
        start_after: Option<u64>,
        objs: impl IntoIterator<Item = Obj>,
    ) {
        let new: Vec<Obj> = objs
            .into_iter()
            .filter(|o| o.epoch == epoch && o.level == level)
            .filter(|o| start_after.is_none_or(|s| o.min > s))
            .collect();
        let e = self
            .levels
            .entry((epoch, level))
            .or_insert_with(|| LevelList {
                start_after,
                objs: Vec::new(),
            });
        e.start_after = match (e.start_after, start_after) {
            (Some(a), Some(b)) => Some(a.min(b)),
            _ => None,
        };
        e.objs.extend(new);
        e.objs.sort();
        e.objs.dedup();
    }

    /// Records the result of one request (`epochs` for
    /// [`ListReq::EpochDirs`], `objs` for a level).
    pub fn record(&mut self, req: &ListReq, epochs: &[u64], objs: &[Obj]) {
        match *req {
            ListReq::EpochDirs => self.add_epochs(epochs.iter().copied()),
            ListReq::Level {
                epoch,
                level,
                start_after,
            } => self.add_level(epoch, level, start_after, objs.iter().copied()),
        }
    }

    /// Is a listing of `level` from `start_after` already in hand?
    pub fn covers(&self, epoch: u64, level: u32, start_after: Option<u64>) -> bool {
        match self.levels.get(&(epoch, level)) {
            None => false,
            Some(l) => match (l.start_after, start_after) {
                (None, _) => true,
                (Some(a), Some(b)) => a <= b,
                (Some(_), None) => false,
            },
        }
    }

    /// Every level celld writes is listed from TXID 0.
    pub fn is_full(&self, epoch: u64) -> bool {
        LEVELS.iter().all(|&l| self.covers(epoch, l, None))
    }

    fn objs(&self) -> impl Iterator<Item = &Obj> {
        self.levels.values().flat_map(|l| l.objs.iter())
    }
}

/// One LIST the planner asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ListReq {
    /// Delimiter LIST of `cells/<cell>/ltx/` (paginated): the epochs.
    EpochDirs,
    /// `cells/<cell>/ltx/e<epoch>/<level>/`, from TXID 0 (`None`) or after
    /// [`start_after_key`]`(cell, epoch, s)` (L0 only).
    Level {
        epoch: u64,
        level: u32,
        start_after: Option<u64>,
    },
}

/// One object of a plan, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Step {
    /// Apply the object's pages to the state at the previous step and emit
    /// the net change.
    Apply(Obj),
    /// A full image (min 1) that opens a snapshot epoch or bootstraps:
    /// diffed against the state at the last proven point (empty for a
    /// bootstrap, so its rows are `r`), emitted as one net change.
    Opener(Obj),
    /// A paged epoch's zero-page marker (proven by its header): moves the
    /// position into the new epoch, emits nothing.
    Marker(Obj),
}

impl Step {
    pub fn obj(&self) -> Obj {
        match *self {
            Step::Apply(o) | Step::Opener(o) | Step::Marker(o) => o,
        }
    }
}

/// Where the cell's pages come from before the first step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Base {
    /// The checkpoint's state at the cursor.
    Cursor,
    /// No saved state: rebuild the state at the cursor from these celld
    /// objects (a full image first), emitting nothing.
    Rebuild(Vec<Obj>),
    /// First sight: start empty; the first step is the newest full image of
    /// the owner's chain.
    Bootstrap,
    /// The cursor is not on the owner's chain (epoch GC, a cut below it,
    /// its epoch skipped, or its state cannot be rebuilt): the first step
    /// is the chain's newest full image, diffed against the restored state
    /// at the cursor (or `live_keys` without one). The only restatement.
    Resync,
}

/// What to GET and apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// The cursor the plan starts from.
    pub from: Option<Pos>,
    pub base: Base,
    pub steps: Vec<Step>,
    /// Objects GET only for their header: the proof needs to know whether
    /// they are markers (a cut below the cursor).
    pub probes: Vec<Obj>,
    /// Known objects past the cursor that are never applied: an unproven
    /// old-epoch tail collapsed into a snapshot opener.
    pub dropped: Vec<Obj>,
    /// The gap memo to keep if the proof reaches its position.
    pub gap: Option<GapMemo>,
    /// The highest L0 end (or marker) at or below the cursor, carried.
    pub last_l0: Option<Pos>,
    /// Every object the planner knew of (notified and listed): the proof
    /// walks the same knowledge.
    pub known: Vec<Obj>,
}

/// Why a cell stops for now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkReason {
    /// The same gap stayed open after `CDCELL_GAP_LISTS` runs of LISTs (or
    /// the chain has no full image at all). Resumes on a later epoch's
    /// opener, or when the gap's object is notified.
    Hole,
    /// The owner's epoch has nothing that proves the old epoch's tail (it
    /// has not written yet), or `own.json` is unreadable. Resumes when the
    /// owner writes.
    AwaitingOpener,
    /// The proof needed a cut that the notified keys and fetched headers do
    /// not give: replan with full epoch listings (no candidate markers).
    Unproven,
}

/// A gap that LISTs did not close: where the path stops, and in how many
/// runs it was listed for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GapMemo {
    pub at: Pos,
    pub lists: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Nothing past the cursor.
    Idle,
    /// LIST these, record them in [`Listed`], and plan again.
    List(Vec<ListReq>),
    Fetch(Plan),
    Park(ParkReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanSettings {
    /// `CDCELL_GRAIN`.
    pub grain: Grain,
    /// `CDCELL_GAP_LISTS`: runs of LISTs for the same gap before parking.
    pub gap_lists: u32,
}

impl Default for PlanSettings {
    fn default() -> Self {
        PlanSettings {
            grain: Grain::Finest,
            gap_lists: 3,
        }
    }
}

/// Everything the planner knows about one cell this run.
#[derive(Clone, Copy, Debug)]
pub struct CellView<'a> {
    /// The committed `(epoch, txid)`; `None` on first sight.
    pub cursor: Option<Pos>,
    pub state: StateAvail,
    /// Keys from the broker and the pending set, any order, duplicates.
    pub notified: &'a [Obj],
    pub listed: &'a Listed,
    pub gap: Option<GapMemo>,
    pub parked: Option<ParkReason>,
    /// The highest L0 end (or marker) known at or below the cursor, in the
    /// cursor's epoch (checkpoint): the only safe `StartAfter` besides a
    /// listed L0 (spike pr0 §1).
    pub last_l0: Option<Pos>,
    /// `own.json`'s epoch from an earlier proof this run: only steers what
    /// to LIST, never proves.
    pub own_hint: Option<u64>,
    /// An earlier proof this run found the cursor off the owner's chain.
    pub resync: bool,
}

/// The facts of a fetched object's LTX header the proof needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fetched {
    pub obj: Obj,
    /// celld's paged marker: one TXID above 1, zero pages, `wal_size == 0`
    /// (`cdcell_core::ltx::Unit::is_paged_marker`). No capture matches it
    /// (spike pr0 §2), so it identifies a cut by content.
    pub is_marker: bool,
}

impl Fetched {
    /// From the header fields (`pages` = page count).
    pub fn from_header(obj: Obj, pages: usize, wal_size: i64) -> Fetched {
        Fetched {
            obj,
            is_marker: obj.min == obj.max && obj.min > 1 && pages == 0 && wal_size == 0,
        }
    }

    /// From a decoded unit; its TXIDs must match the key.
    pub fn from_unit(obj: Obj, unit: &cdcell_core::ltx::Unit) -> Fetched {
        Fetched {
            obj,
            is_marker: unit.is_paged_marker()
                && unit.min_txid == obj.min
                && unit.max_txid == obj.max,
        }
    }
}

/// What the proof confirmed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Proven {
    /// The plan's steps that are history, in order (markers relabelled).
    pub steps: Vec<Step>,
    /// The new cursor (the plan's `from` when nothing is proven).
    pub head: Option<Pos>,
    /// Steps skipped because a proven opener follows them.
    pub dropped: Vec<Obj>,
    pub parked: Option<ParkReason>,
    /// The cursor is off the owner's chain: replan with `resync`.
    pub resync: bool,
    /// The gap memo to keep (`None` clears it).
    pub gap: Option<GapMemo>,
    pub last_l0: Option<Pos>,
}

/// Decides the next action for one cell.
///
/// LISTs only when the known keys do not give an unbroken, provable chain
/// from the cursor (design §2.1): a gap, a first sight without a full
/// image, a lost state, an epoch change whose link the keys do not show,
/// or a cursor off the owner's chain. Each `List` asks only for listings
/// not yet in [`CellView::listed`], so the executor's loop ends.
pub fn plan_cell(v: &CellView, s: &PlanSettings) -> Decision {
    // After a failed proof, a single-TXID key is no longer trusted as a
    // marker candidate: only full listings give a cut.
    let markers = if v.parked == Some(ParkReason::Unproven) {
        Markers::Untrusted
    } else {
        Markers::Candidates
    };
    let pl = Planner {
        v,
        s,
        know: Know::new(v.notified.iter().copied(), v.listed, markers),
    };
    match v.cursor {
        None => pl.chain_from_zero(Base::Bootstrap),
        Some(_) if v.resync => pl.chain_from_zero(Base::Resync),
        Some(c) => pl.incremental(c),
    }
}

/// Which of the plan's steps are history, given the headers of the fetched
/// objects and `own.json`'s epoch read after the GETs (D5, D6).
///
/// An object of epoch `E'` is history if `own == E'`, or if the next epoch
/// on the owner's chain proves a cut `c` and the object ends at or below
/// it. A cut is proven by content only: that epoch's lowest object fetched
/// as a marker, or its lowest TXID from a full listing. An epoch opening at
/// TXID 1 proves no cut: steps before its opener are dropped and the opener
/// is diffed. Intermediate epochs must be known (epoch listing, full
/// listings) to be skipped. Steps stop at the first one that is not proven.
pub fn prove(
    plan: &Plan,
    fetched: &[Fetched],
    listed: &Listed,
    own_epoch: Option<u64>,
    s: &PlanSettings,
) -> Proven {
    let mut out = Proven {
        head: plan.from,
        last_l0: plan.last_l0,
        ..Proven::default()
    };
    if let (Base::Rebuild(objs), Some(c)) = (&plan.base, plan.from) {
        out.last_l0 = objs
            .iter()
            .filter(|o| o.level == 0 && o.epoch == c.epoch)
            .map(|o| Pos {
                epoch: o.epoch,
                txid: o.max,
            })
            .chain(out.last_l0)
            .max();
    }
    let work = !plan.steps.is_empty() || !plan.probes.is_empty();
    let floor = match plan.base {
        Base::Bootstrap | Base::Resync => None,
        _ => plan.from.map(|p| p.epoch),
    };
    let Some(own) = own_epoch.filter(|&own| floor.is_none_or(|f| own >= f)) else {
        // No owner record (or one behind the cursor): nothing is proven.
        if work {
            out.parked = Some(ParkReason::AwaitingOpener);
        }
        return out;
    };
    let marks: BTreeSet<Obj> = fetched
        .iter()
        .filter(|f| f.is_marker)
        .map(|f| f.obj)
        .collect();
    let know = Know::new(
        plan.known
            .iter()
            .copied()
            .chain(plan.steps.iter().map(Step::obj))
            .chain(plan.probes.iter().copied())
            .chain(fetched.iter().map(|f| f.obj)),
        listed,
        Markers::Fetched(&marks),
    );
    let w = know.walk(own, floor);
    if let (Some(c), Some(_)) = (plan.from, floor) {
        let off = match w.stop {
            // The cursor's epoch is skipped by, or absent from, the chain.
            Stop::Passed | Stop::Bottom => true,
            Stop::Floor => w
                .links
                .last()
                .and_then(|l| l.cut)
                .is_some_and(|cut| cut < c.txid),
            _ => false,
        };
        if off {
            out.resync = true;
            return out;
        }
    }

    let mut pos = match plan.base {
        Base::Bootstrap | Base::Resync => None,
        _ => plan.from,
    };
    let steps = &plan.steps;
    let mut opened = false;
    let mut i = 0;
    while i < steps.len() {
        let st = steps[i];
        let o = st.obj();
        let ok = match st {
            Step::Opener(_) => w.opens(&o, pos),
            _ => w.applies(&o, pos),
        };
        if ok {
            let st = match st {
                Step::Apply(o) if marks.contains(&o) && w.lo(o.epoch) == Some(o.min) => {
                    Step::Marker(o)
                }
                st => st,
            };
            opened |= matches!(st, Step::Opener(_));
            out.steps.push(st);
            let at = Pos {
                epoch: o.epoch,
                txid: o.max,
            };
            if o.level == 0 {
                out.last_l0 = Some(at);
            }
            pos = Some(at);
            i += 1;
            continue;
        }
        // An unproven old-epoch tail before a proven snapshot opener
        // collapses into the opener's diff.
        let next = (i + 1..steps.len())
            .find(|&j| matches!(steps[j], Step::Opener(_)) && w.opens(&steps[j].obj(), pos));
        match next {
            Some(j) => {
                out.dropped.extend(steps[i..j].iter().map(Step::obj));
                i = j;
            }
            None => break,
        }
    }
    if !out.steps.is_empty() {
        out.head = pos;
    }
    if opened {
        out.dropped.extend(plan.dropped.iter().copied());
        out.dropped.sort();
        out.dropped.dedup();
    }
    if i < steps.len() || !plan.probes.is_empty() {
        // A probe that proved no cut below the cursor: replan with full
        // listings.
        out.parked = Some(match w.stop {
            Stop::EmptyTop => ParkReason::AwaitingOpener,
            _ => ParkReason::Unproven,
        });
    }
    if let Some(g) = plan.gap.filter(|g| out.head == Some(g.at)) {
        out.gap = Some(g);
        if g.lists >= s.gap_lists && out.parked.is_none() {
            out.parked = Some(ParkReason::Hole);
        }
    }
    out
}

/// The `StartAfter` key for an L0 listing of `epoch` that skips every
/// object whose min is at or below `boundary`: keys sort by min (B1), and
/// no max sorts above `ffffffffffffffff`.
pub fn start_after_key(cell: &str, epoch: u64, boundary: u64) -> String {
    keys::ltx_key(cell, epoch, 0, boundary, u64::MAX)
}

// ---- What is known about a cell ----

/// How a single-TXID L0 that is its epoch's lowest known object counts.
#[derive(Clone, Copy)]
enum Markers<'a> {
    /// Planning: it may be a marker (the proof reads its header).
    Candidates,
    /// Planning after a failed proof: only full listings give a cut.
    Untrusted,
    /// Proving: the fetched objects whose header is a marker.
    Fetched(&'a BTreeSet<Obj>),
}

/// The known objects of a cell (notified, listed, fetched) by epoch.
struct Know<'a> {
    by_epoch: BTreeMap<u64, Vec<Obj>>,
    listed: &'a Listed,
    markers: Markers<'a>,
}

/// One epoch on the owner's chain, as far as it is proven: objects ending
/// at or below `cut` (any, for the top) are history; `lo` is its first
/// TXID when known (not computed for the cursor's epoch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Link {
    epoch: u64,
    cut: Option<u64>,
    lo: Option<u64>,
}

/// A listing that would let the walk continue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Need {
    Dirs,
    Epoch(u64),
}

/// Why a walk ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// At an epoch opening at TXID 1 (snapshot epoch or L9): no cut below.
    Opener,
    /// At the cursor's epoch, linked.
    Floor,
    /// Below the cursor's epoch without linking it.
    Passed,
    /// No lower epoch exists.
    Bottom,
    /// The top epoch is known to have no objects (celld skips it, but it
    /// proves nothing below).
    EmptyTop,
    Need(Need),
}

/// The owner's chain from the top down (celld's `EpochChain` rules over
/// partial knowledge).
struct Walk {
    links: Vec<Link>,
    stop: Stop,
}

impl Walk {
    fn link(&self, epoch: u64) -> Option<(usize, &Link)> {
        self.links
            .iter()
            .enumerate()
            .find(|(_, l)| l.epoch == epoch)
    }

    fn lo(&self, epoch: u64) -> Option<u64> {
        self.link(epoch).and_then(|(_, l)| l.lo)
    }

    fn proves(&self, o: &Obj) -> bool {
        self.link(o.epoch)
            .is_some_and(|(_, l)| l.cut.is_none_or(|c| o.max <= c))
    }

    /// A full image opening its epoch after `pos`'s.
    fn opens(&self, o: &Obj, pos: Option<Pos>) -> bool {
        o.min <= 1
            && self.proves(o)
            && self.lo(o.epoch) == Some(1)
            && pos.is_none_or(|p| o.epoch > p.epoch)
    }

    /// `o` applies at `pos`: in the same epoch, or in the next link up
    /// when `pos` is exactly at its epoch's cut.
    fn applies(&self, o: &Obj, pos: Option<Pos>) -> bool {
        let Some(p) = pos else { return false };
        if !self.proves(o) || !(o.min <= p.txid.saturating_add(1) && p.txid < o.max) {
            return false;
        }
        if o.epoch == p.epoch {
            return true;
        }
        match (self.link(p.epoch), self.link(o.epoch)) {
            (Some((i, below)), Some((j, _))) => below.cut == Some(p.txid) && j + 1 == i,
            _ => false,
        }
    }
}

impl<'a> Know<'a> {
    fn new(objs: impl IntoIterator<Item = Obj>, listed: &'a Listed, markers: Markers<'a>) -> Self {
        let mut by_epoch: BTreeMap<u64, Vec<Obj>> = BTreeMap::new();
        for o in objs.into_iter().chain(listed.objs().copied()) {
            // celld lists levels 0..=9 only.
            if o.level <= SNAPSHOT_LEVEL && o.min <= o.max {
                by_epoch.entry(o.epoch).or_default().push(o);
            }
        }
        for objs in by_epoch.values_mut() {
            objs.sort();
            objs.dedup();
        }
        Know {
            by_epoch,
            listed,
            markers,
        }
    }

    fn objs(&self, epoch: u64) -> &[Obj] {
        self.by_epoch.get(&epoch).map_or(&[], Vec::as_slice)
    }

    fn all(&self) -> impl Iterator<Item = &Obj> {
        self.by_epoch.values().flatten()
    }

    fn is_marker(&self, o: &Obj) -> bool {
        o.level == 0
            && o.min == o.max
            && o.min > 1
            && match self.markers {
                Markers::Candidates => true,
                Markers::Untrusted => false,
                Markers::Fetched(m) => m.contains(o),
            }
    }

    /// Not in the epoch listing and no object known.
    fn absent(&self, epoch: u64) -> bool {
        self.objs(epoch).is_empty()
            && self
                .listed
                .epochs
                .as_ref()
                .is_some_and(|s| !s.contains(&epoch))
    }

    /// The newest epoch known to exist or claimed.
    fn top(&self, own_hint: Option<u64>, cursor: Option<Pos>) -> Option<u64> {
        let dirs = self.listed.epochs.as_ref().and_then(|s| s.last().copied());
        [
            self.by_epoch.keys().next_back().copied(),
            dirs,
            own_hint,
            cursor.map(|c| c.epoch),
        ]
        .into_iter()
        .flatten()
        .max()
    }

    /// An epoch's first TXID, when proven: an object at TXID 1, a full
    /// listing, or a marker as its lowest known object (never a higher one).
    fn lo(&self, epoch: u64, objs: &[Obj]) -> Option<u64> {
        let min = objs.iter().map(|o| o.min).min()?;
        if min <= 1 || self.listed.is_full(epoch) {
            return Some(min);
        }
        objs.iter()
            .any(|o| o.min == min && self.is_marker(o))
            .then_some(min)
    }

    /// The next lower epoch that may exist below `cur`, if the epochs in
    /// between are known not to.
    fn below(&self, cur: u64, floor: Option<u64>) -> Result<Option<u64>, Need> {
        let dirs = self.listed.epochs.as_ref();
        let mut cands: BTreeSet<u64> = self.by_epoch.keys().copied().collect();
        cands.extend(dirs.into_iter().flatten());
        if let Some(f) = floor.filter(|&f| !self.absent(f)) {
            cands.insert(f);
        }
        let next = cands.range(..cur).next_back().copied();
        if dirs.is_some() || cur == 0 || (next.is_some() && next == cur.checked_sub(1)) {
            Ok(next)
        } else {
            Err(Need::Dirs)
        }
    }

    /// Walks the owner's chain down from `top` (celld's `EpochChain`, over
    /// what is known). With a `floor` (the cursor's epoch), stops there; the
    /// cursor's epoch is assumed to link unless a listing shows otherwise
    /// (the cursor was proven on the chain). Without one, walks to an
    /// opener.
    fn walk(&self, top: u64, floor: Option<u64>) -> Walk {
        let mut links = Vec::new();
        let mut cur = top;
        let mut cut: Option<u64> = None;
        loop {
            let objs: Vec<Obj> = self
                .objs(cur)
                .iter()
                .filter(|o| cut.is_none_or(|c| o.max <= c))
                .copied()
                .collect();
            let full = self.listed.is_full(cur);
            let is_floor = floor == Some(cur);
            let linked = if objs.is_empty() {
                let nothing = full || self.absent(cur);
                if cut.is_none() && !is_floor {
                    let stop = if nothing {
                        Stop::EmptyTop
                    } else {
                        Stop::Need(Need::Epoch(cur))
                    };
                    return Walk { links, stop };
                }
                if nothing {
                    false
                } else if is_floor {
                    true
                } else {
                    return Walk {
                        links,
                        stop: Stop::Need(Need::Epoch(cur)),
                    };
                }
            } else if let Some(c) = cut {
                // The link must end exactly at the cut; a listed epoch that
                // does not is skipped (a fenced owner's late snapshot).
                let end = objs.iter().map(|o| o.max).max();
                if end == Some(c) {
                    true
                } else if full {
                    false
                } else if is_floor {
                    true
                } else {
                    return Walk {
                        links,
                        stop: Stop::Need(Need::Epoch(cur)),
                    };
                }
            } else {
                true
            };
            if linked {
                if is_floor {
                    links.push(Link {
                        epoch: cur,
                        cut,
                        lo: None,
                    });
                    return Walk {
                        links,
                        stop: Stop::Floor,
                    };
                }
                let lo = self.lo(cur, &objs);
                links.push(Link {
                    epoch: cur,
                    cut,
                    lo,
                });
                match lo {
                    None => {
                        return Walk {
                            links,
                            stop: Stop::Need(Need::Epoch(cur)),
                        }
                    }
                    Some(l) if l <= 1 => {
                        return Walk {
                            links,
                            stop: Stop::Opener,
                        }
                    }
                    Some(l) => cut = Some(l - 1),
                }
            }
            match self.below(cur, floor) {
                Err(need) => {
                    return Walk {
                        links,
                        stop: Stop::Need(need),
                    }
                }
                Ok(Some(e)) if floor.is_none_or(|f| e >= f) => cur = e,
                Ok(_) => {
                    let stop = if floor.is_some() {
                        Stop::Passed
                    } else {
                        Stop::Bottom
                    };
                    return Walk { links, stop };
                }
            }
        }
    }
}

// ---- Planning ----

struct Planner<'a, 'b> {
    v: &'b CellView<'a>,
    s: &'b PlanSettings,
    know: Know<'a>,
}

/// How the first link's path starts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum First {
    /// From the cursor.
    Cursor,
    /// From TXID 0, the first step an opener (a snapshot epoch above the
    /// cursor: finest from its first image).
    OpenerFinest,
    /// From the newest full image (bootstrap, resync).
    OpenerNewest,
}

enum Rebuilt {
    List(Vec<ListReq>),
    Objs(Vec<Obj>),
    Gone,
}

fn full_reqs(listed: &Listed, epoch: u64) -> Vec<ListReq> {
    LEVELS
        .iter()
        .filter(|&&level| !listed.covers(epoch, level, None))
        .map(|&level| ListReq::Level {
            epoch,
            level,
            start_after: None,
        })
        .collect()
}

/// The newest full image (min 1) among `objs` ending at or below `limit`.
fn newest_image(objs: &[Obj], limit: Option<u64>) -> Option<Obj> {
    objs.iter()
        .filter(|o| o.min <= 1 && limit.is_none_or(|l| o.max <= l))
        .max_by(|a, b| a.max.cmp(&b.max).then(b.cmp(a)))
        .copied()
}

impl Planner<'_, '_> {
    fn plan(&self, base: Base) -> Plan {
        Plan {
            from: self.v.cursor,
            base,
            steps: Vec::new(),
            probes: Vec::new(),
            dropped: Vec::new(),
            gap: None,
            last_l0: self.v.last_l0,
            known: self.know.all().copied().collect(),
        }
    }

    fn incremental(&self, c: Pos) -> Decision {
        let after = |o: &Obj| o.epoch > c.epoch || (o.epoch == c.epoch && o.max > c.txid);
        if !self.know.all().any(after) {
            return Decision::Idle;
        }
        let base = match self.v.state {
            StateAvail::Present => Base::Cursor,
            StateAvail::Missing => match self.rebuild(c) {
                Rebuilt::List(reqs) => return Decision::List(reqs),
                Rebuilt::Objs(objs) => Base::Rebuild(objs),
                Rebuilt::Gone => return self.chain_from_zero(Base::Resync),
            },
        };
        let top = self
            .know
            .top(self.v.own_hint, Some(c))
            .expect("the cursor's epoch");
        let w = self.know.walk(top, Some(c.epoch));
        let mut plan = self.plan(base);
        let mut up = w.links;
        up.reverse();
        let first = match w.stop {
            Stop::Floor => {
                if up[0].cut.is_some_and(|cut| cut < c.txid) {
                    // A cut below the cursor: only the proof can tell (the
                    // marker's header, or a full listing).
                    let above = up[1];
                    if !self.v.listed.is_full(above.epoch) {
                        plan.probes = self
                            .know
                            .objs(above.epoch)
                            .iter()
                            .filter(|o| Some(o.min) == above.lo)
                            .copied()
                            .collect();
                    }
                    return Decision::Fetch(plan);
                }
                First::Cursor
            }
            Stop::Opener => {
                plan.dropped = self
                    .know
                    .all()
                    .filter(|o| after(o) && o.epoch < up[0].epoch)
                    .copied()
                    .collect();
                First::OpenerFinest
            }
            Stop::Passed | Stop::Bottom => return self.chain_from_zero(Base::Resync),
            Stop::EmptyTop => return Decision::Park(ParkReason::AwaitingOpener),
            Stop::Need(_) => return self.epoch_walk(c.epoch, top),
        };
        let (steps, gap) = self.path(&up, Some(c), first);
        plan.steps = steps;
        self.finish(plan, gap)
    }

    /// Bootstrap or resync: the owner's chain down to its newest full image.
    fn chain_from_zero(&self, base: Base) -> Decision {
        let Some(top) = self.know.top(self.v.own_hint, self.v.cursor) else {
            return Decision::Idle;
        };
        let w = self.know.walk(top, None);
        match w.stop {
            Stop::Opener => {}
            Stop::EmptyTop => return Decision::Park(ParkReason::AwaitingOpener),
            Stop::Need(need) => {
                let mut reqs = Vec::new();
                if self.v.listed.epochs.is_none() {
                    reqs.push(ListReq::EpochDirs);
                }
                if let Need::Epoch(e) = need {
                    reqs.extend(full_reqs(self.v.listed, e));
                }
                return if reqs.is_empty() {
                    Decision::Park(ParkReason::Unproven)
                } else {
                    Decision::List(reqs)
                };
            }
            // No full image anywhere on the chain: nothing to start from.
            Stop::Bottom | Stop::Floor | Stop::Passed => return Decision::Park(ParkReason::Hole),
        }
        let mut up = w.links;
        up.reverse();
        let mut plan = self.plan(base);
        let (steps, gap) = self.path(&up, None, First::OpenerNewest);
        plan.steps = steps;
        self.finish(plan, gap)
    }

    /// Lists for an epoch change: the epochs, then every epoch between the
    /// cursor's and the top in full (from TXID 0).
    fn epoch_walk(&self, floor: u64, top: u64) -> Decision {
        let listed = self.v.listed;
        let mut reqs = Vec::new();
        if listed.epochs.is_none() {
            reqs.push(ListReq::EpochDirs);
        }
        let mut epochs: BTreeSet<u64> = self.know.by_epoch.keys().copied().collect();
        epochs.extend(listed.epochs.iter().flatten());
        epochs.extend(self.v.own_hint);
        for &e in epochs.range(floor + 1..=top) {
            if !self.know.absent(e) {
                reqs.extend(full_reqs(listed, e));
            }
        }
        if reqs.is_empty() {
            Decision::Park(ParkReason::Unproven)
        } else {
            Decision::List(reqs)
        }
    }

    /// The steps along the links (oldest first), and where a gap stops them.
    fn path(&self, up: &[Link], cursor: Option<Pos>, first: First) -> (Vec<Step>, Option<Pos>) {
        let mut steps = Vec::new();
        let mut pos = cursor;
        for (i, l) in up.iter().enumerate() {
            let e = l.epoch;
            let objs: Vec<Obj> = self
                .know
                .objs(e)
                .iter()
                .filter(|o| pos.is_none_or(|p| e > p.epoch || o.max > p.txid))
                .copied()
                .collect();
            let from = match (i, first, pos) {
                (0, First::OpenerFinest, _) => 0,
                (0, First::OpenerNewest, _) => match newest_image(&objs, l.cut) {
                    Some(f) => {
                        steps.push(Step::Opener(f));
                        f.max
                    }
                    None => return (steps, Some(Pos { epoch: e, txid: 0 })),
                },
                (_, _, Some(p)) => p.txid,
                (_, _, None) => 0,
            };
            let p = best_path(&objs, from, l.cut, self.s.grain);
            for (k, &o) in p.steps.iter().enumerate() {
                steps.push(if i == 0 && k == 0 && first == First::OpenerFinest {
                    Step::Opener(o)
                } else {
                    Step::Apply(o)
                });
            }
            let at = Pos {
                epoch: e,
                txid: p.reach,
            };
            pos = Some(at);
            // A link ends exactly at its cut; the top has no cut, but an
            // object past where the path stops means one is missing.
            let gap = match l.cut {
                Some(c) => p.reach < c,
                None => objs.iter().any(|o| o.min > p.reach.saturating_add(1)),
            };
            if gap {
                return (steps, Some(at));
            }
        }
        (steps, None)
    }

    /// Gap handling: LIST until the run's rounds are spent, then plan to
    /// the gap and count the run; park once `gap_lists` runs failed.
    fn finish(&self, mut plan: Plan, gap: Option<Pos>) -> Decision {
        let Some(at) = gap else {
            if plan.steps.is_empty() && plan.probes.is_empty() {
                return Decision::Idle;
            }
            return Decision::Fetch(plan);
        };
        let prev = self.v.gap.filter(|m| m.at == at);
        if prev.is_some_and(|m| m.lists >= self.s.gap_lists) {
            if plan.steps.is_empty() {
                return Decision::Park(ParkReason::Hole);
            }
            plan.gap = prev;
            return Decision::Fetch(plan);
        }
        let reqs = self.gap_reqs(at);
        if !reqs.is_empty() {
            return Decision::List(reqs);
        }
        plan.gap = Some(GapMemo {
            at,
            lists: prev.map_or(1, |m| m.lists + 1),
        });
        Decision::Fetch(plan)
    }

    /// The gap LISTs for `at.epoch` (design §2.1): L1 and L9 in full; L0
    /// after the highest known L0 end at or below the gap (never an L1 or
    /// L9 end), from 0 if none; then L0 in full.
    fn gap_reqs(&self, at: Pos) -> Vec<ListReq> {
        let listed = self.v.listed;
        let e = at.epoch;
        let mut reqs: Vec<ListReq> = [1, SNAPSHOT_LEVEL]
            .into_iter()
            .filter(|&level| !listed.covers(e, level, None))
            .map(|level| ListReq::Level {
                epoch: e,
                level,
                start_after: None,
            })
            .collect();
        match listed.levels.get(&(e, 0)) {
            None => {
                let start_after = self
                    .know
                    .objs(e)
                    .iter()
                    .filter(|o| o.level == 0 && o.max <= at.txid)
                    .map(|o| o.max)
                    .chain(
                        self.v
                            .last_l0
                            .filter(|p| p.epoch == e && p.txid <= at.txid)
                            .map(|p| p.txid),
                    )
                    .max();
                reqs.push(ListReq::Level {
                    epoch: e,
                    level: 0,
                    start_after,
                });
            }
            Some(l) if l.start_after.is_some() => reqs.push(ListReq::Level {
                epoch: e,
                level: 0,
                start_after: None,
            }),
            Some(_) => {}
        }
        reqs
    }

    /// The objects that rebuild the state at the cursor: the cursor's epoch
    /// in full, then epochs down (one per round) until celld's chain below
    /// the cursor reaches a full image.
    fn rebuild(&self, c: Pos) -> Rebuilt {
        let listed = self.v.listed;
        let reqs = full_reqs(listed, c.epoch);
        if !reqs.is_empty() {
            return Rebuilt::List(reqs);
        }
        let mut by: BTreeMap<u64, Vec<Obj>> = BTreeMap::new();
        by.insert(
            c.epoch,
            self.know
                .objs(c.epoch)
                .iter()
                .filter(|o| o.max <= c.txid)
                .copied()
                .collect(),
        );
        let mut low = c.epoch;
        loop {
            if let Some(chain) = epoch_chain(&by).filter(|ch| ch[0].lo <= 1) {
                return self.rebuild_path(&chain, c);
            }
            let Some(dirs) = listed.epochs.as_ref() else {
                return Rebuilt::List(vec![ListReq::EpochDirs]);
            };
            let mut cands: BTreeSet<u64> = dirs.clone();
            cands.extend(self.know.by_epoch.keys());
            let Some(&next) = cands.range(..low).next_back() else {
                return Rebuilt::Gone;
            };
            let reqs = full_reqs(listed, next);
            if !reqs.is_empty() {
                return Rebuilt::List(reqs);
            }
            by.insert(next, self.know.objs(next).to_vec());
            low = next;
        }
    }

    fn rebuild_path(&self, chain: &[super::chain::ChainEpoch], c: Pos) -> Rebuilt {
        if chain.last().map(|ce| ce.epoch) != Some(c.epoch) {
            return Rebuilt::Gone;
        }
        let limit = |i: usize| chain.get(i + 1).map_or(c.txid, |next| next.lo - 1);
        let Some(f) = newest_image(&chain[0].objs, Some(limit(0))) else {
            return Rebuilt::Gone;
        };
        let mut objs = vec![f];
        let mut at = f.max;
        for (i, ce) in chain.iter().enumerate() {
            let p = best_path(&ce.objs, at, Some(limit(i)), Grain::Fold);
            objs.extend(p.steps);
            at = p.reach;
            if at != limit(i) {
                return Rebuilt::Gone;
            }
        }
        Rebuilt::Objs(objs)
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

    fn l0(epoch: u64, min: u64, max: u64) -> Obj {
        o(epoch, 0, min, max)
    }

    fn p(epoch: u64, txid: u64) -> Pos {
        Pos { epoch, txid }
    }

    /// An owned [`CellView`].
    #[derive(Clone, Debug)]
    struct V {
        cursor: Option<Pos>,
        state: StateAvail,
        notified: Vec<Obj>,
        listed: Listed,
        gap: Option<GapMemo>,
        parked: Option<ParkReason>,
        last_l0: Option<Pos>,
        own_hint: Option<u64>,
        resync: bool,
    }

    fn v(cursor: Option<Pos>, notified: &[Obj]) -> V {
        V {
            cursor,
            state: StateAvail::Present,
            notified: notified.to_vec(),
            listed: Listed::default(),
            gap: None,
            parked: None,
            last_l0: None,
            own_hint: None,
            resync: false,
        }
    }

    impl V {
        fn view(&self) -> CellView<'_> {
            CellView {
                cursor: self.cursor,
                state: self.state,
                notified: &self.notified,
                listed: &self.listed,
                gap: self.gap,
                parked: self.parked,
                last_l0: self.last_l0,
                own_hint: self.own_hint,
                resync: self.resync,
            }
        }

        fn plan(&self) -> Decision {
            plan_cell(&self.view(), &PlanSettings::default())
        }

        fn plan_with(&self, s: PlanSettings) -> Decision {
            plan_cell(&self.view(), &s)
        }

        /// Answers LISTs from `truth` until the planner stops listing.
        fn drive(&mut self, truth: &[Obj]) -> Decision {
            for _ in 0..16 {
                match self.plan() {
                    Decision::List(reqs) => answer(&mut self.listed, truth, &reqs),
                    d => return d,
                }
            }
            panic!("planner kept listing");
        }
    }

    fn answer(listed: &mut Listed, truth: &[Obj], reqs: &[ListReq]) {
        let epochs: Vec<u64> = truth.iter().map(|o| o.epoch).collect();
        for r in reqs {
            listed.record(r, &epochs, truth);
        }
    }

    fn fetch(d: Decision) -> Plan {
        match d {
            Decision::Fetch(plan) => plan,
            d => panic!("expected Fetch, got {d:?}"),
        }
    }

    fn lists(d: Decision) -> BTreeSet<ListReq> {
        match d {
            Decision::List(reqs) => reqs.into_iter().collect(),
            d => panic!("expected List, got {d:?}"),
        }
    }

    fn full(epoch: u64) -> Vec<ListReq> {
        LEVELS
            .iter()
            .map(|&level| ListReq::Level {
                epoch,
                level,
                start_after: None,
            })
            .collect()
    }

    fn lv(epoch: u64, level: u32, start_after: Option<u64>) -> ListReq {
        ListReq::Level {
            epoch,
            level,
            start_after,
        }
    }

    fn set(reqs: impl IntoIterator<Item = ListReq>) -> BTreeSet<ListReq> {
        reqs.into_iter().collect()
    }

    fn applies(objs: &[Obj]) -> Vec<Step> {
        objs.iter().map(|&o| Step::Apply(o)).collect()
    }

    fn markers(objs: &[Obj]) -> Vec<Fetched> {
        objs.iter()
            .map(|&obj| Fetched::from_header(obj, 0, 0))
            .collect()
    }

    fn data(objs: &[Obj]) -> Vec<Fetched> {
        objs.iter()
            .map(|&obj| Fetched::from_header(obj, 3, 3 * 4120))
            .collect()
    }

    fn prove1(plan: &Plan, fetched: &[Fetched], listed: &Listed, own: Option<u64>) -> Proven {
        prove(plan, fetched, listed, own, &PlanSettings::default())
    }

    /// A hand-made plan from `from` over `steps`, knowing `known` too.
    fn hand(from: Option<Pos>, steps: Vec<Step>, known: &[Obj]) -> Plan {
        let mut all: Vec<Obj> = known.to_vec();
        all.extend(steps.iter().map(Step::obj));
        all.sort();
        all.dedup();
        Plan {
            from,
            base: Base::Cursor,
            steps,
            probes: Vec::new(),
            dropped: Vec::new(),
            gap: None,
            last_l0: None,
            known: all,
        }
    }

    // ---- Notifications: order, duplicates, late keys ----

    #[test]
    fn contiguous_notified_keys_advance_without_list() {
        let a = l0(1, 11, 20);
        let b = l0(1, 21, 30);
        let plan = fetch(v(Some(p(1, 10)), &[a, b]).plan());
        assert_eq!(plan.base, Base::Cursor);
        assert_eq!(plan.steps, applies(&[a, b]));
        assert_eq!(plan.gap, None);
        let got = prove1(&plan, &data(&[a, b]), &Listed::default(), Some(1));
        assert_eq!(got.steps, applies(&[a, b]));
        assert_eq!(got.head, Some(p(1, 30)));
        assert_eq!(got.parked, None);
        assert_eq!(got.last_l0, Some(p(1, 30)));
    }

    #[test]
    fn out_of_order_keys_sorted_before_planning() {
        let a = l0(1, 11, 20);
        let b = l0(1, 21, 30);
        let c = l0(1, 31, 31);
        let plan = fetch(v(Some(p(1, 10)), &[c, b, a]).plan());
        assert_eq!(plan.steps, applies(&[a, b, c]));
    }

    #[test]
    fn duplicate_keys_deduped() {
        let a = l0(1, 11, 20);
        let b = l0(1, 21, 30);
        let plan = fetch(v(Some(p(1, 10)), &[a, b, a, b, a]).plan());
        assert_eq!(plan.steps, applies(&[a, b]));
    }

    #[test]
    fn late_key_at_or_below_cursor_ignored() {
        let a = l0(1, 11, 20);
        let b = l0(1, 21, 30);
        // Both at or below the cursor: nothing to do, no LIST.
        assert_eq!(v(Some(p(1, 30)), &[a, b]).plan(), Decision::Idle);
        // A late one among new ones is skipped.
        let c = l0(1, 31, 40);
        let plan = fetch(v(Some(p(1, 30)), &[a, c, b]).plan());
        assert_eq!(plan.steps, applies(&[c]));
    }

    #[test]
    fn late_file_in_older_epoch_with_higher_txid_ignored() {
        // (1, 60) < (2, 10): a higher TXID in an older epoch is behind.
        assert_eq!(
            v(Some(p(2, 10)), &[l0(1, 50, 60), o(1, 1, 1, 60)]).plan(),
            Decision::Idle
        );
        let a = l0(2, 11, 12);
        let plan = fetch(v(Some(p(2, 10)), &[l0(1, 50, 60), a]).plan());
        assert_eq!(plan.steps, applies(&[a]));
    }

    // ---- Gaps ----

    #[test]
    fn missing_middle_requests_gap_list() {
        // Cursor 299 after an L0; the broker has 556-811, not 300-555.
        let mut c = v(Some(p(1, 299)), &[l0(1, 556, 811)]);
        c.last_l0 = Some(p(1, 299));
        assert_eq!(
            lists(c.plan()),
            set([lv(1, 1, None), lv(1, 9, None), lv(1, 0, Some(299))])
        );
    }

    #[test]
    fn gap_not_closed_by_start_after_lists_l0_in_full() {
        let late = l0(1, 556, 811);
        let mut c = v(Some(p(1, 299)), &[late]);
        c.last_l0 = Some(p(1, 299));
        c.listed.add_level(1, 1, None, []);
        c.listed.add_level(1, 9, None, []);
        c.listed.add_level(1, 0, Some(299), [late]);
        assert_eq!(lists(c.plan()), set([lv(1, 0, None)]));
    }

    #[test]
    fn drain_l0_straddle_found_only_by_full_l0_list() {
        // The cursor (512) ended on an L1; the last known L0 ends at 100.
        // A drain L0 101-700 straddles it, so it is found by the start-after
        // LIST at 100; with a known L0 end above the drain's start (the
        // reload edge) only the full L0 LIST finds it.
        let truth = [
            l0(1, 1, 100),
            o(1, 1, 1, 256),
            o(1, 1, 257, 512),
            l0(1, 101, 700),
            l0(1, 701, 710),
        ];
        let mut c = v(Some(p(1, 512)), &[l0(1, 701, 710)]);
        c.last_l0 = Some(p(1, 100));
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.steps, applies(&[l0(1, 101, 700), l0(1, 701, 710)]));
        assert_eq!(c.listed.levels[&(1, 0)].start_after, Some(100));

        let mut c = v(Some(p(1, 512)), &[l0(1, 701, 710)]);
        c.last_l0 = Some(p(1, 300)); // an L0 that ended at 300 (reload edge)
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.steps, applies(&[l0(1, 101, 700), l0(1, 701, 710)]));
        assert_eq!(c.listed.levels[&(1, 0)].start_after, None);
    }

    #[test]
    fn missing_end_advances_to_last_provable() {
        let a = l0(1, 300, 555);
        let plan = fetch(v(Some(p(1, 299)), &[a]).plan());
        assert_eq!(plan.steps, applies(&[a]));
        assert_eq!(plan.gap, None);
        let got = prove1(&plan, &data(&[a]), &Listed::default(), Some(1));
        assert_eq!(got.head, Some(p(1, 555)));
    }

    #[test]
    fn after_gap_list_plans_through_listed_object() {
        let (a, b) = (l0(1, 300, 555), l0(1, 556, 811));
        let mut c = v(Some(p(1, 299)), &[b]);
        c.last_l0 = Some(p(1, 299));
        c.listed.add_level(1, 1, None, []);
        c.listed.add_level(1, 9, None, []);
        c.listed.add_level(1, 0, Some(299), [a, b]);
        let plan = fetch(c.plan());
        assert_eq!(plan.steps, applies(&[a, b]));
        assert_eq!(plan.gap, None);
        let got = prove1(&plan, &data(&[a, b]), &c.listed, Some(1));
        assert_eq!(got.head, Some(p(1, 811)));
        assert_eq!(got.gap, None);
    }

    #[test]
    fn lost_only_notification_found_by_next_write_gap_list() {
        // 21-40 was the cell's only notification and it was lost: nothing
        // to do until the next write, whose key leaves a gap.
        assert_eq!(v(Some(p(1, 20)), &[]).plan(), Decision::Idle);
        let truth = [l0(1, 1, 20), l0(1, 21, 40), l0(1, 41, 50)];
        let mut c = v(Some(p(1, 20)), &[l0(1, 41, 50)]);
        c.last_l0 = Some(p(1, 20));
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.steps, applies(&[l0(1, 21, 40), l0(1, 41, 50)]));
    }

    /// Cursor 299, 556-811 notified, every LIST already done this run, and
    /// 300-555 still missing.
    fn hole_view(gap: Option<GapMemo>) -> V {
        let late = l0(1, 556, 811);
        let mut c = v(Some(p(1, 299)), &[late]);
        c.gap = gap;
        for level in LEVELS {
            c.listed
                .add_level(1, level, None, [late].into_iter().filter(|_| level == 0));
        }
        c
    }

    #[test]
    fn gap_list_count_increments_only_for_same_gap() {
        let at = p(1, 299);
        let plan = fetch(hole_view(None).plan());
        assert_eq!(plan.steps, vec![]);
        assert_eq!(plan.gap, Some(GapMemo { at, lists: 1 }));
        let plan = fetch(hole_view(Some(GapMemo { at, lists: 1 })).plan());
        assert_eq!(plan.gap, Some(GapMemo { at, lists: 2 }));
        // A different gap starts over.
        let other = GapMemo {
            at: p(1, 100),
            lists: 2,
        };
        let plan = fetch(hole_view(Some(other)).plan());
        assert_eq!(plan.gap, Some(GapMemo { at, lists: 1 }));
        // The proof keeps the memo only if it reaches the gap.
        let got = prove1(&plan, &[], &hole_view(None).listed, Some(1));
        assert_eq!(got.gap, Some(GapMemo { at, lists: 1 }));
        assert_eq!(got.parked, None);
    }

    #[test]
    fn gap_lists_exhausted_parks_hole() {
        let at = p(1, 299);
        // The third run's LISTs fail: the proof parks the cell.
        let plan = fetch(hole_view(Some(GapMemo { at, lists: 2 })).plan());
        assert_eq!(plan.gap, Some(GapMemo { at, lists: 3 }));
        let got = prove1(&plan, &[], &hole_view(None).listed, Some(1));
        assert_eq!(got.parked, Some(ParkReason::Hole));
        assert_eq!(got.head, Some(at));
        // The next run does not LIST again.
        let mut c = v(Some(at), &[l0(1, 556, 811)]);
        c.gap = Some(GapMemo { at, lists: 3 });
        c.parked = Some(ParkReason::Hole);
        assert_eq!(c.plan(), Decision::Park(ParkReason::Hole));
        // CDCELL_GAP_LISTS is honoured.
        c.gap = Some(GapMemo { at, lists: 1 });
        let s = PlanSettings {
            gap_lists: 1,
            ..PlanSettings::default()
        };
        assert_eq!(c.plan_with(s), Decision::Park(ParkReason::Hole));
    }

    #[test]
    fn parked_hole_resumes_on_later_epoch_opener() {
        let at = p(1, 299);
        let late = l0(1, 556, 811);
        let (op, next) = (l0(2, 1, 1), l0(2, 2, 9));
        let mut c = v(Some(at), &[late, op, next]);
        c.gap = Some(GapMemo { at, lists: 3 });
        c.parked = Some(ParkReason::Hole);
        let plan = fetch(c.plan());
        assert_eq!(plan.base, Base::Cursor);
        assert_eq!(plan.steps, vec![Step::Opener(op), Step::Apply(next)]);
        assert_eq!(plan.dropped, vec![late]);
        assert_eq!(plan.gap, None);
        let got = prove1(&plan, &data(&[op, next]), &Listed::default(), Some(2));
        assert_eq!(got.steps, plan.steps);
        assert_eq!(got.head, Some(p(2, 9)));
        assert_eq!(got.parked, None);
        // A late notification that closes the gap also resumes.
        let mut c = v(Some(at), &[late, l0(1, 300, 555)]);
        c.gap = Some(GapMemo { at, lists: 3 });
        c.parked = Some(ParkReason::Hole);
        assert_eq!(fetch(c.plan()).steps, applies(&[l0(1, 300, 555), late]));
    }

    #[test]
    fn cursor_on_l1_start_after_uses_last_known_l0_end() {
        // The cursor (512) ended on an L1: StartAfter goes at the last
        // known L0 end, never at the L1's end.
        let fold = o(1, 1, 257, 512);
        let new = l0(1, 600, 700);
        let mut c = v(Some(p(1, 512)), &[fold, new]);
        c.last_l0 = Some(p(1, 200));
        assert!(lists(c.plan()).contains(&lv(1, 0, Some(200))));
        // A late notified L0 is known too; an L1 or L9 end never counts.
        let mut c = v(
            Some(p(1, 512)),
            &[fold, o(1, 9, 1, 400), l0(1, 101, 250), new],
        );
        c.last_l0 = Some(p(1, 200));
        assert!(lists(c.plan()).contains(&lv(1, 0, Some(250))));
        // The last L0 of another epoch does not count; nothing known: from 0.
        let mut c = v(Some(p(1, 512)), &[fold, new]);
        c.last_l0 = Some(p(0, 500));
        let got = lists(c.plan());
        assert!(got.contains(&lv(1, 0, None)), "{got:?}");
    }

    // ---- First sight, rebuild ----

    #[test]
    fn first_seen_with_txid1_opener_needs_no_list() {
        let (op, a) = (l0(3, 1, 1), l0(3, 2, 5));
        let plan = fetch(v(None, &[a, op]).plan());
        assert_eq!(plan.base, Base::Bootstrap);
        assert_eq!(plan.steps, vec![Step::Opener(op), Step::Apply(a)]);
        let got = prove1(&plan, &data(&[op, a]), &Listed::default(), Some(3));
        assert_eq!(got.steps, plan.steps);
        assert_eq!(got.head, Some(p(3, 5)));
        // The newest full image is the base.
        let l9 = o(3, 9, 1, 5);
        let b = l0(3, 6, 7);
        let plan = fetch(v(None, &[op, a, l9, b]).plan());
        assert_eq!(plan.steps, vec![Step::Opener(l9), Step::Apply(b)]);
    }

    #[test]
    fn first_seen_without_full_image_lists_epoch_chain() {
        let truth = [
            o(1, 9, 1, 30),
            l0(1, 31, 40),
            l0(2, 41, 41),
            l0(2, 42, 49),
            l0(3, 50, 50),
            l0(3, 51, 60),
        ];
        let mut c = v(None, &[l0(3, 51, 60)]);
        let first = lists(c.plan());
        assert!(first.contains(&ListReq::EpochDirs), "{first:?}");
        assert!(set(full(3)).is_subset(&first), "{first:?}");
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.base, Base::Bootstrap);
        assert_eq!(
            plan.steps,
            vec![
                Step::Opener(o(1, 9, 1, 30)),
                Step::Apply(l0(1, 31, 40)),
                Step::Apply(l0(2, 41, 41)),
                Step::Apply(l0(2, 42, 49)),
                Step::Apply(l0(3, 50, 50)),
                Step::Apply(l0(3, 51, 60)),
            ]
        );
        let got = prove1(&plan, &[], &c.listed, Some(3));
        assert_eq!(got.head, Some(p(3, 60)));
        assert_eq!(got.parked, None);
    }

    #[test]
    fn bootstrap_with_empty_owner_epoch_parks() {
        let (op, a) = (l0(2, 1, 1), l0(2, 2, 5));
        let mut c = v(None, &[op, a]);
        let plan = fetch(c.plan());
        // own.json says epoch 3, which the planner did not know of.
        let got = prove1(&plan, &data(&[op, a]), &c.listed, Some(3));
        assert_eq!(got.steps, vec![]);
        assert_eq!(got.head, None);
        assert_eq!(got.parked, Some(ParkReason::Unproven));
        // Replanned with the owner's epoch: list it; it is empty.
        c.own_hint = Some(3);
        c.parked = Some(ParkReason::Unproven);
        let truth = [op, a];
        assert_eq!(c.drive(&truth), Decision::Park(ParkReason::AwaitingOpener));
        assert!(c.listed.is_full(3));
        let got = prove1(&plan, &data(&[op, a]), &c.listed, Some(3));
        assert_eq!(got.head, None);
        assert_eq!(got.parked, Some(ParkReason::AwaitingOpener));
    }

    #[test]
    fn state_missing_rebuilds_from_newest_full_image_at_or_below_cursor() {
        let truth = [
            l0(1, 1, 1),
            l0(1, 2, 100),
            o(1, 1, 1, 150),
            o(1, 9, 1, 200),
            l0(1, 201, 300),
            l0(1, 301, 310),
            o(1, 9, 1, 310),
        ];
        let mut c = v(Some(p(1, 300)), &[l0(1, 301, 310)]);
        c.state = StateAvail::Missing;
        assert_eq!(lists(c.plan()), set(full(1)));
        let plan = fetch(c.drive(&truth));
        assert_eq!(
            plan.base,
            Base::Rebuild(vec![o(1, 9, 1, 200), l0(1, 201, 300)])
        );
        assert_eq!(plan.steps, applies(&[l0(1, 301, 310)]));
        // No new objects: no rebuild either.
        let mut c = v(Some(p(1, 300)), &[]);
        c.state = StateAvail::Missing;
        assert_eq!(c.plan(), Decision::Idle);
    }

    // ---- Epoch changes and the proof ----

    #[test]
    fn epoch_change_without_notified_opener_lists_epochs() {
        let c = v(Some(p(1, 10)), &[l0(1, 11, 20), l0(2, 25, 30)]);
        let got = lists(c.plan());
        assert!(got.contains(&ListReq::EpochDirs), "{got:?}");
        assert!(set(full(2)).is_subset(&got), "{got:?}");
    }

    #[test]
    fn snapshot_opener_collapses_unproven_tail() {
        // The owner restarted into snapshot epoch 2 before the run read
        // epoch 1's last objects: they cannot be proven.
        let tail = l0(1, 11, 20);
        let (op, a) = (l0(2, 1, 1), l0(2, 2, 5));
        let plan = fetch(v(Some(p(1, 10)), &[tail, op, a]).plan());
        assert_eq!(plan.steps, vec![Step::Opener(op), Step::Apply(a)]);
        assert_eq!(plan.dropped, vec![tail]);
        let got = prove1(&plan, &data(&[op, a]), &Listed::default(), Some(2));
        assert_eq!(got.steps, plan.steps);
        assert_eq!(got.head, Some(p(2, 5)));
        // A plan that tried the tail first: the proof skips it.
        let plan = hand(
            Some(p(1, 10)),
            vec![Step::Apply(tail), Step::Opener(op), Step::Apply(a)],
            &[],
        );
        let got = prove1(&plan, &data(&[tail, op, a]), &Listed::default(), Some(2));
        assert_eq!(got.steps, vec![Step::Opener(op), Step::Apply(a)]);
        assert_eq!(got.dropped, vec![tail]);
        assert_eq!(got.head, Some(p(2, 5)));
    }

    #[test]
    fn old_epoch_objects_proven_when_own_equals_epoch() {
        let a = l0(1, 11, 20);
        let plan = fetch(v(Some(p(1, 10)), &[a]).plan());
        let got = prove1(&plan, &data(&[a]), &Listed::default(), Some(1));
        assert_eq!(got.head, Some(p(1, 20)));
        // own moved on and nothing proves a cut: unproven.
        let got = prove1(&plan, &data(&[a]), &Listed::default(), Some(2));
        assert_eq!(got.head, Some(p(1, 10)));
        assert_eq!(got.steps, vec![]);
        assert_eq!(got.parked, Some(ParkReason::Unproven));
    }

    #[test]
    fn released_owner_same_epoch_still_proves() {
        // A released owner keeps its epoch in own.json (node cleared).
        let a = l0(4, 11, 20);
        let plan = fetch(v(Some(p(4, 10)), &[a]).plan());
        let got = prove1(&plan, &data(&[a]), &Listed::default(), Some(4));
        assert_eq!(got.steps, applies(&[a]));
        assert_eq!(got.head, Some(p(4, 20)));
    }

    #[test]
    fn zero_page_marker_header_proves_cut() {
        let (a, m, b) = (l0(1, 11, 20), l0(2, 21, 21), l0(2, 22, 30));
        let plan = fetch(v(Some(p(1, 10)), &[a, m, b]).plan());
        assert_eq!(plan.steps, applies(&[a, m, b]));
        let mut fetched = data(&[a, b]);
        fetched.extend(markers(&[m]));
        let got = prove1(&plan, &fetched, &Listed::default(), Some(2));
        assert_eq!(
            got.steps,
            vec![Step::Apply(a), Step::Marker(m), Step::Apply(b)]
        );
        assert_eq!(got.head, Some(p(2, 30)));
        assert_eq!(got.last_l0, Some(p(2, 30)));
    }

    #[test]
    fn single_txid_l0_is_not_marker_without_zero_page_header() {
        let (a, m, b) = (l0(1, 11, 20), l0(2, 21, 21), l0(2, 22, 30));
        let plan = fetch(v(Some(p(1, 10)), &[a, m, b]).plan());
        // Bucket mode: every L0 is `n-n`. Pages, or a WAL size, mean data.
        for fm in [
            Fetched::from_header(m, 3, 3 * 4120),
            Fetched::from_header(m, 0, 4120),
            Fetched::from_header(m, 2, 0),
        ] {
            assert!(!fm.is_marker);
            let got = prove1(&plan, &[data(&[a])[0], fm], &Listed::default(), Some(2));
            assert_eq!(got.head, Some(p(1, 10)), "{fm:?}");
            assert_eq!(got.parked, Some(ParkReason::Unproven));
        }
        // The key alone (no header) proves nothing either.
        let got = prove1(&plan, &data(&[a]), &Listed::default(), Some(2));
        assert_eq!(got.head, Some(p(1, 10)));
        // A marker never sits at TXID 1, and spans one TXID.
        assert!(!Fetched::from_header(l0(2, 1, 1), 0, 0).is_marker);
        assert!(!Fetched::from_header(l0(2, 21, 22), 0, 0).is_marker);
        // Replanned after the failed proof: full listings, no candidates.
        let mut c = v(Some(p(1, 10)), &[a, m, b]);
        c.parked = Some(ParkReason::Unproven);
        c.own_hint = Some(2);
        let got = lists(c.plan());
        assert!(set(full(2)).is_subset(&got), "{got:?}");
    }

    #[test]
    fn non_lowest_object_in_new_epoch_never_sets_cut() {
        let a = l0(1, 11, 20);
        let (b, m) = (l0(2, 21, 25), l0(2, 26, 26));
        let plan = hand(Some(p(1, 10)), applies(&[a, b, m]), &[]);
        let mut fetched = data(&[a, b]);
        fetched.extend(markers(&[m]));
        let got = prove1(&plan, &fetched, &Listed::default(), Some(2));
        assert_eq!(got.head, Some(p(1, 10)));
        assert_eq!(got.steps, vec![]);
        // The planner does not treat it as a cut either.
        let c = v(Some(p(1, 10)), &[a, b, m]);
        assert!(set(full(2)).is_subset(&lists(c.plan())));
        // A full listing showing a lower object: the marker-like key is not
        // the epoch's lowest, the listing's lo (21) is the cut.
        let mut listed = Listed::default();
        listed.add_epochs([1, 2]);
        for level in LEVELS {
            listed.add_level(2, level, None, [b, m]);
        }
        let got = prove1(&plan, &fetched, &listed, Some(2));
        assert_eq!(got.head, Some(p(2, 26)));
    }

    #[test]
    fn objects_above_cut_never_proven() {
        // e1's owner was fenced after 20 but wrote 21-30 (D10).
        let (a, tail) = (l0(1, 11, 20), l0(1, 21, 30));
        let (m, b) = (l0(2, 21, 21), l0(2, 22, 25));
        let plan = fetch(v(Some(p(1, 10)), &[a, tail, m, b]).plan());
        assert_eq!(plan.steps, applies(&[a, m, b]));
        let plan = hand(Some(p(1, 10)), applies(&[a, tail]), &[m]);
        let mut fetched = data(&[a, tail]);
        fetched.extend(markers(&[m]));
        let got = prove1(&plan, &fetched, &Listed::default(), Some(2));
        assert_eq!(got.steps, applies(&[a]));
        assert_eq!(got.head, Some(p(1, 20)));
    }

    #[test]
    fn full_listing_lo_of_next_epoch_proves_cut() {
        let (a, b) = (l0(1, 11, 20), l0(2, 21, 30));
        let truth = [l0(1, 1, 10), a, b];
        let mut c = v(Some(p(1, 10)), &[a, b]);
        let plan = fetch(c.drive(&truth));
        assert!(c.listed.is_full(2));
        assert_eq!(plan.steps, applies(&[a, b]));
        let got = prove1(&plan, &data(&[a, b]), &c.listed, Some(2));
        assert_eq!(got.head, Some(p(2, 30)));
    }

    #[test]
    fn cut_below_cursor_triggers_resync() {
        let (m, b) = (l0(2, 21, 21), l0(2, 22, 30));
        let plan = fetch(v(Some(p(1, 25)), &[m, b]).plan());
        assert_eq!(plan.steps, vec![]);
        assert_eq!(plan.probes, vec![m]);
        let got = prove1(&plan, &markers(&[m]), &Listed::default(), Some(2));
        assert!(got.resync);
        assert_eq!(got.steps, vec![]);
        // Not a marker: no cut, no resync.
        let got = prove1(&plan, &data(&[m]), &Listed::default(), Some(2));
        assert!(!got.resync);
    }

    #[test]
    fn cursor_epoch_skipped_by_chain_triggers_resync() {
        // A full listing of the cursor's epoch ends at 15, not at the cut
        // (20) that epoch 2 continues from: celld skips it.
        let truth = [l0(1, 1, 10), l0(1, 11, 15), l0(2, 21, 30)];
        let mut listed = Listed::default();
        answer(&mut listed, &truth, &[ListReq::EpochDirs]);
        answer(&mut listed, &truth, &full(1));
        answer(&mut listed, &truth, &full(2));
        let plan = hand(Some(p(1, 10)), applies(&[l0(1, 11, 15)]), &[]);
        let got = prove1(&plan, &[], &listed, Some(2));
        assert!(got.resync);
        assert_eq!(got.steps, vec![]);
        // The planner goes to the resync walk; no full image: parks.
        let mut c = v(Some(p(1, 10)), &[l0(1, 11, 15), l0(2, 21, 30)]);
        c.listed = listed;
        assert_eq!(c.plan(), Decision::Park(ParkReason::Hole));
    }

    #[test]
    fn intermediate_epoch_must_be_listed_before_marker_proves_cut() {
        let (a, m, b) = (l0(1, 11, 20), l0(3, 21, 21), l0(3, 22, 30));
        // Epoch 2 holds a fenced owner's late snapshot (skipped by celld).
        let late = o(2, 9, 1, 15);
        let truth = [l0(1, 1, 10), a, late, m, b];
        let mut fetched = data(&[a, b]);
        fetched.extend(markers(&[m]));
        // Without the epochs, the marker of epoch 3 proves nothing for 1.
        let plan = hand(Some(p(1, 10)), applies(&[a, m, b]), &[]);
        let got = prove1(&plan, &fetched, &Listed::default(), Some(3));
        assert_eq!(got.head, Some(p(1, 10)));
        assert_eq!(got.parked, Some(ParkReason::Unproven));
        // The planner lists the epochs, then epoch 2 in full.
        let mut c = v(Some(p(1, 10)), &[a, m, b]);
        assert!(lists(c.plan()).contains(&ListReq::EpochDirs));
        let plan = fetch(c.drive(&truth));
        assert!(c.listed.is_full(2));
        assert_eq!(plan.steps, applies(&[a, m, b]));
        let got = prove1(&plan, &fetched, &c.listed, Some(3));
        assert_eq!(
            got.steps,
            vec![Step::Apply(a), Step::Marker(m), Step::Apply(b)]
        );
        assert_eq!(got.head, Some(p(3, 30)));
    }

    #[test]
    fn unproven_without_opener_parks_awaiting_opener() {
        // own moved to epoch 2, which has nothing yet.
        let a = l0(1, 11, 20);
        let truth = [l0(1, 1, 10), a];
        let mut c = v(Some(p(1, 10)), &[a]);
        let plan = fetch(c.plan());
        let got = prove1(&plan, &data(&[a]), &c.listed, Some(2));
        assert_eq!(got.parked, Some(ParkReason::Unproven));
        c.own_hint = Some(2);
        c.parked = got.parked;
        assert_eq!(c.drive(&truth), Decision::Park(ParkReason::AwaitingOpener));
        let got = prove1(&plan, &data(&[a]), &c.listed, Some(2));
        assert_eq!(got.head, Some(p(1, 10)));
        assert_eq!(got.parked, Some(ParkReason::AwaitingOpener));
    }

    #[test]
    fn own_missing_or_unparseable_proves_nothing() {
        let a = l0(1, 11, 20);
        let plan = fetch(v(Some(p(1, 10)), &[a]).plan());
        let got = prove1(&plan, &data(&[a]), &Listed::default(), None);
        assert_eq!(got.steps, vec![]);
        assert_eq!(got.head, Some(p(1, 10)));
        assert!(got.parked.is_some());
        let (op, b) = (l0(1, 1, 1), l0(1, 2, 3));
        let plan = fetch(v(None, &[op, b]).plan());
        let got = prove1(&plan, &data(&[op, b]), &Listed::default(), None);
        assert_eq!(got.head, None);
        assert_eq!(got.steps, vec![]);
    }

    #[test]
    fn cursor_epoch_gcd_resyncs_from_owner_chain_head() {
        // Epochs 1-4 were GC'd; epoch 5 opens with an L9.
        let truth = [o(5, 9, 1, 100), l0(5, 101, 110)];
        let new = l0(5, 101, 110);
        // With the cursor's state: the opener is diffed onto it.
        let mut c = v(Some(p(1, 10)), &[new]);
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.base, Base::Cursor);
        assert_eq!(
            plan.steps,
            vec![Step::Opener(o(5, 9, 1, 100)), Step::Apply(new)]
        );
        // Without it: the cursor's state cannot be rebuilt, so resync.
        let mut c = v(Some(p(1, 10)), &[new]);
        c.state = StateAvail::Missing;
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.base, Base::Resync);
        assert_eq!(
            plan.steps,
            vec![Step::Opener(o(5, 9, 1, 100)), Step::Apply(new)]
        );
        let got = prove1(&plan, &data(&[o(5, 9, 1, 100), new]), &c.listed, Some(5));
        assert_eq!(got.head, Some(p(5, 110)));
        assert!(!got.resync);
        // A chain that links below the cursor's (absent) epoch: resync.
        let truth = [o(1, 9, 1, 20), l0(3, 21, 21), l0(3, 22, 30)];
        let mut c = v(Some(p(2, 25)), &[l0(3, 22, 30)]);
        let plan = fetch(c.drive(&truth));
        assert_eq!(plan.base, Base::Resync);
        assert_eq!(
            plan.steps,
            vec![
                Step::Opener(o(1, 9, 1, 20)),
                Step::Apply(l0(3, 21, 21)),
                Step::Apply(l0(3, 22, 30)),
            ]
        );
    }

    #[test]
    fn fold_grain_selects_l1_over_l0s() {
        let mut objs: Vec<Obj> = (11..=266).map(|t| l0(1, t, t)).collect();
        objs.push(o(1, 1, 11, 266));
        let fold = PlanSettings {
            grain: Grain::Fold,
            ..PlanSettings::default()
        };
        let c = v(Some(p(1, 10)), &objs);
        assert_eq!(fetch(c.plan_with(fold)).steps, applies(&[o(1, 1, 11, 266)]));
        assert_eq!(fetch(c.plan()).steps.len(), 256);
    }

    #[test]
    fn start_after_key_excludes_min_at_or_below_boundary() {
        let k = start_after_key("Shop:ab", 3, 0x2a);
        assert_eq!(
            k,
            "cells/Shop:ab/ltx/e3/0000/000000000000002a-ffffffffffffffff.ltx"
        );
        for (min, max, listed) in [
            (1, 0x2a, false),
            (0x2a, 0x2a, false),
            (0x2a, u64::MAX, false),
            (0x2b, 0x2b, true),
            (0x2b, 0x100, true),
            (0x1000, 0x1000, true),
        ] {
            let key = l0(3, min, max).key("Shop:ab");
            assert_eq!(key.as_str() > k.as_str(), listed, "{key}");
        }
    }
}
