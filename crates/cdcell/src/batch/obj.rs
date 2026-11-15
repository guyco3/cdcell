// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! The units the planner reasons about: a position in a cell's history and
//! one per-cell LTX object, as named by its key. PURE.

use crate::keys::{self, ObjKey};

/// A point in one cell's history. A cell's units are totally ordered by
/// `(epoch, txid)` (design §2), so the derived `Ord` is that order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos {
    pub epoch: u64,
    pub txid: u64,
}

/// One per-cell LTX object `cells/<cell>/ltx/e<epoch>/<level>/<min>-<max>.ltx`
/// (the cell is implied by context). The derived `Ord` is
/// `(epoch, level, min, max)`, the order LIST results are sorted into before
/// planning (design §2.1); it is also the tie-break order of
/// [`super::chain::best_path`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Obj {
    pub epoch: u64,
    pub level: u32,
    pub min: u64,
    pub max: u64,
}

impl Obj {
    /// The cell and object a decoded per-cell LTX key names; `None` for any
    /// other key (`own.json`, bundles, facets) or an inverted range.
    pub fn from_key(key: &str) -> Option<(String, Obj)> {
        match keys::parse(key) {
            ObjKey::Ltx {
                cell,
                epoch,
                level,
                min,
                max,
            } if min <= max => Some((
                cell,
                Obj {
                    epoch,
                    level,
                    min,
                    max,
                },
            )),
            _ => None,
        }
    }

    /// The object's key under `cell` (inverse of [`Obj::from_key`]).
    pub fn key(&self, cell: &str) -> String {
        keys::ltx_key(cell, self.epoch, self.level, self.min, self.max)
    }
}
