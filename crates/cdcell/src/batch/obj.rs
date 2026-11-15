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
    /// other key (`own.json`, bundles, facets), an inverted range, a cell
    /// that is not one path segment (celld's scope charset has no `/`), or
    /// a spelling celld never writes (short or uppercase hex, `e01`, `+1`):
    /// celld's restore does not list those, and [`Obj::key`] would not
    /// name the same object, so only the canonical key parses.
    pub fn from_key(key: &str) -> Option<(String, Obj)> {
        match keys::parse(key) {
            ObjKey::Ltx {
                cell,
                epoch,
                level,
                min,
                max,
            } if min <= max && !cell.is_empty() && !cell.contains('/') => {
                let obj = Obj {
                    epoch,
                    level,
                    min,
                    max,
                };
                (obj.key(&cell) == key).then_some((cell, obj))
            }
            _ => None,
        }
    }

    /// The object's key under `cell` (inverse of [`Obj::from_key`]).
    pub fn key(&self, cell: &str) -> String {
        keys::ltx_key(cell, self.epoch, self.level, self.min, self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const L0: &str = "0000/0000000000000001-0000000000000005.ltx";

    fn parses(key: &str) -> Option<(String, Obj)> {
        Obj::from_key(key)
    }

    #[test]
    fn from_key_odd_but_valid_cells() {
        // celld's scope charset: alphanumerics and `_ - . : $`.
        for cell in ["Shop:ab", "a:b:c", "$X:..", "Class:.", "_-.:$", "0"] {
            let key = format!("cells/{cell}/ltx/e1/{L0}");
            let want = Obj {
                epoch: 1,
                level: 0,
                min: 1,
                max: 5,
            };
            assert_eq!(parses(&key), Some((cell.to_string(), want)), "{key}");
            assert_eq!(want.key(cell), key);
        }
    }

    #[test]
    fn from_key_rejects_facets_and_multi_segment_cells() {
        for key in [
            format!("cells/a/facets/0123/ltx/e1/{L0}"),
            format!("cells/a/facets/01/facets/23/ltx/e1/{L0}"),
            // Not a facet (no hash segment) and not a celld scope either:
            // a root scope cannot contain `/` (celld logic/cell.rs).
            format!("cells/a/facets/ltx/e1/{L0}"),
            format!("cells/a/b/ltx/e1/{L0}"),
            format!("cells//ltx/e1/{L0}"),
            format!("cells/a/ltx/b/ltx/e1/{L0}"),
        ] {
            assert_eq!(parses(&key), None, "{key}");
        }
    }

    #[test]
    fn from_key_rejects_keys_celld_never_writes() {
        // celld writes level `{:04x}`, TXIDs `{:016x}` (lowercase, fixed
        // width), epoch dirs `e{epoch}` in plain decimal. Any other spelling
        // is a key celld's restore never lists (a level dir other than
        // `{:04x}`, a filename whose halves are not 16 chars) or one that
        // `Obj::key` would not reproduce, so a GET of the planned object
        // would miss.
        for key in [
            "cells/a/ltx/e1/0000/1-5.ltx",
            "cells/a/ltx/e1/0000/000000000000000A-000000000000000B.ltx",
            "cells/a/ltx/e1/0000/+00000000000000a-000000000000000b.ltx",
            "cells/a/ltx/e1/0000/00000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e1/9/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e1/00009/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e1/000A/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e01/0000/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e+1/0000/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e/0000/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/E1/0000/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e1/0000/0000000000000001-0000000000000005.ltx.tmp",
            "cells/a/ltx/e1/0000/0000000000000001_0000000000000005.ltx",
            "cells/a/ltx/e1/0000/0000000000000001-0000000000000005-0000000000000006.ltx",
            "cells/a/ltx/e1/0000/x/0000000000000001-0000000000000005.ltx",
            "cells/a/ltx/e1/0000/",
            "cells/a/ltx/retired.json",
            "cells/a/own.json",
            "log/n/g/bundle/e1-1.ltxb",
            "",
        ] {
            assert_eq!(parses(key), None, "{key}");
        }
    }

    #[test]
    fn from_key_levels_and_extremes() {
        // Levels 2-8 and above 9 parse (epoch_chain drops levels above 9,
        // as celld lists only 0..=9); epoch 0 and u64 extremes round-trip.
        for (level, min, max, epoch) in [
            (2, 1, 1, 0),
            (8, 7, 9, u64::MAX),
            (10, 1, 2, 1),
            (0xffff, 0, u64::MAX, 3),
            (9, 1, u64::MAX, 18446744073709551615),
        ] {
            let o = Obj {
                epoch,
                level,
                min,
                max,
            };
            assert_eq!(parses(&o.key("c")), Some(("c".to_string(), o)));
        }
        // Wider than four digits is still canonical for a level above
        // 0xffff (`{:04x}` is a minimum width); epoch_chain drops it.
        assert_eq!(
            parses("cells/c/ltx/e1/10000/0000000000000001-0000000000000001.ltx")
                .map(|(_, o)| o.level),
            Some(0x10000)
        );
    }

    fn arb_cell() -> impl Strategy<Value = String> {
        "[A-Za-z0-9_.:$-]{1,12}".prop_filter("not . or ..", |c| c != "." && c != "..")
    }

    fn arb_obj() -> impl Strategy<Value = Obj> {
        (any::<u64>(), 0u32..=0xffff, any::<u64>(), any::<u64>()).prop_map(
            |(epoch, level, a, b)| Obj {
                epoch,
                level,
                min: a.min(b),
                max: a.max(b),
            },
        )
    }

    proptest! {
        #[test]
        fn prop_key_round_trips(cell in arb_cell(), o in arb_obj()) {
            prop_assert_eq!(Obj::from_key(&o.key(&cell)), Some((cell, o)));
        }

        /// Whatever parses is canonical: `key` gives back the same string.
        #[test]
        fn prop_from_key_is_canonical(
            cell in arb_cell(),
            o in arb_obj(),
            at in any::<prop::sample::Index>(),
            ch in prop::sample::select(vec!['0', '1', 'a', 'A', 'f', 'F', '+', '-', '/', 'e', 'x', ':']),
            op in 0u8..3,
        ) {
            let mut key: Vec<char> = o.key(&cell).chars().collect();
            let i = at.index(key.len() + 1);
            match op {
                0 if i < key.len() => key[i] = ch,
                1 if i < key.len() => {
                    key.remove(i);
                }
                _ => key.insert(i, ch),
            }
            let key: String = key.into_iter().collect();
            if let Some((c, p)) = Obj::from_key(&key) {
                prop_assert_eq!(p.key(&c), key);
            }
        }
    }
}
