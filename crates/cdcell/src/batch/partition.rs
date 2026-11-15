// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Which partition a cell belongs to (design §2). PURE.

use sha2::{Digest, Sha256};

/// The partition of `cell` among `p`: the first four bytes of
/// `sha256(cell)`, big-endian, mod `p`. `p` is a power of two, so doubling
/// it splits every partition `k` into `k` and `k + p` (`cdcell
/// repartition` only moves shards, never re-hashes). The hash is pinned:
/// changing it would move every cell's checkpoint.
pub fn partition_of(cell: &str, p: u32) -> u32 {
    assert!(
        p.is_power_of_two(),
        "partition count {p} is not a power of two"
    );
    let h = Sha256::digest(cell.as_bytes());
    u32::from_be_bytes([h[0], h[1], h[2], h[3]]) % p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_of_pinned_values() {
        // sha256 first 4 bytes: Shop:ab a75c6a38, a ca978112,
        // cell-0 e876ca59, user:42 ea3fd43b, $X:.. bc226288.
        for (cell, p16, p32) in [
            ("Shop:ab", 8, 24),
            ("a", 2, 18),
            ("cell-0", 9, 25),
            ("user:42", 11, 27),
            ("$X:..", 8, 8),
        ] {
            assert_eq!(partition_of(cell, 16), p16, "{cell}");
            assert_eq!(partition_of(cell, 32), p32, "{cell}");
            assert_eq!(partition_of(cell, 1), 0, "{cell}");
        }
        assert_eq!(partition_of("a", 1 << 31), 0xca978112 % (1 << 31));
    }

    #[test]
    fn partition_doubling_splits_ranges() {
        for i in 0..2000 {
            let cell = format!("Shop:{i}");
            for p in [1u32, 2, 4, 8, 16, 1024] {
                let k = partition_of(&cell, p);
                let k2 = partition_of(&cell, 2 * p);
                assert!(k < p && k2 < 2 * p);
                assert!(k2 == k || k2 == k + p, "{cell}: {k} of {p} -> {k2}");
            }
        }
        // Both halves are used.
        let moved = (0..2000)
            .filter(|i| partition_of(&format!("Shop:{i}"), 32) >= 16)
            .count();
        assert!((800..1200).contains(&moved), "{moved}");
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn partition_count_must_be_power_of_two() {
        partition_of("a", 12);
    }
}
