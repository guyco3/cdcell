//! Object keys celld writes (design §3).

use percent_encoding::percent_decode_str;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjKey {
    /// `cells/<cell>/ltx/e<E>/<level>/<min>-<max>.ltx`
    Ltx {
        cell: String,
        epoch: u64,
        level: u32,
        min: u64,
        max: u64,
    },
    /// `cells/<cell>/own.json`
    Own {
        cell: String,
    },
    /// `log/<node>/<gen>/bundle/e<LE>-<seq>.ltxb`
    Bundle {
        node: String,
        generation: String,
        log_epoch: u64,
        seq: u64,
    },
    /// `log/<node>/<gen>/recovered/<sha>.json`
    Recovered {
        node: String,
        generation: String,
    },
    Other,
}

/// S3 notification keys are form-URL-encoded (`:` → `%3A`, space → `+`).
pub fn url_decode(key: &str) -> String {
    percent_decode_str(&key.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

/// Parses a decoded key with the fleet prefix already stripped.
pub fn parse(key: &str) -> ObjKey {
    if let Some(rest) = key.strip_prefix("cells/") {
        if let Some(cell) = rest.strip_suffix("/own.json") {
            if !cell.contains('/') {
                return ObjKey::Own {
                    cell: cell.to_string(),
                };
            }
        }
        if let Some((cell, tail)) = rest.split_once("/ltx/") {
            if cell.contains("/facets/") {
                return ObjKey::Other; // facets deferred in v1 (design §9)
            }
            let parts: Vec<&str> = tail.split('/').collect();
            if let [e, level, name] = parts[..] {
                if let (Some(epoch), Ok(level), Some((min, max))) = (
                    e.strip_prefix('e').and_then(|x| x.parse().ok()),
                    u32::from_str_radix(level, 16),
                    name.strip_suffix(".ltx").and_then(|n| n.split_once('-')),
                ) {
                    if let (Ok(min), Ok(max)) =
                        (u64::from_str_radix(min, 16), u64::from_str_radix(max, 16))
                    {
                        return ObjKey::Ltx {
                            cell: cell.to_string(),
                            epoch,
                            level,
                            min,
                            max,
                        };
                    }
                }
            }
        }
        return ObjKey::Other;
    }
    if let Some(rest) = key.strip_prefix("log/") {
        let parts: Vec<&str> = rest.split('/').collect();
        match parts[..] {
            [node, generation, "bundle", name] => {
                if let Some((le, seq)) = name
                    .strip_suffix(".ltxb")
                    .and_then(|n| n.strip_prefix('e'))
                    .and_then(|n| n.split_once('-'))
                {
                    if let (Ok(log_epoch), Ok(seq)) = (le.parse(), seq.parse()) {
                        return ObjKey::Bundle {
                            node: node.into(),
                            generation: generation.into(),
                            log_epoch,
                            seq,
                        };
                    }
                }
            }
            [node, generation, "recovered", name] if name.ends_with(".json") => {
                return ObjKey::Recovered {
                    node: node.into(),
                    generation: generation.into(),
                };
            }
            _ => {}
        }
    }
    ObjKey::Other
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert_eq!(
            url_decode("cells/Shop%3Aab/own.json"),
            "cells/Shop:ab/own.json"
        );
        assert_eq!(
            parse("cells/Shop:ab/own.json"),
            ObjKey::Own {
                cell: "Shop:ab".into()
            }
        );
        assert_eq!(
            parse("cells/Shop:ab/ltx/e12/0000/000000000000000a-000000000000000c.ltx"),
            ObjKey::Ltx {
                cell: "Shop:ab".into(),
                epoch: 12,
                level: 0,
                min: 10,
                max: 12
            }
        );
        assert_eq!(
            parse("log/celld-a/72c6/bundle/e2-00000017.ltxb"),
            ObjKey::Bundle {
                node: "celld-a".into(),
                generation: "72c6".into(),
                log_epoch: 2,
                seq: 17
            }
        );
        assert_eq!(parse("nodes/celld-a.json"), ObjKey::Other);
        assert_eq!(
            parse("cells/R:1/facets/ab/ltx/e1/0000/0000000000000001-0000000000000001.ltx"),
            ObjKey::Other
        );
    }
}
