// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Intake (design §3): which notified keys carry units, and decoding a
//! fetched object into per-cell units. Shared by the service loop
//! (`run.rs`) and by tests/simulation ([`ingest`]).

use anyhow::Result;
use tokio::time::Instant;
use tracing::warn;

use crate::keys::{self, ObjKey};
use crate::pipeline::{MsgId, Origin, Pipeline, UnitIn};
use crate::source::Store;

/// The parsed key if its object carries units: per-cell L0 files, bundles
/// not yet read by reconciliation, and L1/L9 objects that reach past what
/// cdcell has for the cell (after an outage they can be the only trace of an
/// idle cell's changes; otherwise they are skipped without a GET). own.json
/// and recovery hints carry none.
pub fn wanted(pipe: &mut Pipeline, key: &str) -> Option<ObjKey> {
    let parsed = keys::parse(key);
    match &parsed {
        ObjKey::Ltx { level: 0, .. } => Some(parsed),
        ObjKey::Ltx {
            level: 1 | 9,
            cell,
            epoch,
            max,
            ..
        } => pipe.behind(cell, *epoch, *max).then_some(parsed),
        ObjKey::Bundle { .. } => pipe.mark_bundle(key).then_some(parsed),
        _ => None,
    }
}

/// Decodes a fetched object into units. An undecodable object is counted
/// and yields none.
pub fn units(
    key: &str,
    parsed: ObjKey,
    body: &[u8],
    msg: Option<MsgId>,
    got_at: Instant,
) -> Vec<UnitIn> {
    let mut out = Vec::new();
    match parsed {
        ObjKey::Ltx { cell, epoch, .. } => match cdcell_core::ltx::decode_file(body) {
            Ok(unit) => out.push(UnitIn {
                cell,
                epoch,
                unit,
                origin: Origin::Cell,
                msg,
                got_at,
                bootstrap: false,
            }),
            Err(e) => {
                crate::metrics::inc(&crate::metrics::M.decode_errors);
                warn!(key, %e, "undecodable LTX object")
            }
        },
        ObjKey::Bundle {
            node,
            generation,
            log_epoch,
            ..
        } => match cdcell_core::ltx::decode_bundle(body) {
            Ok(rows) => out.extend(rows.into_iter().map(|r| UnitIn {
                cell: r.cell,
                epoch: r.cell_epoch,
                unit: r.unit,
                origin: Origin::Bundle {
                    node: node.clone(),
                    generation: generation.clone(),
                    log_epoch,
                },
                msg,
                got_at,
                bootstrap: false,
            })),
            Err(e) => {
                crate::metrics::inc(&crate::metrics::M.decode_errors);
                warn!(key, %e, "undecodable bundle")
            }
        },
        _ => {}
    }
    out
}

/// One notification without a broker message (tests, simulation): filter,
/// GET, decode, add. Returns the number of units added.
pub async fn ingest(pipe: &mut Pipeline, src: &Store, key: &str) -> Result<usize> {
    let Some(parsed) = wanted(pipe, key) else {
        return Ok(0);
    };
    let got_at = Instant::now();
    let Some(body) = src.get(key).await? else {
        return Ok(0);
    };
    let us = units(key, parsed, &body, None, got_at);
    let n = us.len();
    for u in us {
        pipe.add(u);
    }
    Ok(n)
}
