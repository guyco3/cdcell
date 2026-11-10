//! LTX files and bundles as decoder units (design §4).

use crate::{Error, Result};

/// One unit for the per-cell pipeline: the decoded pages of one L0 file.
#[derive(Clone, Debug)]
pub struct Unit {
    pub min_txid: u64,
    pub max_txid: u64,
    /// Database size in pages after this unit.
    pub commit: u32,
    pub page_size: u32,
    /// LTX header timestamp (owner wall clock, ms).
    pub timestamp_ms: i64,
    pub wal_size: i64,
    pub pages: Vec<(u32, Vec<u8>)>,
}

impl Unit {
    /// A TXID-1 snapshot (all pages `1..=commit`), an epoch opener.
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }
    /// A paged-epoch continuation marker: one TXID, zero pages, empty WAL.
    pub fn is_paged_marker(&self) -> bool {
        self.min_txid == self.max_txid
            && self.pages.is_empty()
            && self.wal_size == 0
            && self.min_txid > 1
    }
}

/// Decodes and verifies one LTX file (CRC64, page index, trailer).
pub fn decode_file(bytes: &[u8]) -> Result<Unit> {
    let header = celld_ltx::ltx::Header::parse(bytes).map_err(|e| Error::Ltx(e.to_string()))?;
    let pages = celld_ltx::ltx::decode_file_pages(bytes).map_err(|e| Error::Ltx(e.to_string()))?;
    Ok(Unit {
        min_txid: header.min_txid.0,
        max_txid: header.max_txid.0,
        commit: header.commit,
        page_size: header.page_size,
        timestamp_ms: header.timestamp,
        wal_size: header.wal_size,
        pages,
    })
}

/// One bundle row with its decoded L0 file.
#[derive(Clone, Debug)]
pub struct BundleUnit {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub unit: Unit,
}

/// Splits a bundle (`*.ltxb`) into its rows, each decoded as a unit.
pub fn decode_bundle(bytes: &[u8]) -> Result<Vec<BundleUnit>> {
    let rows = celld_ltx::bundle::decode_rows(bytes).map_err(|e| Error::Ltx(e.to_string()))?;
    rows.iter()
        .map(|r| {
            let l0 = celld_ltx::bundle::slice(bytes, r).map_err(|e| Error::Ltx(e.to_string()))?;
            Ok(BundleUnit {
                cell: r.cell.clone(),
                cell_epoch: r.cell_epoch,
                txid: r.txid,
                unit: decode_file(l0)?,
            })
        })
        .collect()
}
