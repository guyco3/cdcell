//! B-tree traversal over a page source (shadow pages, optionally overlaid with
//! the pages of one LTX unit).

use std::collections::{BTreeSet, HashMap};

use crate::format::{self, be_u32, PageHeader, TextEncoding};
use crate::value::Value;
use crate::{Error, Result};

/// A database's pages as cdcell holds them: page number → page bytes, plus the
/// database size in pages (`commit` in LTX terms).
#[derive(Clone, Debug, Default)]
pub struct Pages {
    pub page_size: usize,
    /// Usable bytes per page: page size minus the reserved bytes (header 20).
    pub usable: usize,
    pub commit: u32,
    pub pages: HashMap<u32, Vec<u8>>,
}

impl Pages {
    /// Builds pages from a full database image.
    pub fn from_image(image: &[u8]) -> Result<Pages> {
        if image.len() < 100 {
            return Err(Error::Malformed("image shorter than the header"));
        }
        let page_size = page_size_from_header(image);
        if !image.len().is_multiple_of(page_size) {
            return Err(Error::Malformed("image not a whole number of pages"));
        }
        let commit = (image.len() / page_size) as u32;
        let pages = image
            .chunks(page_size)
            .enumerate()
            .map(|(i, p)| (i as u32 + 1, p.to_vec()))
            .collect();
        Ok(Pages {
            page_size,
            usable: page_size - usize::from(image[20]),
            commit,
            pages,
        })
    }

    /// The full database image (missing pages zero-filled, e.g. the lock page).
    pub fn to_image(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.page_size * self.commit as usize];
        for (&pgno, data) in &self.pages {
            if pgno >= 1 && pgno <= self.commit {
                let at = (pgno as usize - 1) * self.page_size;
                out[at..at + self.page_size].copy_from_slice(data);
            }
        }
        out
    }

    pub fn encoding(&self) -> TextEncoding {
        self.pages
            .get(&1)
            .map_or(TextEncoding::Utf8, |p| TextEncoding::from_header(p))
    }
}

pub(crate) fn page_size_from_header(page1: &[u8]) -> usize {
    match format::be_u16(&page1[16..18]) {
        1 => 65536,
        n => usize::from(n),
    }
}

/// Read access to pages, cut at `commit`.
pub struct View<'a> {
    pub base: &'a Pages,
    pub overlay: Option<&'a HashMap<u32, Vec<u8>>>,
    pub commit: u32,
}

impl<'a> View<'a> {
    pub fn before(base: &'a Pages) -> View<'a> {
        View {
            base,
            overlay: None,
            commit: base.commit,
        }
    }
    pub fn after(base: &'a Pages, overlay: &'a HashMap<u32, Vec<u8>>, commit: u32) -> View<'a> {
        View {
            base,
            overlay: Some(overlay),
            commit,
        }
    }

    pub fn page(&self, pgno: u32) -> Result<&'a [u8]> {
        if pgno == 0 || pgno > self.commit {
            return Err(Error::MissingPage(pgno));
        }
        if let Some(p) = self.overlay.and_then(|o| o.get(&pgno)) {
            return Ok(p);
        }
        self.base
            .pages
            .get(&pgno)
            .map(Vec::as_slice)
            .ok_or(Error::MissingPage(pgno))
    }

    pub fn usable(&self) -> usize {
        self.base.usable
    }
}

/// The pages of one b-tree.
#[derive(Clone, Debug, Default)]
pub struct Structure {
    pub all: BTreeSet<u32>,
    pub interior: BTreeSet<u32>,
    /// True for table b-trees (rowid), false for index b-trees (also WITHOUT ROWID tables).
    pub is_table: bool,
}

impl Structure {
    /// Pages that hold entries: leaves for table b-trees; every page for
    /// index b-trees (WITHOUT ROWID rows also live in interior pages).
    pub fn entry_pages(&self) -> BTreeSet<u32> {
        if self.is_table {
            self.all.difference(&self.interior).copied().collect()
        } else {
            self.all.clone()
        }
    }
}

pub fn walk(view: &View<'_>, root: u32) -> Result<Structure> {
    let mut s = Structure::default();
    let mut stack = vec![root];
    let mut first = true;
    while let Some(pgno) = stack.pop() {
        if !s.all.insert(pgno) {
            return Err(Error::Malformed("b-tree cycle"));
        }
        let page = view.page(pgno)?;
        let h = format::page_header(page, pgno)?;
        if first {
            s.is_table = h.is_table();
            first = false;
        }
        if h.is_interior() {
            s.interior.insert(pgno);
            for &c in &h.cells {
                stack.push(be_u32(
                    page.get(c..c + 4)
                        .ok_or(Error::Malformed("child pointer"))?,
                ));
            }
            stack.push(h.right_child.unwrap_or(0));
        }
    }
    Ok(s)
}

/// One entry on a page.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Rowid for table b-trees.
    pub rowid: Option<i64>,
    pub record: Vec<Value>,
    /// Overflow pages of this entry's payload.
    pub overflow: Vec<u32>,
}

/// Entries stored on one page of a b-tree.
pub fn entries(view: &View<'_>, pgno: u32, enc: TextEncoding) -> Result<Vec<Entry>> {
    let page = view.page(pgno)?;
    let h: PageHeader = format::page_header(page, pgno)?;
    let mut out = Vec::with_capacity(h.cells.len());
    match h.kind {
        format::TABLE_INTERIOR => {}
        format::TABLE_LEAF => {
            for &c in &h.cells {
                let (len, q) = format::varint(page, c)?;
                let (rowid, q) = format::varint(page, q)?;
                let (payload, overflow) = payload(view, page, q, len, true)?;
                out.push(Entry {
                    rowid: Some(rowid as i64),
                    record: format::record(&payload, enc)?,
                    overflow,
                });
            }
        }
        format::INDEX_LEAF | format::INDEX_INTERIOR => {
            let skip = if h.kind == format::INDEX_INTERIOR {
                4
            } else {
                0
            };
            for &c in &h.cells {
                let (len, q) = format::varint(page, c + skip)?;
                let (payload, overflow) = payload(view, page, q, len, false)?;
                out.push(Entry {
                    rowid: None,
                    record: format::record(&payload, enc)?,
                    overflow,
                });
            }
        }
        _ => unreachable!(),
    }
    Ok(out)
}

fn payload(
    view: &View<'_>,
    page: &[u8],
    at: usize,
    len: u64,
    table: bool,
) -> Result<(Vec<u8>, Vec<u32>)> {
    let usable = view.usable();
    let local = format::local_payload_size(len, usable, table);
    let mut out = page
        .get(at..at + local)
        .ok_or(Error::Malformed("payload past end"))?
        .to_vec();
    let mut overflow = Vec::new();
    if (local as u64) < len {
        let mut next = be_u32(
            page.get(at + local..at + local + 4)
                .ok_or(Error::Malformed("overflow pointer"))?,
        );
        while next != 0 && (out.len() as u64) < len {
            if overflow.len() > view.commit as usize {
                return Err(Error::Malformed("overflow chain cycle"));
            }
            overflow.push(next);
            let op = view.page(next)?;
            let take = (usable - 4).min(len as usize - out.len());
            out.extend_from_slice(&op[4..4 + take]);
            next = be_u32(&op[..4]);
        }
        if (out.len() as u64) < len {
            return Err(Error::Malformed("overflow chain too short"));
        }
    }
    Ok((out, overflow))
}
