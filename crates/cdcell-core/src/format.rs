//! SQLite file format: varints, b-tree page headers, cell payloads, records.
//! Reference: https://www.sqlite.org/fileformat2.html

use crate::value::Value;
use crate::{Error, Result};

/// Text encoding from the database header (offset 56).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

impl TextEncoding {
    pub fn from_header(page1: &[u8]) -> TextEncoding {
        match be_u32(&page1[56..60]) {
            2 => TextEncoding::Utf16Le,
            3 => TextEncoding::Utf16Be,
            _ => TextEncoding::Utf8,
        }
    }
}

pub(crate) fn be_u16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}

pub(crate) fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// SQLite varint at `pos`: (value, next position).
pub fn varint(b: &[u8], pos: usize) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..8 {
        let c = *b.get(pos + i).ok_or(Error::Malformed("varint past end"))?;
        v = (v << 7) | u64::from(c & 0x7f);
        if c < 0x80 {
            return Ok((v, pos + i + 1));
        }
    }
    let c = *b.get(pos + 8).ok_or(Error::Malformed("varint past end"))?;
    Ok(((v << 8) | u64::from(c), pos + 9))
}

/// B-tree page types (first byte of the page header).
pub const INDEX_INTERIOR: u8 = 0x02;
pub const TABLE_INTERIOR: u8 = 0x05;
pub const INDEX_LEAF: u8 = 0x0a;
pub const TABLE_LEAF: u8 = 0x0d;

/// Parsed b-tree page header.
#[derive(Debug)]
pub struct PageHeader {
    pub kind: u8,
    /// Absolute offsets of each cell within the page.
    pub cells: Vec<usize>,
    pub right_child: Option<u32>,
}

impl PageHeader {
    pub fn is_interior(&self) -> bool {
        matches!(self.kind, INDEX_INTERIOR | TABLE_INTERIOR)
    }
    pub fn is_table(&self) -> bool {
        matches!(self.kind, TABLE_INTERIOR | TABLE_LEAF)
    }
}

pub fn page_header(page: &[u8], pgno: u32) -> Result<PageHeader> {
    let off = if pgno == 1 { 100 } else { 0 };
    let kind = *page.get(off).ok_or(Error::Malformed("short page"))?;
    if !matches!(
        kind,
        INDEX_INTERIOR | TABLE_INTERIOR | INDEX_LEAF | TABLE_LEAF
    ) {
        return Err(Error::NotBtree(pgno, kind));
    }
    let ncell = usize::from(be_u16(&page[off + 3..]));
    let interior = matches!(kind, INDEX_INTERIOR | TABLE_INTERIOR);
    let right_child = interior.then(|| be_u32(&page[off + 8..]));
    let ptrs = off + if interior { 12 } else { 8 };
    if ptrs + 2 * ncell > page.len() {
        return Err(Error::Malformed("cell pointer array past end"));
    }
    let cells = (0..ncell)
        .map(|i| usize::from(be_u16(&page[ptrs + 2 * i..])))
        .collect();
    Ok(PageHeader {
        kind,
        cells,
        right_child,
    })
}

/// Bytes of a payload stored on the b-tree page itself (fileformat2 §1.6).
pub fn local_payload_size(payload: u64, usable: usize, is_table_leaf: bool) -> usize {
    let u = usable as u64;
    let x = if is_table_leaf {
        u - 35
    } else {
        ((u - 12) * 64 / 255) - 23
    };
    if payload <= x {
        return payload as usize;
    }
    let m = ((u - 12) * 32 / 255) - 23;
    let k = m + ((payload - m) % (u - 4));
    (if k <= x { k } else { m }) as usize
}

/// Decodes a record into values. `enc` applies to text values.
pub fn record(payload: &[u8], enc: TextEncoding) -> Result<Vec<Value>> {
    let (header_len, mut p) = varint(payload, 0)?;
    let header_len = header_len as usize;
    if header_len > payload.len() {
        return Err(Error::Malformed("record header past end"));
    }
    let mut types = Vec::new();
    while p < header_len {
        let (t, next) = varint(payload, p)?;
        types.push(t);
        p = next;
    }
    let mut q = header_len;
    let mut out = Vec::with_capacity(types.len());
    let take = |q: &mut usize, n: usize| -> Result<&[u8]> {
        let s = payload
            .get(*q..*q + n)
            .ok_or(Error::Malformed("record body past end"))?;
        *q += n;
        Ok(s)
    };
    for t in types {
        let v = match t {
            0 => Value::Null,
            1..=6 => {
                let n = [0, 1, 2, 3, 4, 6, 8][t as usize];
                let b = take(&mut q, n)?;
                let mut v: i64 = if b[0] & 0x80 != 0 { -1 } else { 0 };
                for &x in b {
                    v = (v << 8) | i64::from(x);
                }
                Value::Integer(v)
            }
            7 => {
                let b = take(&mut q, 8)?;
                Value::Real(f64::from_be_bytes(b.try_into().unwrap()))
            }
            8 => Value::Integer(0),
            9 => Value::Integer(1),
            10 | 11 => return Err(Error::Malformed("reserved serial type")),
            t if t % 2 == 0 => Value::Blob(take(&mut q, ((t - 12) / 2) as usize)?.to_vec()),
            t => {
                let b = take(&mut q, ((t - 13) / 2) as usize)?;
                Value::Text(decode_text(b, enc)?)
            }
        };
        out.push(v);
    }
    Ok(out)
}

fn decode_text(b: &[u8], enc: TextEncoding) -> Result<String> {
    match enc {
        TextEncoding::Utf8 => Ok(String::from_utf8_lossy(b).into_owned()),
        TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
            let units: Vec<u16> = b
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| {
                    if enc == TextEncoding::Utf16Le {
                        u16::from_le_bytes([c[0], c[1]])
                    } else {
                        u16::from_be_bytes([c[0], c[1]])
                    }
                })
                .collect();
            Ok(String::from_utf16_lossy(&units))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints() {
        assert_eq!(varint(&[0x05], 0).unwrap(), (5, 1));
        assert_eq!(varint(&[0x81, 0x00], 0).unwrap(), (128, 2));
        assert_eq!(varint(&[0xff; 9], 0).unwrap().0, u64::MAX);
    }

    #[test]
    fn records() {
        // header len 4: types int8(1), text len 1 (15), null(0); body 0x2a 'x'
        let r = record(&[4, 1, 15, 0, 0x2a, b'x'], TextEncoding::Utf8).unwrap();
        assert_eq!(
            r,
            vec![Value::Integer(42), Value::Text("x".into()), Value::Null]
        );
        // negative 16-bit
        let r = record(&[2, 2, 0xff, 0xfe], TextEncoding::Utf8).unwrap();
        assert_eq!(r, vec![Value::Integer(-2)]);
    }
}
