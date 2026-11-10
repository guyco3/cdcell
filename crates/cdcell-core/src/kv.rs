//! Durable Object KV values (`_cf_KV.v`, design §8.7).
//!
//! celld stores `ctx.storage.put` values as V8 ValueSerializer bytes with the
//! version byte forced to 15 (`ff 0f …`), stub rows with a leading `0x01`, and
//! legacy rows as JSON text. This module decodes the V8 format into tagged
//! JSON: `{"$date":iso}`, `{"$bigint":"…"}`, `{"$map":[[k,v],…]}`,
//! `{"$set":[…]}`, `{"$bytes":b64,"$kind":"Uint8Array"}`, `{"$undefined":true}`,
//! `{"$ref":n}` for a repeated (shared or cyclic) object.
//! Format reference: v8/src/objects/value-serializer.cc.

use crate::value::{base64, json_string, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvType {
    Json,
    V8,
    Stub,
    LegacyText,
}

impl KvType {
    pub fn as_str(&self) -> &'static str {
        match self {
            KvType::Json => "json",
            KvType::V8 => "v8",
            KvType::Stub => "stub",
            KvType::LegacyText => "legacy_text",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct KvValue {
    pub value_type: KvType,
    /// Tagged JSON; `None` for stub rows or values cdcell cannot decode.
    pub value_json: Option<String>,
    pub value_raw: Vec<u8>,
}

/// Decodes one `_cf_KV.v` value.
pub fn decode(v: &Value) -> KvValue {
    match v {
        Value::Text(t) => KvValue {
            value_type: KvType::LegacyText,
            value_json: Some(t.clone()),
            value_raw: t.as_bytes().to_vec(),
        },
        Value::Blob(b) if b.first() == Some(&0x01) => KvValue {
            value_type: KvType::Stub,
            value_json: None,
            value_raw: b.clone(),
        },
        Value::Blob(b) => {
            let json = V8::new(b).document().ok();
            // Plain JSON values (no tags) are reported as `json`.
            let value_type = match &json {
                Some(j) if !j.contains("\"$") => KvType::Json,
                _ => KvType::V8,
            };
            KvValue {
                value_type,
                value_json: json,
                value_raw: b.clone(),
            }
        }
        Value::Null => KvValue {
            value_type: KvType::Json,
            value_json: Some("null".into()),
            value_raw: Vec::new(),
        },
        Value::Integer(i) => KvValue {
            value_type: KvType::LegacyText,
            value_json: Some(i.to_string()),
            value_raw: Vec::new(),
        },
        Value::Real(f) => KvValue {
            value_type: KvType::LegacyText,
            value_json: Some(v.to_json()),
            value_raw: f.to_be_bytes().to_vec(),
        },
    }
}

struct V8<'a> {
    b: &'a [u8],
    p: usize,
    next_id: u32,
}

type R<T> = std::result::Result<T, &'static str>;

impl<'a> V8<'a> {
    fn new(b: &'a [u8]) -> V8<'a> {
        V8 {
            b,
            p: 0,
            next_id: 0,
        }
    }

    fn document(&mut self) -> R<String> {
        if self.peek()? == 0xff {
            self.p += 1;
            self.varint()?; // version
        }
        let v = self.value()?;
        Ok(v)
    }

    fn peek(&self) -> R<u8> {
        self.b.get(self.p).copied().ok_or("eof")
    }
    fn byte(&mut self) -> R<u8> {
        let c = self.peek()?;
        self.p += 1;
        Ok(c)
    }
    fn bytes(&mut self, n: usize) -> R<&'a [u8]> {
        let s = self.b.get(self.p..self.p + n).ok_or("eof")?;
        self.p += n;
        Ok(s)
    }
    fn varint(&mut self) -> R<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let c = self.byte()?;
            if shift < 64 {
                v |= u64::from(c & 0x7f) << shift;
            }
            if c & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
        }
    }
    fn zigzag(&mut self) -> R<i64> {
        let v = self.varint()?;
        Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
    }
    fn double(&mut self) -> R<f64> {
        Ok(f64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn string_body(&mut self, tag: u8) -> R<String> {
        let n = self.varint()? as usize;
        let raw = self.bytes(n)?;
        Ok(match tag {
            b'"' => raw.iter().map(|&c| c as char).collect(), // Latin-1
            b'c' => String::from_utf16_lossy(
                &raw.as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect::<Vec<_>>(),
            ),
            _ => String::from_utf8_lossy(raw).into_owned(),
        })
    }

    fn value(&mut self) -> R<String> {
        loop {
            let tag = self.byte()?;
            return Ok(match tag {
                0x00 => continue, // padding
                b'_' => "{\"$undefined\":true}".into(),
                b'0' => "null".into(),
                b'T' => "true".into(),
                b'F' => "false".into(),
                b'I' => self.zigzag()?.to_string(),
                b'U' => self.varint()?.to_string(),
                b'N' => number(self.double()?),
                b'Z' => format!("{{\"$bigint\":\"{}\"}}", self.bigint()?),
                b'"' | b'c' | b'S' => json_string(&self.string_body(tag)?),
                b'D' => {
                    self.id();
                    format!("{{\"$date\":{}}}", json_string(&iso8601(self.double()?)))
                }
                b'o' => {
                    self.id();
                    self.object(b'{')?
                }
                b'A' => {
                    self.id();
                    let len = self.varint()? as usize;
                    let mut items = Vec::with_capacity(len.min(1 << 16));
                    for _ in 0..len {
                        items.push(if self.peek()? == b'-' {
                            self.p += 1;
                            "null".into() // hole
                        } else {
                            self.value()?
                        });
                    }
                    // trailing properties until '$'
                    while self.peek()? != b'$' {
                        self.value()?;
                        self.value()?;
                    }
                    self.p += 1;
                    self.varint()?;
                    self.varint()?;
                    format!("[{}]", items.join(","))
                }
                b'a' => {
                    self.id();
                    let len = self.varint()? as usize;
                    let mut items = vec!["null".to_string(); len.min(1 << 16)];
                    while self.peek()? != b'@' {
                        let k = self.value()?;
                        let v = self.value()?;
                        if let Ok(i) = k.parse::<usize>() {
                            if i < items.len() {
                                items[i] = v;
                            }
                        }
                    }
                    self.p += 1;
                    self.varint()?;
                    self.varint()?;
                    format!("[{}]", items.join(","))
                }
                b';' => {
                    self.id();
                    let mut pairs = Vec::new();
                    while self.peek()? != b':' {
                        let k = self.value()?;
                        let v = self.value()?;
                        pairs.push(format!("[{k},{v}]"));
                    }
                    self.p += 1;
                    self.varint()?;
                    format!("{{\"$map\":[{}]}}", pairs.join(","))
                }
                b'\'' => {
                    self.id();
                    let mut items = Vec::new();
                    while self.peek()? != b',' {
                        items.push(self.value()?);
                    }
                    self.p += 1;
                    self.varint()?;
                    format!("{{\"$set\":[{}]}}", items.join(","))
                }
                b'y' | b'x' => {
                    self.id();
                    (if tag == b'y' { "true" } else { "false" }).into()
                }
                b'n' => {
                    self.id();
                    number(self.double()?)
                }
                b'z' => {
                    self.id();
                    format!("{{\"$bigint\":\"{}\"}}", self.bigint()?)
                }
                b's' => {
                    self.id();
                    let t = self.byte()?;
                    json_string(&self.string_body(t)?)
                }
                b'R' => {
                    self.id();
                    let t = self.byte()?;
                    let pattern = self.string_body(t)?;
                    let flags = self.varint()?;
                    format!(
                        "{{\"$regexp\":{},\"$flags\":{flags}}}",
                        json_string(&pattern)
                    )
                }
                b'B' => {
                    self.id();
                    let n = self.varint()? as usize;
                    let data = self.bytes(n)?.to_vec();
                    if self.peek().ok() == Some(b'V') {
                        self.p += 1;
                        self.view(&data)?
                    } else {
                        format!(
                            "{{\"$bytes\":\"{}\",\"$kind\":\"ArrayBuffer\"}}",
                            base64(&data)
                        )
                    }
                }
                b'^' => format!("{{\"$ref\":{}}}", self.varint()?),
                _ => return Err("unsupported V8 tag"),
            });
        }
    }

    /// JS object properties until `end`, then the property count.
    fn object(&mut self, end: u8) -> R<String> {
        let mut props = Vec::new();
        while self.peek()? != end {
            let k = self.value()?;
            let v = self.value()?;
            let key = if k.starts_with('"') {
                k
            } else {
                json_string(&k)
            };
            props.push(format!("{key}:{v}"));
        }
        self.p += 1;
        self.varint()?;
        Ok(format!("{{{}}}", props.join(",")))
    }

    fn view(&mut self, buffer: &[u8]) -> R<String> {
        self.id();
        let kind = match self.byte()? {
            b'b' => "Int8Array",
            b'B' => "Uint8Array",
            b'C' => "Uint8ClampedArray",
            b'w' => "Int16Array",
            b'W' => "Uint16Array",
            b'd' => "Int32Array",
            b'D' => "Uint32Array",
            b'h' => "Float16Array",
            b'f' => "Float32Array",
            b'F' => "Float64Array",
            b'q' => "BigInt64Array",
            b'Q' => "BigUint64Array",
            b'?' => "DataView",
            _ => return Err("unknown view type"),
        };
        let offset = self.varint()? as usize;
        let len = self.varint()? as usize;
        // Version ≥ 14 writes view flags.
        if self.peek().is_ok_and(|c| c < 0x10) {
            self.varint()?;
        }
        let data = buffer.get(offset..offset + len).ok_or("view past buffer")?;
        Ok(format!(
            "{{\"$bytes\":\"{}\",\"$kind\":\"{kind}\"}}",
            base64(data)
        ))
    }

    fn bigint(&mut self) -> R<String> {
        let bitfield = self.varint()?;
        let negative = bitfield & 1 == 1;
        let n = (bitfield >> 1) as usize;
        let digits = self.bytes(n)?;
        // little-endian magnitude → decimal
        let mut limbs: Vec<u32> = digits
            .chunks(4)
            .map(|c| {
                let mut b = [0u8; 4];
                b[..c.len()].copy_from_slice(c);
                u32::from_le_bytes(b)
            })
            .collect();
        let mut out = Vec::new();
        while limbs.iter().any(|&l| l != 0) {
            let mut rem = 0u64;
            for l in limbs.iter_mut().rev() {
                let cur = (rem << 32) | u64::from(*l);
                *l = (cur / 10) as u32;
                rem = cur % 10;
            }
            out.push(b'0' + rem as u8);
        }
        if out.is_empty() {
            out.push(b'0');
        }
        out.reverse();
        let s = String::from_utf8(out).unwrap();
        Ok(if negative { format!("-{s}") } else { s })
    }
}

fn number(f: f64) -> String {
    if f.is_finite() {
        if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
            format!("{}", f as i64)
        } else {
            format!("{f:?}")
        }
    } else if f.is_nan() {
        "\"NaN\"".into()
    } else if f > 0.0 {
        "\"Infinity\"".into()
    } else {
        "\"-Infinity\"".into()
    }
}

/// ms since epoch → RFC 3339 UTC with milliseconds.
fn iso8601(ms: f64) -> String {
    if !ms.is_finite() {
        return "Invalid Date".into();
    }
    let ms = ms as i64;
    let (days, rem) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    // civil from days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn j(bytes: &[u8]) -> String {
        decode(&Value::Blob(bytes.to_vec())).value_json.unwrap()
    }

    #[test]
    fn scalars() {
        assert_eq!(j(&[0xff, 0x0f, b'I', 0xd0, 0x01]), "104"); // the live counter value
        assert_eq!(j(&[0xff, 0x0f, b'"', 3, b'a', b'b', b'c']), "\"abc\"");
        assert_eq!(j(&[0xff, 0x0f, b'T']), "true");
        assert_eq!(j(&[0xff, 0x0f, b'_']), "{\"$undefined\":true}");
        let mut d = vec![0xff, 0x0f, b'N'];
        d.extend_from_slice(&1.5f64.to_le_bytes());
        assert_eq!(j(&d), "1.5");
    }

    #[test]
    fn objects_dates_maps() {
        // {a: 1} then end-object with 1 property
        assert_eq!(
            j(&[0xff, 0x0f, b'o', b'"', 1, b'a', b'I', 2, b'{', 1]),
            "{\"a\":1}"
        );
        let mut d = vec![0xff, 0x0f, b'D'];
        d.extend_from_slice(&0f64.to_le_bytes());
        assert_eq!(j(&d), "{\"$date\":\"1970-01-01T00:00:00.000Z\"}");
        // Map {1 => 'x'}
        assert_eq!(
            j(&[0xff, 0x0f, b';', b'I', 2, b'"', 1, b'x', b':', 2]),
            "{\"$map\":[[1,\"x\"]]}"
        );
        // BigInt -5: bitfield = (1 byte << 1) | sign... digits length 1 → bitfield = 1<<1 | 1 = 3
        assert_eq!(j(&[0xff, 0x0f, b'Z', 3, 5]), "{\"$bigint\":\"-5\"}");
    }

    #[test]
    fn stubs_and_legacy() {
        assert_eq!(decode(&Value::Blob(vec![1, 2, 3])).value_type, KvType::Stub);
        assert_eq!(
            decode(&Value::Text("{\"a\":1}".into())).value_type,
            KvType::LegacyText
        );
    }
}
