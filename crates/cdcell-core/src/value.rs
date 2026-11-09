//! SQLite values as stored in records.

use std::fmt;

/// One SQLite value, by storage class.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Canonical JSON scalar used for keys (`pk_json`, design §8.5):
    /// integers and reals as numbers, text as strings, blobs as `{"$b64":…}`.
    pub fn to_json(&self) -> String {
        match self {
            Value::Null => "null".into(),
            Value::Integer(i) => i.to_string(),
            Value::Real(f) if f.is_finite() => {
                let s = format!("{f:?}");
                s
            }
            Value::Real(f) if *f > 0.0 => "\"Infinity\"".into(),
            Value::Real(_) => "\"-Infinity\"".into(),
            Value::Text(s) => json_string(s),
            Value::Blob(b) => format!("{{\"$b64\":\"{}\"}}", base64(b)),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json())
    }
}

/// `pk_json`: canonical JSON array of key values.
pub fn key_json(key: &[Value]) -> String {
    let parts: Vec<String> = key.iter().map(Value::to_json).collect();
    format!("[{}]", parts.join(","))
}

pub(crate) fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(crate) fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_forms() {
        assert_eq!(key_json(&[Value::Integer(42)]), "[42]");
        assert_eq!(key_json(&[Value::Text("a\"b".into()), Value::Null]), "[\"a\\\"b\",null]");
        assert_eq!(Value::Blob(b"hi".to_vec()).to_json(), "{\"$b64\":\"aGk=\"}");
        assert_eq!(Value::Real(1.5).to_json(), "1.5");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }
}
