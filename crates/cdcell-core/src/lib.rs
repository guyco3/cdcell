//! cdcell-core: turn celld LTX units into row-level change events.
//!
//! No I/O. The pipeline (intake, confirmation, commits) lives elsewhere; this
//! crate decodes LTX files and bundles (via `celld-ltx`) and diffs SQLite pages
//! into row changes (design §6).

pub mod btree;
pub mod decoder;
pub mod format;
pub mod kv;
pub mod ltx;
pub mod schema;
pub mod value;

pub use decoder::{Changes, Decoder, Op, Row, RowChange, SchemaChange};
pub use value::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("malformed database: {0}")]
    Malformed(&'static str),
    #[error("page {0} is not a b-tree page (type {1:#04x})")]
    NotBtree(u32, u8),
    #[error("page {0} is missing")]
    MissingPage(u32),
    #[error("schema: {0}")]
    Schema(String),
    #[error("ltx: {0}")]
    Ltx(String),
}

pub type Result<T> = std::result::Result<T, Error>;
