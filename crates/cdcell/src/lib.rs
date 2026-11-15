// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! cdcell: CDC from celld Durable Objects to Iceberg change logs.
//!
//! The library holds the service (`run`) and its parts; `main.rs` is the
//! CLI. Tests and the simulator drive [`pipeline::Pipeline`] directly
//! through [`intake`], with [`source::Store::memory`] buckets and an
//! [`out::MemSink`].

pub mod capture;
pub mod emit;
pub mod iceberg_out;
pub mod intake;
pub mod keys;
pub mod metrics;
pub mod out;
pub mod pipeline;
pub mod resync;
pub mod run;
pub mod source;
