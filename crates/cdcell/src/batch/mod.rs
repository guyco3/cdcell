// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! The batch architecture (docs/batch-design.md): per-run planning over
//! celld's per-cell objects, staged files, one fenced commit.
//!
//! Built up across the PR stack (docs/batch-implementation-plan.md); the
//! streaming pipeline stays in place until it is removed.

pub mod chain;
pub mod obj;
pub mod partition;
pub mod plan;
