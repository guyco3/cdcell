# The toolchain scripts/cargo.sh runs in: celld's Rust version (celld's
# Dockerfile builds on rust:1.98.1-bookworm) plus the lint/format components.
FROM rust:1.98.1-bookworm
RUN rustup component add clippy rustfmt
