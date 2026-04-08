#![allow(
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::unwrap_used,
    clippy::expect_used
)]

/// Database read performance benchmarks.
pub mod db;
/// Fanout (sharding) scheme comparison benchmarks.
pub mod fanout;
/// Commit history generation and traversal benchmarks.
pub mod history;
/// Serialization roundtrip benchmarks.
pub mod serialize;
