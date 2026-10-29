//! redis-rs: a RESP2 Redis subset.
//!
//! The crate is a library plus a thin binary (`src/main.rs`) so that the
//! integration tests and the criterion benches can link the real parser, store
//! and server instead of re-implementing them.
//!
//! Module map (read in this order):
//! * [`resp`]    - RESP2 frames, encoder, incremental decoders
//! * [`store`]   - sharded keyspace; the only code that touches data
//! * [`expiry`]  - clock + background sweeper
//! * [`aof`]     - append-only file: writer threads, fsync policy, replay
//! * [`command`] - command table, argument validation, handlers
//! * [`conn`]    - one client connection (read, execute, reply)
//! * [`server`]  - accept loop, graceful shutdown
//! * [`signal`]  - SIGINT/SIGTERM and a tiny stop-flag for background threads

#![forbid(unsafe_code)]
// Hard rule (ARCHITECTURE.md): never hold a std::sync guard across an .await.
// Also denied crate-wide in Cargo.toml `[lints.clippy]`; CI runs clippy with -D warnings.
#![deny(clippy::await_holding_lock)]
#![deny(clippy::await_holding_refcell_ref)]

pub mod aof;
pub mod command;
pub mod conn;
pub mod expiry;
pub mod resp;
pub mod server;
pub mod signal;
pub mod store;
