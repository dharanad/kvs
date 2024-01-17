#![deny(missing_docs)]
//! A key-value store library

mod kv;
mod cli;

pub use kv::KvStore;
pub use cli::{Cli, Command};