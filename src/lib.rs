#![deny(missing_docs)]
//! A key-value store library

pub use cli::{Cli, Command};
pub use kv::KvStore;
use log_entry::LogEntry;

mod cli;
mod kv;
mod log_entry;
mod datafile;
mod index;

/// KvStore custom error
pub type Result<T> = anyhow::Result<T>;

