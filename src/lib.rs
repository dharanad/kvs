#![deny(missing_docs)]
//! A key-value store library

mod cli;
mod kv;

pub use cli::{Cli, Command};
pub use kv::{KvStore, Result};
