use clap::Parser;
use kvs::KvStore;
use kvs::cli::{Cli, Command};
fn main() {
    let mut kvs = KvStore::new();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Get(args)) => {
            kvs.get(args.key);
        }
        Some(Command::Set(args)) => {
            kvs.set(args.key, args.value);
        }
        Some(Command::Remove(args)) => {
            kvs.remove(args.key)
        }
        None => {
            unimplemented!()
        }
    }
}