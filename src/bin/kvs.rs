use clap::Parser;
use kvs::KvStore;
use kvs::cli::{Cli, Command};
fn main() {
    let mut _kvs = KvStore::new();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Get(args)) => {
            // kvs.get(args.key);
            panic!("unimplemented")
        }
        Some(Command::Set(args)) => {
            // kvs.set(args.key, args.value);
            panic!("unimplemented")
        }
        Some(Command::Remove(args)) => {
            // kvs.remove(args.key)
            panic!("unimplemented")
        }
        None => {
            panic!("unimplemented")
        }
    }
}