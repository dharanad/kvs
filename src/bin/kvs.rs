use clap::Parser;

use kvs::{KvStore, Result};
use kvs::{Cli, Command};

fn main() -> Result<()> {
    let mut _kvs = KvStore::new();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Get(_args)) => {
            // kvs.get(args.key);
            panic!("unimplemented")
        }
        Some(Command::Set(_args)) => {
            // kvs.set(args.key, args.value);
            panic!("unimplemented")
        }
        Some(Command::Remove(_args)) => {
            // kvs.remove(args.key)
            panic!("unimplemented")
        }
        None => {
            panic!("unimplemented")
        }
    }
}
