use clap::Parser;

use kvs::{KvStore, Result};
use kvs::{Cli, Command};

fn main() -> Result<()> {
    let pwd = std::env::current_dir().unwrap();
    let mut kvs = KvStore::open(&pwd)?;
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Get(args)) => {
            match kvs.get(args.key)? {
                Some(v) => {
                    println!("{}", v)
                }
                None => {
                    println!("Key not found")
                }
            }
        }
        Some(Command::Set(args)) => {
            kvs.set(args.key, args.value)?;
        }
        Some(Command::Remove(args)) => {
            return match kvs.remove(args.key) {
                Ok(_) => {
                    Ok(())
                }
                Err(e) => {
                    println!("{}",e);
                    Err(e)
                }
            }
        }
        None => {
            panic!("unimplemented")
        }
    }
    Ok(())
}
