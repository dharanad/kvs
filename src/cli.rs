use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>
}

#[derive(Subcommand)]
pub enum Command {
    // gets the value of a given key
    Get(GetArgs),
    // sets the value of a key
    Set(SetArgs),
    // removes a given key
    #[clap(name = "rm")]
    Remove(RemoveArgs)
}
#[derive(Args)]
pub struct GetArgs {
    pub key: String
}
#[derive(Args)]
pub struct SetArgs {
    pub key: String,
    pub value: String
}
#[derive(Args)]
pub struct RemoveArgs {
    pub key: String
}