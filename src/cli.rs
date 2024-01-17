use clap::{Args, Parser, Subcommand};

/// Command line interface struct.
#[derive(Parser)]
#[command(author, version, about)]
pub struct Cli {
    /// The command to run.
    #[command(subcommand)]
    pub command: Option<Command>,
}
/// Enum representing the possible commands.
#[derive(Subcommand)]
pub enum Command {
    /// Gets the value of a given key
    Get(GetArgs),
    /// Sets the value of a key
    Set(SetArgs),
    /// Removes a given key
    #[clap(name = "rm")]
    Remove(RemoveArgs),
}
/// Struct representing the arguments for the get command.
#[derive(Args)]
pub struct GetArgs {
    /// The key.
    pub key: String,
}
/// Struct representing the arguments for the set command.
#[derive(Args)]
pub struct SetArgs {
    /// The key.
    pub key: String,
    /// The value.
    pub value: String,
}
/// Struct representing the arguments for the remove command.
#[derive(Args)]
pub struct RemoveArgs {
    /// The key.
    pub key: String,
}
