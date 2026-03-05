mod cli;
mod commands;
mod db;
mod git_utils;
mod list_value;
mod types;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Set {
            value_type,
            file,
            target,
            key,
            value,
        } => commands::set::run(
            &target,
            &key,
            value.as_deref(),
            file.as_deref(),
            &value_type,
        ),

        Commands::Get {
            json,
            with_authorship,
            target,
            key,
        } => commands::get::run(&target, key.as_deref(), json, with_authorship),

        Commands::Rm { target, key } => commands::rm::run(&target, &key),

        Commands::ListPush { target, key, value } => {
            commands::list::run_push(&target, &key, &value)
        }

        Commands::ListPop { target, key, value } => commands::list::run_pop(&target, &key, &value),

        Commands::Serialize => commands::serialize::run(),

        Commands::Materialize { remote, dry_run } => {
            commands::materialize::run(remote.as_deref(), dry_run)
        }
    }
}
