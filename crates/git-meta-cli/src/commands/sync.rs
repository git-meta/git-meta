use anyhow::{bail, Result};

use crate::context::CommandContext;

const MAX_RETRIES: u32 = 5;

/// Pull remote metadata, merge it locally, then push the merged local state.
///
/// If `remote` is provided, only that metadata remote is synchronized. If it
/// is omitted, every configured metadata remote is pulled first, then every
/// configured metadata remote is pushed.
pub(crate) fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let remotes = remotes_to_sync(&ctx, remote)?;

    for remote_name in &remotes {
        pull_remote(&ctx, remote_name, verbose)?;
    }

    for remote_name in &remotes {
        push_remote(&ctx, remote_name, verbose)?;
    }

    if remotes.len() > 1 {
        println!("Synced metadata with {} remotes.", remotes.len());
    }

    Ok(())
}

fn remotes_to_sync(ctx: &CommandContext, remote: Option<&str>) -> Result<Vec<String>> {
    if let Some(name) = remote {
        return Ok(vec![ctx.session.resolve_remote(Some(name))?]);
    }

    let remotes = git_meta_lib::git_utils::list_meta_remotes(ctx.session.repo())?;
    if remotes.is_empty() {
        bail!("no metadata remotes configured");
    }

    Ok(remotes.into_iter().map(|(name, _url)| name).collect())
}

fn pull_remote(ctx: &CommandContext, remote_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        let ns = ctx.session.namespace();
        let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
        eprintln!("[verbose] remote: {remote_name}");
        eprintln!("[verbose] fetch refspec: {fetch_refspec}");
    }

    eprintln!("Pulling from {remote_name}...");
    let output = ctx.session.pull(Some(remote_name))?;

    if !output.materialized {
        println!("Already up-to-date from {}.", output.remote_name);
        return Ok(());
    }

    if output.new_commits > 0 {
        eprintln!(
            "Fetched {} new commit{} from {}.",
            output.new_commits,
            if output.new_commits == 1 { "" } else { "s" },
            output.remote_name
        );
    }

    if output.indexed_keys > 0 {
        eprintln!(
            "Indexed {} keys from {} history (available on demand).",
            output.indexed_keys, output.remote_name
        );
    }

    println!("Pulled metadata from {}", output.remote_name);
    Ok(())
}

fn push_remote(ctx: &CommandContext, remote_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        let ns = ctx.session.namespace();
        let local_ref = format!("refs/{ns}/local/main");
        let remote_refspec = format!("refs/{ns}/main");
        eprintln!("[verbose] remote: {remote_name}");
        eprintln!("[verbose] local ref: {local_ref}");
        eprintln!("[verbose] remote refspec: {remote_refspec}");
    }

    for attempt in 1..=MAX_RETRIES {
        if verbose {
            eprintln!("[verbose] push attempt {attempt}/{MAX_RETRIES}");
        }

        eprintln!("Pushing to {remote_name}...");
        let output = ctx.session.push_once(Some(remote_name))?;

        if output.success {
            if output.up_to_date {
                println!("Everything up-to-date on {}", output.remote_name);
            } else {
                println!(
                    "Pushed metadata to {} ({})",
                    output.remote_name, output.remote_ref
                );
            }
            return Ok(());
        }

        if !output.non_fast_forward || attempt == MAX_RETRIES {
            bail!("push to {remote_name} failed");
        }

        eprintln!(
            "Push to {remote_name} rejected (remote has new data), fetching and merging (attempt {attempt}/{MAX_RETRIES})..."
        );

        ctx.session.resolve_push_conflict(Some(remote_name))?;

        if verbose {
            eprintln!("[verbose] conflict resolved, retrying push");
        }
    }

    bail!("push to {remote_name} failed after {MAX_RETRIES} attempts");
}
