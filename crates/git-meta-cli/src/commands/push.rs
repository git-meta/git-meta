use anyhow::{bail, Result};
use git_meta_lib::push::PushProgress;

use crate::context::CommandContext;

/// Push a README commit to refs/heads/main on the meta remote.
/// This only succeeds if the branch doesn't already exist (no force push).
pub(crate) fn run_readme(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    let remote_name = ctx.session.resolve_remote(remote)?;

    // Gather project info from git config
    let origin_url = ctx
        .session
        .git_config_string("remote.origin.url")
        .unwrap_or_else(|| "unknown".to_string());
    let meta_url_key = format!("remote.{remote_name}.url");
    let meta_url = ctx
        .session
        .git_config_string(&meta_url_key)
        .unwrap_or_else(|| "unknown".to_string());
    let ns = ctx.session.namespace();

    let readme_content = generate_readme(&origin_url, &meta_url, ns);

    if verbose {
        eprintln!("[verbose] remote: {remote_name}");
        eprintln!("[verbose] origin url: {origin_url}");
        eprintln!("[verbose] meta url: {meta_url}");
    }

    let commit_oid = ctx.session.commit_readme_to_ref(
        "refs/meta/tmp/readme",
        &readme_content,
        "Initial metadata repository setup\n\nCreated by git meta to provide documentation for contributors.",
        None,
        "git-meta: create README",
        false,
    )?;

    if verbose {
        eprintln!("[verbose] created commit: {commit_oid}");
    }

    // Push commit to refs/heads/main on the remote, but only if it doesn't exist.
    // We use a refspec without '+' so it fails if the ref already exists.
    let push_refspec = format!("{commit_oid}:refs/heads/main");

    if verbose {
        eprintln!("[verbose] push refspec: {push_refspec}");
    }

    eprintln!("Pushing README to {remote_name}...");
    let result = ctx.session.run_git(&["push", &remote_name, &push_refspec]);

    match result {
        Ok(_) => {
            println!("Pushed README to {remote_name} (refs/heads/main)");
            Ok(())
        }
        Err(e) => {
            let err_msg = e.to_string();
            if err_msg.contains("non-fast-forward")
                || err_msg.contains("rejected")
                || err_msg.contains("fetch first")
                || err_msg.contains("already exists")
            {
                bail!("refs/heads/main already exists on {remote_name}. The README can only be pushed to a new repository.");
            }
            bail!("push failed: {err_msg}");
        }
    }
}

fn generate_readme(origin_url: &str, meta_url: &str, namespace: &str) -> String {
    format!(
        r#"# Git Metadata Repository

This repository stores structured metadata for the project at:

    {origin_url}

It is managed by [git meta](https://github.com/git-meta/git-meta), a tool for associating
key-value metadata with Git objects (commits, branches, paths, and more) and syncing
them across repositories.

## How It Works

Metadata is stored locally in a SQLite database (`.git/git-meta.sqlite`) and serialized
into Git trees and commits under `refs/{namespace}/` refs for synchronization. This
repository serves as the shared remote for that metadata.

You do **not** need to clone this repository directly. Instead, configure it as a
metadata remote in your local checkout of the main project.

## Setup

1. Install git meta (see the [git meta README](https://github.com/git-meta/git-meta) for details).

2. In your local clone of the main project, add this repository as a metadata remote:

   ```
   git meta remote add {meta_url}
   ```

3. Pull existing metadata:

   ```
   git meta pull
   ```

4. You're ready to read and write metadata:

   ```
   git meta get commit:HEAD
   git meta set commit:HEAD review:status "approved"
   git meta push
   ```

## Contributing Metadata

- **Set values:** `git meta set <target> <key> <value>`
- **Read values:** `git meta get <target> [key]`
- **Push changes:** `git meta push`
- **Pull updates:** `git meta pull`

Target types include `commit:<sha>`, `branch:<name>`, `change-id:<id>`,
`path:<file>`, and `project` (for repo-wide metadata).

See `git meta --help` for the full command reference.

## Important Notes

- Metadata is stored on `refs/{namespace}/main`, not on `refs/heads/main`.
  The `main` branch you see here is just this README for orientation.
- Never push directly to `refs/{namespace}/main` — always use `git meta push`,
  which handles serialization and conflict resolution.
- Metadata can be pruned over time. See `git meta config:prune` for auto-prune rules.
"#
    )
}

const MAX_RETRIES: u32 = 5;

pub(crate) fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let resolved_remote = ctx.session.resolve_remote(remote)?;

    if verbose {
        let ns = ctx.session.namespace();
        let local_ref = format!("refs/{ns}/local/main");
        let remote_refspec = format!("refs/{ns}/main");
        eprintln!("[verbose] remote: {resolved_remote}");
        eprintln!("[verbose] local ref: {local_ref}");
        eprintln!("[verbose] remote refspec: {remote_refspec}");
    }

    for attempt in 1..=MAX_RETRIES {
        if verbose {
            eprintln!("[verbose] push attempt {attempt}/{MAX_RETRIES}");
        }

        eprintln!("Pushing to {resolved_remote}...");
        let output = ctx
            .session
            .push_once_with_progress(remote, print_push_progress)?;

        if output.success {
            if output.up_to_date {
                println!("Everything up-to-date");
            } else {
                println!(
                    "Pushed metadata to {} ({})",
                    output.remote_name, output.remote_ref
                );
            }
            return Ok(());
        }

        if !output.non_fast_forward || attempt == MAX_RETRIES {
            bail!("push failed");
        }

        eprintln!(
            "Push rejected (remote has new data), fetching and merging (attempt {attempt}/{MAX_RETRIES})..."
        );

        ctx.session
            .resolve_push_conflict_with_progress(remote, print_push_progress)?;

        if verbose {
            eprintln!("[verbose] conflict resolved, retrying push");
        }
    }

    bail!("push failed after {MAX_RETRIES} attempts");
}

fn print_push_progress(event: PushProgress) {
    match event {
        PushProgress::CheckingLocalState => {
            eprintln!("  checking local and remote metadata refs...");
        }
        PushProgress::Serializing => {
            eprintln!("  serializing local metadata...");
        }
        PushProgress::SerializationSkipped => {
            eprintln!("  local metadata is already serialized.");
        }
        PushProgress::RebasingLocal => {
            eprintln!("  rebasing local metadata onto remote tip...");
        }
        PushProgress::Pushing {
            remote_name,
            local_ref,
            remote_ref,
        } => {
            eprintln!("  pushing {local_ref} to {remote_name}:{remote_ref}...");
        }
        PushProgress::FetchingRemote {
            remote_name,
            remote_ref,
        } => {
            eprintln!("  fetching {remote_name}:{remote_ref}...");
        }
        PushProgress::HydratingRemoteTip => {
            eprintln!("  hydrating remote metadata blobs...");
        }
        PushProgress::MaterializingRemote => {
            eprintln!("  materializing remote metadata...");
        }
        PushProgress::SerializingMerged => {
            eprintln!("  serializing merged metadata...");
        }
        PushProgress::RebasingMerged => {
            eprintln!("  rebasing merged metadata onto remote tip...");
        }
    }
}
