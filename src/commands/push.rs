use anyhow::{bail, Result};

use crate::commands::{materialize, serialize};
use crate::git_utils;

const MAX_RETRIES: u32 = 5;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let ns = git_utils::get_namespace(&repo)?;

    // Resolve which remote to push to
    let remote_name = git_utils::resolve_meta_remote(&repo, remote)?;
    let local_ref = git_utils::local_ref(&repo)?;
    let remote_refspec = format!("refs/{}/main", ns);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] local ref: {}", local_ref);
        eprintln!("[verbose] remote refspec: {}", remote_refspec);
    }

    // Serialize local metadata to the local ref
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Verify we have something to push
    if repo.find_reference(&local_ref).is_err() {
        bail!("nothing to push (no local metadata ref)");
    }

    // Check if local ref already matches the remote ref (nothing new to push)
    let remote_tracking_ref = format!("refs/{}/remotes/main", ns);
    let local_oid = repo
        .find_reference(&local_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());
    let remote_oid = repo
        .find_reference(&remote_tracking_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    if let (Some(local), Some(remote)) = (local_oid, remote_oid) {
        if local == remote {
            println!("Everything up-to-date");
            return Ok(());
        }
    }

    // Try push with retry loop for non-fast-forward failures
    let push_refspec = format!("{}:{}", local_ref, remote_refspec);

    for attempt in 1..=MAX_RETRIES {
        if verbose {
            eprintln!("[verbose] push attempt {}/{}", attempt, MAX_RETRIES);
        }

        eprintln!("Pushing to {}...", remote_name);
        let result = git_utils::run_git(&repo, &["push", &remote_name, &push_refspec]);

        match result {
            Ok(_) => {
                println!("Pushed metadata to {} ({})", remote_name, remote_refspec);
                return Ok(());
            }
            Err(e) => {
                let err_msg = e.to_string();
                let is_non_ff = err_msg.contains("non-fast-forward")
                    || err_msg.contains("rejected")
                    || err_msg.contains("fetch first");

                if !is_non_ff || attempt == MAX_RETRIES {
                    bail!("push failed: {}", err_msg);
                }

                eprintln!(
                    "Push rejected (remote has new data), fetching and merging (attempt {}/{})...",
                    attempt, MAX_RETRIES
                );

                // Fetch latest remote data
                let fetch_refspec =
                    format!("{}:refs/{}/remotes/main", remote_refspec, ns);
                git_utils::run_git(
                    &repo,
                    &["fetch", &remote_name, &fetch_refspec],
                )?;

                // Materialize the remote data (merge into local DB)
                materialize::run(None, false, verbose)?;

                // Re-serialize with merged data
                eprintln!("Re-serializing after merge...");
                serialize::run(verbose)?;
            }
        }
    }

    bail!("push failed after {} attempts", MAX_RETRIES);
}
