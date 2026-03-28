use anyhow::Result;

use crate::commands::{materialize, serialize};
use crate::git_utils;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let ns = git_utils::get_namespace(&repo)?;

    let remote_name = git_utils::resolve_meta_remote(&repo, remote)?;
    let remote_refspec = format!("refs/{}/main", ns);
    let fetch_refspec = format!("{}:refs/{}/remotes/main", remote_refspec, ns);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] fetch refspec: {}", fetch_refspec);
    }

    // Fetch latest remote metadata
    eprintln!("Fetching metadata from {}...", remote_name);
    git_utils::run_git(&repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Serialize local state so materialize can do a proper 3-way merge
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Materialize: merge remote tree into local DB
    eprintln!("Materializing remote metadata...");
    materialize::run(None, false, verbose)?;

    println!("Pulled metadata from {}", remote_name);
    Ok(())
}
