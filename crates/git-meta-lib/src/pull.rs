//! Pull remote metadata: fetch, materialize, and index history.
//!
//! This module implements the full pull workflow: resolving the remote,
//! fetching the metadata ref, counting new commits, hydrating tip blobs,
//! serializing local state for merge, materializing remote changes, and
//! indexing historical keys for lazy loading.
//!
//! The public entry point is [`run()`], which takes a [`Session`](crate::Session)
//! and returns a [`PullOutput`] describing what happened.

use crate::error::Result;
use crate::git_utils;
use crate::session::Session;

/// Result of a pull operation.
///
/// Contains all the information needed by a CLI or other consumer
/// to report what happened, without performing any I/O itself.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullOutput {
    /// The remote that was pulled from.
    pub remote_name: String,
    /// Number of new commits fetched.
    pub new_commits: usize,
    /// Number of historical keys indexed for lazy loading.
    pub indexed_keys: usize,
    /// Whether materialization was performed.
    pub materialized: bool,
}

/// Pull remote metadata: fetch, materialize, and index history.
///
/// Resolves the remote, fetches the metadata ref, hydrates tip blobs,
/// serializes local state for merge, materializes remote changes, and
/// indexes historical keys for lazy loading.
///
/// # Parameters
///
/// - `session`: the git-meta session providing the repository, store, and config.
/// - `remote`: optional remote name to pull from. If `None`, the first
///   configured metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for database writes during materialization.
///
/// # Returns
///
/// A [`PullOutput`] describing the remote pulled from, new commits fetched,
/// whether materialization occurred, and how many history keys were indexed.
///
/// # Errors
///
/// Returns an error if the remote cannot be resolved, fetch fails,
/// materialization fails, or history indexing fails.
pub fn run(session: &Session, remote: Option<&str>, now: i64) -> Result<PullOutput> {
    let repo = &session.repo;
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let remote_refspec = format!("refs/{ns}/main");
    let config = repo.config_snapshot();
    let tracking_ref = if config.boolean(&format!("remote.{remote_name}.metaside")) == Some(true) {
        format!("refs/{ns}/remotes/{remote_name}/main")
    } else {
        format!("refs/{ns}/remotes/main")
    };
    let fetch_refspec = format!("{remote_refspec}:{tracking_ref}");

    // Record the old tip so we can count new commits
    let old_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Fetch latest remote metadata
    git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Get the new tip
    let new_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Check if we need to materialize even if no new commits were fetched.
    // A previous pull may have advanced the tracking ref before failing during
    // hydration or materialization, so also verify the local ref contains the
    // fetched remote tip before treating this pull as a no-op.
    let local_ref = session.local_ref();
    let last_materialized_missing = session.store.get_last_materialized()?.is_none();
    let local_ref_missing = repo.find_reference(&local_ref).is_err();
    let needs_materialize = last_materialized_missing
        || local_ref_missing
        || !local_ref_contains_tip(
            repo,
            &local_ref,
            new_tip.as_ref().map(|tip| (*tip).detach()),
        );

    // Count new commits
    let new_commits = match (old_tip.as_ref(), new_tip.as_ref()) {
        (Some(old), Some(new)) if old == new => {
            if !needs_materialize {
                return Ok(PullOutput {
                    remote_name,
                    new_commits: 0,
                    indexed_keys: 0,
                    materialized: false,
                });
            }
            0
        }
        (Some(old), Some(new)) => count_commits_between(repo, old.detach(), new.detach()),
        (None, Some(_)) => 1, // initial fetch, at least one commit
        _ => 0,
    };

    // Hydrate tip tree blobs so gix can read them
    let short_ref = tracking_ref
        .strip_prefix("refs/")
        .unwrap_or(&tracking_ref)
        .to_string();
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Serialize local state so materialize can do a proper 3-way merge
    let _ = crate::serialize::run(session, now, false)?;

    // Materialize: merge remote tree into local DB
    let _ = crate::materialize::run(session, None, now)?;

    // Insert promisor entries from non-tip commits so we know what keys exist
    // in the history even though we haven't fetched their blob data yet.
    // On first materialize, walk the entire history (pass None as old_tip).
    let indexed_keys = if let Some(new) = new_tip {
        let tracking_ref_unchanged = old_tip.as_ref() == Some(&new);
        let ref_state_needs_repair = tracking_ref_unchanged && needs_materialize;
        let full_history_index =
            last_materialized_missing || local_ref_missing || ref_state_needs_repair;
        let walk_from = if full_history_index {
            None
        } else {
            old_tip.map(gix::Id::detach)
        };
        session.index_history(new.detach(), walk_from)?
    } else {
        0
    };

    Ok(PullOutput {
        remote_name,
        new_commits,
        indexed_keys,
        materialized: true,
    })
}

fn local_ref_contains_tip(
    repo: &gix::Repository,
    local_ref: &str,
    remote_tip: Option<gix::ObjectId>,
) -> bool {
    let Some(remote_tip) = remote_tip else {
        return true;
    };
    let Some(local_tip) = repo
        .find_reference(local_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(gix::Id::detach)
    else {
        return false;
    };

    local_tip == remote_tip
        || repo
            .merge_base(local_tip, remote_tip)
            .is_ok_and(|base| base == remote_tip)
}

/// Count commits reachable from `new` but not from `old`.
fn count_commits_between(repo: &gix::Repository, old: gix::ObjectId, new: gix::ObjectId) -> usize {
    let walk = repo.rev_walk(Some(new)).with_boundary(Some(old));
    match walk.all() {
        // Subtract the boundary commit itself
        Ok(iter) => iter
            .filter(std::result::Result::is_ok)
            .count()
            .saturating_sub(1),
        Err(_) => 0,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use gix::refs::transaction::PreviousValue;

    use super::*;

    #[test]
    fn local_ref_contains_tip_when_local_descends_from_remote() {
        let (_dir, repo) = setup_repo();
        let remote_tip = write_commit(&repo, Vec::new());
        let local_tip = write_commit(&repo, vec![remote_tip]);
        repo.reference(
            "refs/meta/local/main",
            local_tip,
            PreviousValue::Any,
            "local metadata",
        )
        .unwrap();

        assert!(local_ref_contains_tip(
            &repo,
            "refs/meta/local/main",
            Some(remote_tip)
        ));
    }

    #[test]
    fn local_ref_does_not_contain_newer_remote_tip() {
        let (_dir, repo) = setup_repo();
        let local_tip = write_commit(&repo, Vec::new());
        let remote_tip = write_commit(&repo, vec![local_tip]);
        repo.reference(
            "refs/meta/local/main",
            local_tip,
            PreviousValue::Any,
            "local metadata",
        )
        .unwrap();

        assert!(!local_ref_contains_tip(
            &repo,
            "refs/meta/local/main",
            Some(remote_tip)
        ));
    }

    fn setup_repo() -> (tempfile::TempDir, gix::Repository) {
        let dir = tempfile::TempDir::new().unwrap();
        let _repo = gix::init(dir.path()).unwrap();
        let repo = gix::open_opts(
            dir.path(),
            gix::open::Options::isolated()
                .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
        )
        .unwrap();

        (dir, repo)
    }

    fn write_commit(repo: &gix::Repository, parents: Vec<gix::ObjectId>) -> gix::ObjectId {
        let tree_oid = repo.empty_tree().edit().unwrap().write().unwrap().detach();
        let sig = gix::actor::Signature {
            name: "Test User".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(946684800, 0),
        };
        let commit = gix::objs::Commit {
            message: "metadata".into(),
            tree: tree_oid,
            author: sig.clone(),
            committer: sig,
            encoding: None,
            parents: parents.into(),
            extra_headers: Default::default(),
        };
        repo.write_object(&commit).unwrap().detach()
    }
}
