use anyhow::{Context, Result};
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::db::Db;
use crate::git_utils;
use crate::types::Target;

pub fn run() -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let local_ref_name = git_utils::local_ref(&repo)?;
    let last_materialized = db.get_last_materialized()?;

    // Determine if we're doing incremental or full serialization
    // If we have a previous local ref commit, start from existing tree
    let existing_tree = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.tree().unwrap());

    // Build new tree entries
    let entries = if let Some(since) = last_materialized {
        // Incremental: only modified entries
        let modified = db.get_modified_since(since)?;
        if modified.is_empty() && existing_tree.is_some() {
            // Nothing changed
            return Ok(());
        }
        // We need the full metadata to rebuild the tree properly
        db.get_all_metadata()?
    } else {
        db.get_all_metadata()?
    };

    if entries.is_empty() {
        println!("no metadata to serialize");
        return Ok(());
    }

    let tree_oid = build_tree(&repo, &entries)?;

    // Create commit
    let email = git_utils::get_email(&repo)?;
    let sig = git2::Signature::now(&email, &email)?;

    let tree = repo.find_tree(tree_oid)?;

    // Find parent commit if exists
    let parent = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok());

    let parents: Vec<&git2::Commit> = parent.iter().collect();

    let commit_oid = repo.commit(Some(&local_ref_name), &sig, &sig, "", &tree, &parents)?;

    let now = Utc::now().timestamp_millis();
    db.set_last_materialized(now)?;

    println!(
        "serialized to {} ({})",
        local_ref_name,
        &commit_oid.to_string()[..8]
    );

    Ok(())
}

/// Build a complete Git tree from all metadata entries.
fn build_tree(
    repo: &git2::Repository,
    entries: &[(String, String, String, String, String, i64)],
) -> Result<git2::Oid> {
    // Collect all file paths -> blob content
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (target_type, target_value, key, value, value_type, last_timestamp) in entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        let base_path = target.tree_base_path();
        let key_path = key.replace(':', "/");

        match value_type.as_str() {
            "string" => {
                let raw_value: String = serde_json::from_str(value)
                    .context("failed to decode string value")?;
                let full_path = format!("{}/{}", base_path, key_path);
                files.insert(full_path, raw_value.into_bytes());
            }
            "list" => {
                let list: Vec<String> = serde_json::from_str(value)
                    .context("failed to decode list value")?;
                for (i, item) in list.iter().enumerate() {
                    let ts = last_timestamp + i as i64;
                    let mut hasher = Sha256::new();
                    hasher.update(item.as_bytes());
                    let hash = format!("{:x}", hasher.finalize());
                    let entry_name = format!("{}-{}", ts, &hash[..5]);
                    let full_path = format!("{}/{}/{}", base_path, key_path, entry_name);
                    files.insert(full_path, item.clone().into_bytes());
                }
            }
            _ => {}
        }
    }

    // Build nested tree from flat paths
    build_tree_from_paths(repo, &files)
}

/// Build a nested Git tree structure from flat file paths.
fn build_tree_from_paths(
    repo: &git2::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<git2::Oid> {
    // Build a nested structure
    #[derive(Default)]
    struct Dir {
        files: BTreeMap<String, Vec<u8>>,
        dirs: BTreeMap<String, Dir>,
    }

    fn insert_path(dir: &mut Dir, parts: &[&str], content: Vec<u8>) {
        if parts.len() == 1 {
            dir.files.insert(parts[0].to_string(), content);
        } else {
            let child = dir.dirs.entry(parts[0].to_string()).or_default();
            insert_path(child, &parts[1..], content);
        }
    }

    fn build_dir(repo: &git2::Repository, dir: &Dir) -> Result<git2::Oid> {
        let mut tb = repo.treebuilder(None)?;

        for (name, content) in &dir.files {
            let blob_oid = repo.blob(content)?;
            tb.insert(name, blob_oid, 0o100644)?;
        }

        for (name, child_dir) in &dir.dirs {
            let child_oid = build_dir(repo, child_dir)?;
            tb.insert(name, child_oid, 0o040000)?;
        }

        let oid = tb.write()?;
        Ok(oid)
    }

    let mut root = Dir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}
