use anyhow::Result;

use git_meta_lib::tree_paths;
use git_meta_lib::types::{Target, TargetType, ValueType};

/// Hydrate promised entries by looking up their blob OIDs in the tip tree
/// and fetching any that aren't already local.
///
/// Returns the number of metadata entries resolved.
pub(super) fn hydrate_promised_entries(
    session: &git_meta_lib::Session,
    target_type: &TargetType,
    entries: &[(String, String)],
) -> Result<usize> {
    let db = session.store();
    let ns = session.namespace();
    let tracking_ref = format!("refs/{ns}/remotes/main");

    let Some(tip_commit) = session.find_ref_oid(&tracking_ref)? else {
        return Ok(0);
    };
    let tip_tree_id = session.commit_info(&tip_commit)?.tree_oid;

    struct PendingEntry {
        idx: usize,
        oids: Vec<String>,
        value_type: ValueType,
    }

    let mut pending: Vec<PendingEntry> = Vec::new();
    let mut not_found: Vec<usize> = Vec::new();

    for (idx, (target_value, key)) in entries.iter().enumerate() {
        let entry_target = entry_target(target_type, target_value);

        if let Ok(path) = tree_paths::tree_path(&entry_target, key) {
            if let Some(oid) = session.find_blob_oid_in_tree(&tip_tree_id, &path)? {
                pending.push(PendingEntry {
                    idx,
                    oids: vec![oid],
                    value_type: ValueType::String,
                });
                continue;
            }
        }

        if let Ok(path) = tree_paths::list_dir_path(&entry_target, key) {
            if let Some(dir_oid) = session.find_blob_oid_in_tree(&tip_tree_id, &path)? {
                let oids = blob_oids_from_tree(session, &dir_oid)?;
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::List,
                    });
                    continue;
                }
            }
        }

        if let Ok(set_path) = tree_paths::set_dir_path(&entry_target, key) {
            if let Some(dir_oid) = session.find_blob_oid_in_tree(&tip_tree_id, &set_path)? {
                let oids = blob_oids_from_tree(session, &dir_oid)?;
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::Set,
                    });
                    continue;
                }
            }
        }

        not_found.push(idx);
    }

    for idx in &not_found {
        let (target_value, key) = &entries[*idx];
        db.delete_promised(&entry_target(target_type, target_value), key)?;
    }

    if pending.is_empty() {
        return Ok(0);
    }

    let all_oids: Vec<String> = pending
        .iter()
        .flat_map(|p| p.oids.iter().cloned())
        .collect();
    let mut missing: Vec<String> = Vec::new();
    for oid in &all_oids {
        if session.read_blob_string(oid).is_err() {
            missing.push(oid.clone());
        }
    }

    if !missing.is_empty() {
        let remote_name = session.resolve_remote(None)?;
        eprintln!(
            "Fetching {} blob{} from remote...",
            missing.len(),
            if missing.len() == 1 { "" } else { "s" }
        );
        session.fetch_blob_oids(&remote_name, &missing)?;
    }

    let mut hydrated = 0;
    for entry in &pending {
        let (target_value, key) = &entries[entry.idx];
        let entry_target = entry_target(target_type, target_value);

        match entry.value_type {
            ValueType::String => {
                let oid = &entry.oids[0];
                let Ok(content) = session.read_blob_string(oid) else {
                    continue;
                };
                db.resolve_promised(&entry_target, key, &content, &ValueType::String, false)?;
                hydrated += 1;
            }
            ValueType::List => {
                let mut list_entries = Vec::new();
                for oid in &entry.oids {
                    if let Ok(s) = session.read_blob_string(oid) {
                        list_entries.push(s);
                    }
                }
                let json_value = serde_json::to_string(&list_entries)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::List, false)?;
                hydrated += 1;
            }
            ValueType::Set => {
                let mut set_members = Vec::new();
                for oid in &entry.oids {
                    if let Ok(s) = session.read_blob_string(oid) {
                        set_members.push(s);
                    }
                }
                set_members.sort();
                let json_value = serde_json::to_string(&set_members)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::Set, false)?;
                hydrated += 1;
            }
            _ => anyhow::bail!("unsupported value type"),
        }
    }

    Ok(hydrated)
}

fn entry_target(target_type: &TargetType, target_value: &str) -> Target {
    if *target_type == TargetType::Project {
        Target::project()
    } else {
        Target::from_parts(target_type.clone(), Some(target_value.to_string()))
    }
}

fn blob_oids_from_tree(session: &git_meta_lib::Session, tree_oid: &str) -> Result<Vec<String>> {
    Ok(session
        .tree_entries(tree_oid)?
        .into_iter()
        .filter(|entry| {
            !entry.name.starts_with("__")
                && entry.kind == git_meta_lib::session::GitTreeEntryKind::Blob
        })
        .map(|entry| entry.oid)
        .collect())
}
