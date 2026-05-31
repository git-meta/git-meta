use anyhow::{bail, Result};

use crate::context::CommandContext;
use git_meta_lib::types::{TargetType, ValueType};

pub(crate) fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let ns = ctx.session.namespace();

    let tracking_ref = format!("refs/{ns}/remotes/main");
    let Some(tip_oid) = ctx.session.find_ref_oid(&tracking_ref)? else {
        bail!("no remote tracking ref ({tracking_ref}).\nAdd a remote first: git meta remote add <url>");
    };

    eprintln!("Walking history from {} ...", &tip_oid[..12]);

    let iter = ctx.session.rev_walk_oids(&tip_oid)?;

    let mut commits_walked = 0;
    let mut commits_parsed = 0;
    let mut commits_unparseable = 0;
    let mut inserted = 0;
    let mut skipped_existing = 0;
    let mut skipped_deletes = 0;
    let mut is_tip = true;

    for oid in iter {
        commits_walked += 1;
        let commit_info = ctx.session.commit_info(&oid)?;

        if is_tip {
            is_tip = false;
            let msg_first_line = commit_info.message.lines().next().unwrap_or("");
            eprintln!(
                "  {} (tip, skipped -- already materialized) {}",
                &oid[..12],
                msg_first_line,
            );
            continue;
        }

        let message = commit_info.message;
        let first_line = message.lines().next().unwrap_or("");

        match git_meta_lib::sync::parse_commit_changes(&message) {
            Some(changes) => {
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;
                let mut commit_deletes = 0;

                for change in &changes {
                    if change.op == 'D' {
                        commit_deletes += 1;
                        skipped_deletes += 1;
                        continue;
                    }
                    let tt = change.target_type.parse::<TargetType>()?;
                    let target = if tt == TargetType::Project {
                        git_meta_lib::types::Target::project()
                    } else {
                        git_meta_lib::types::Target::from_parts(
                            tt,
                            Some(change.target_value.clone()),
                        )
                    };
                    if ctx.session.store().insert_promised(
                        &target,
                        &change.key,
                        &ValueType::String,
                    )? {
                        commit_inserted += 1;
                        inserted += 1;
                    } else {
                        commit_skipped += 1;
                        skipped_existing += 1;
                    }
                }

                eprintln!(
                    "  {} ({} changes: +{} inserted, ~{} existing, -{} deletes) {}",
                    &oid[..12],
                    changes.len(),
                    commit_inserted,
                    commit_skipped,
                    commit_deletes,
                    first_line,
                );
            }
            None if commit_info.parent_count == 0
                || git_meta_lib::sync::commit_changes_omitted(&message) =>
            {
                let reason = if commit_info.parent_count == 0 {
                    "root"
                } else {
                    "changes omitted"
                };
                let keys = ctx.session.extract_keys_from_tree(&commit_info.tree_oid)?;
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;

                for (target_type, target_value, key) in &keys {
                    let tt = target_type.parse::<TargetType>()?;
                    let target = if tt == TargetType::Project {
                        git_meta_lib::types::Target::project()
                    } else {
                        git_meta_lib::types::Target::from_parts(tt, Some(target_value.clone()))
                    };
                    if ctx
                        .session
                        .store()
                        .insert_promised(&target, key, &ValueType::String)?
                    {
                        commit_inserted += 1;
                        inserted += 1;
                    } else {
                        commit_skipped += 1;
                        skipped_existing += 1;
                    }
                }

                eprintln!(
                    "  {} ({reason}, {} tree keys: +{} inserted, ~{} existing) {}",
                    &oid[..12],
                    keys.len(),
                    commit_inserted,
                    commit_skipped,
                    first_line,
                );
            }
            None => {
                commits_unparseable += 1;
                eprintln!("  {} (no change list) {}", &oid[..12], first_line,);
            }
        }
    }

    eprintln!();
    println!(
        "Walked {commits_walked} commits ({commits_parsed} parsed, {commits_unparseable} without change lists)",
    );
    println!(
        "Inserted {inserted} promisor keys ({skipped_existing} already existed, {skipped_deletes} deletes skipped)",
    );

    Ok(())
}
