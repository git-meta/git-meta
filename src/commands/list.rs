use anyhow::Result;
use chrono::Utc;

use crate::db::Db;
use crate::git_utils;
use crate::types::{validate_key, Target};

pub fn run_push(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    db.list_push(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &email,
        timestamp,
    )?;

    Ok(())
}

pub fn run_rm(target_str: &str, key: &str, index: Option<usize>) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let entries = db.list_entries(target.type_str(), target.value_str(), key)?;

    match index {
        None => {
            // Display mode: show entries with indices
            if entries.is_empty() {
                println!("(empty list)");
            } else {
                for (i, entry) in entries.iter().enumerate() {
                    let preview = if entry.value.len() > 80 {
                        format!("{}...", &entry.value[..77])
                    } else {
                        entry.value.clone()
                    };
                    println!("[{}] {}", i, preview);
                }
            }
        }
        Some(idx) => {
            let email = git_utils::get_email(&repo)?;
            let timestamp = Utc::now().timestamp_millis();
            db.list_rm(
                target.type_str(),
                target.value_str(),
                key,
                idx,
                &email,
                timestamp,
            )?;
        }
    }

    Ok(())
}

pub fn run_pop(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    db.list_pop(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &email,
        timestamp,
    )?;

    Ok(())
}
