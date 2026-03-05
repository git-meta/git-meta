use anyhow::Result;
use serde_json::{json, Map, Value};

use crate::db::Db;
use crate::git_utils;
use crate::list_value::list_values_from_json;
use crate::types::Target;

const NODE_VALUE_KEY: &str = "__value";

pub fn run(
    target_str: &str,
    key: Option<&str>,
    json_output: bool,
    with_authorship: bool,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let entries = db.get_all(target.type_str(), target.value_str(), key)?;

    if entries.is_empty() {
        return Ok(());
    }

    if json_output {
        print_json(&db, &target, &entries, with_authorship)?;
    } else {
        print_plain(&entries)?;
    }

    Ok(())
}

fn print_plain(entries: &[(String, String, String)]) -> Result<()> {
    for (key, value, value_type) in entries {
        let display_value = format_value(value, value_type)?;
        println!("{}  {}", key, display_value);
    }
    Ok(())
}

fn format_value(value: &str, value_type: &str) -> Result<String> {
    match value_type {
        "string" => {
            // value is JSON-encoded string like "\"claude-4.6\""
            let s: String = serde_json::from_str(value)?;
            Ok(s)
        }
        "list" => {
            let list = list_values_from_json(value)?;
            Ok(format!("{:?}", list))
        }
        _ => Ok(value.to_string()),
    }
}

fn print_json(
    db: &Db,
    target: &Target,
    entries: &[(String, String, String)],
    with_authorship: bool,
) -> Result<()> {
    let mut root = Map::new();

    for (key, value, value_type) in entries {
        let parsed_value = parse_stored_value(value, value_type)?;

        let leaf_value = if with_authorship {
            let authorship = db.get_authorship(target.type_str(), target.value_str(), key)?;
            let (author, timestamp) = authorship.unwrap_or_else(|| ("unknown".to_string(), 0));
            json!({
                "value": parsed_value,
                "author": author,
                "timestamp": timestamp
            })
        } else {
            parsed_value
        };

        // Split key by ':' and nest into JSON object
        let parts: Vec<&str> = key.split(':').collect();
        insert_nested(&mut root, &parts, leaf_value);
    }

    let output = serde_json::to_string_pretty(&Value::Object(root))?;
    println!("{}", output);
    Ok(())
}

fn parse_stored_value(value: &str, value_type: &str) -> Result<Value> {
    match value_type {
        "string" => {
            let s: String = serde_json::from_str(value)?;
            Ok(Value::String(s))
        }
        "list" => {
            let list = list_values_from_json(value)?;
            Ok(Value::Array(list.into_iter().map(Value::String).collect()))
        }
        _ => Ok(serde_json::from_str(value)?),
    }
}

fn insert_nested(map: &mut Map<String, Value>, keys: &[&str], value: Value) {
    if keys.len() == 1 {
        let key = keys[0].to_string();
        match map.get_mut(&key) {
            None => {
                map.insert(key, value);
            }
            Some(existing) => {
                if let Value::Object(obj) = existing {
                    obj.insert(NODE_VALUE_KEY.to_string(), value);
                } else {
                    *existing = value;
                }
            }
        }
        return;
    }

    let entry = map
        .entry(keys[0].to_string())
        .or_insert_with(|| Value::Object(Map::new()));

    if !entry.is_object() {
        let previous = std::mem::replace(entry, Value::Null);
        let mut promoted = Map::new();
        promoted.insert(NODE_VALUE_KEY.to_string(), previous);
        *entry = Value::Object(promoted);
    }

    if let Value::Object(child_map) = entry {
        insert_nested(child_map, &keys[1..], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_nested_keeps_leaf_and_nested_values() {
        let mut root = Map::new();
        insert_nested(&mut root, &["agent"], json!("anthropic"));
        insert_nested(&mut root, &["agent", "model"], json!("claude-4.6"));

        assert_eq!(
            Value::Object(root),
            json!({
                "agent": {
                    "__value": "anthropic",
                    "model": "claude-4.6"
                }
            })
        );
    }

    #[test]
    fn test_insert_nested_keeps_leaf_and_nested_values_reverse_order() {
        let mut root = Map::new();
        insert_nested(&mut root, &["agent", "model"], json!("claude-4.6"));
        insert_nested(&mut root, &["agent"], json!("anthropic"));

        assert_eq!(
            Value::Object(root),
            json!({
                "agent": {
                    "__value": "anthropic",
                    "model": "claude-4.6"
                }
            })
        );
    }
}
