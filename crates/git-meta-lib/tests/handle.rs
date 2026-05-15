#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod helpers;

use git_meta_lib::*;
use helpers::*;
use serde::{Deserialize, Serialize};

#[test]
fn handle_set_convenience() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    // From<&str> conversion
    handle.set("config:key", "value-from-str").unwrap();
    assert_eq!(
        handle.get_value("config:key").unwrap(),
        Some(MetaValue::String("value-from-str".to_string()))
    );

    // From<String> conversion
    handle
        .set("config:key2", String::from("value-from-string"))
        .unwrap();
    assert_eq!(
        handle.get_value("config:key2").unwrap(),
        Some(MetaValue::String("value-from-string".to_string()))
    );
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct AgentSource<'a> {
    agent: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_version: Option<&'a str>,
}

#[test]
fn handle_set_record_writes_serialized_fields_under_prefix() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle
        .set_record(
            "agent-session:abc:source:def",
            AgentSource {
                agent: "codex",
                provider: Some("openai"),
                model: Some("gpt-5.5"),
                tool_version: Some("1.2.3"),
            },
        )
        .unwrap();

    assert_eq!(
        handle
            .get_value("agent-session:abc:source:def:agent")
            .unwrap(),
        Some(MetaValue::String("codex".to_string()))
    );
    assert_eq!(
        handle
            .get_value("agent-session:abc:source:def:provider")
            .unwrap(),
        Some(MetaValue::String("openai".to_string()))
    );
    assert_eq!(
        handle
            .get_value("agent-session:abc:source:def:tool-version")
            .unwrap(),
        Some(MetaValue::String("1.2.3".to_string()))
    );
}

#[test]
fn handle_set_record_skips_missing_fields() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);
    handle
        .set("agent-session:abc:source:def:provider", "old")
        .unwrap();

    handle
        .set_record(
            "agent-session:abc:source:def",
            AgentSource {
                agent: "codex",
                provider: None,
                model: None,
                tool_version: None,
            },
        )
        .unwrap();

    assert_eq!(
        handle
            .get_value("agent-session:abc:source:def:provider")
            .unwrap(),
        Some(MetaValue::String("old".to_string()))
    );
    assert_eq!(
        handle
            .get_value("agent-session:abc:source:def:model")
            .unwrap(),
        None
    );
}

#[test]
fn handle_set_record_requires_string_fields() {
    #[derive(Serialize)]
    struct BadRecord {
        count: u64,
    }

    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let error = handle
        .set_record("agent-session:abc", BadRecord { count: 1 })
        .unwrap_err();

    assert!(error.to_string().contains("field 'count'"));
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
struct StoredAgentSource {
    agent: String,
    provider: Option<String>,
    model: Option<String>,
    tool_version: Option<String>,
}

#[test]
fn handle_get_record_reads_serialized_fields_from_prefix() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle
        .set_record(
            "agent-session:abc:source:def",
            AgentSource {
                agent: "codex",
                provider: Some("openai"),
                model: None,
                tool_version: Some("1.2.3"),
            },
        )
        .unwrap();

    let source: StoredAgentSource = handle
        .get_record("agent-session:abc:source:def")
        .unwrap()
        .unwrap();

    assert_eq!(
        source,
        StoredAgentSource {
            agent: "codex".to_string(),
            provider: Some("openai".to_string()),
            model: None,
            tool_version: Some("1.2.3".to_string()),
        }
    );
}

#[test]
fn handle_get_record_returns_none_for_missing_prefix() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);
    let source: Option<StoredAgentSource> = handle.get_record("agent-session:missing").unwrap();

    assert_eq!(source, None);
}

#[test]
fn handle_get_all_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("agent:model", "claude").unwrap();
    handle.set("agent:provider", "anthropic").unwrap();
    handle.set("review:status", "approved").unwrap();

    // Filter by "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(agent_values.len(), 2);
    let keys: Vec<&str> = agent_values.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"agent:model"));
    assert!(keys.contains(&"agent:provider"));

    // No filter returns everything
    let all_values = handle.get_all_values(None).unwrap();
    assert_eq!(all_values.len(), 3);
}

#[test]
fn handle_list_operations() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::branch("main");
    let handle = session.target(&target);

    // Push entries
    handle.list_push("comments", "hello").unwrap();
    handle.list_push("comments", "world").unwrap();
    handle.list_push("comments", "goodbye").unwrap();

    // Read entries
    let entries = handle.list_entries("comments").unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].value, "hello");
    assert_eq!(entries[1].value, "world");
    assert_eq!(entries[2].value, "goodbye");

    // Pop a specific value
    handle.list_pop("comments", "world").unwrap();
    let entries = handle.list_entries("comments").unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].value, "hello");
    assert_eq!(entries[1].value, "goodbye");
}

#[test]
fn handle_set_operations() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "charlie").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 3);
    } else {
        panic!("expected Set");
    }

    handle.set_remove("owners", "bob").unwrap();
    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 2);
        assert!(members.contains("alice"));
        assert!(members.contains("charlie"));
        assert!(!members.contains("bob"));
    } else {
        panic!("expected Set");
    }
}
