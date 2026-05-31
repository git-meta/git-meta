#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

#[test]
fn set_and_get_string_value() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    let handle = session.target(&target);

    handle.set("agent:model", "claude-4.6").unwrap();

    let value = handle.get_value("agent:model").unwrap();
    assert!(value.is_some(), "expected a value for agent:model");
    let value = value.unwrap();
    assert_eq!(value, MetaValue::String("claude-4.6".to_string()));
    assert_eq!(value.value_type(), ValueType::String);
}

#[test]
fn set_and_get_list_value() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::branch("feature-x");
    let handle = session.target(&target);

    handle.list_push("review:comments", "first").unwrap();
    handle.list_push("review:comments", "second").unwrap();
    handle.list_push("review:comments", "third").unwrap();

    let value = handle.get_value("review:comments").unwrap();
    assert!(value.is_some());
    let value = value.unwrap();
    assert_eq!(value.value_type(), ValueType::List);

    if let MetaValue::List(entries) = &value {
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].value, "first");
        assert_eq!(entries[1].value, "second");
        assert_eq!(entries[2].value, "third");
    } else {
        panic!("expected MetaValue::List, got {value:?}");
    }
}

#[test]
fn set_and_get_set_value() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "charlie").unwrap();
    // Duplicate -- should not increase count
    handle.set_add("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap();
    assert!(value.is_some());
    let value = value.unwrap();
    assert_eq!(value.value_type(), ValueType::Set);

    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 3);
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
        assert!(members.contains("charlie"));
    } else {
        panic!("expected MetaValue::Set, got {value:?}");
    }
}

#[test]
fn remove_key() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    let handle = session.target(&target);

    handle.set("agent:model", "claude-4.6").unwrap();
    assert!(handle.get_value("agent:model").unwrap().is_some());

    let removed = handle.remove("agent:model").unwrap();
    assert!(removed, "remove should return true for existing key");

    let value = handle.get_value("agent:model").unwrap();
    assert!(value.is_none(), "value should be gone after remove");
}

#[test]
fn publish_local_publishes_key_prefix_without_rewriting_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());
    let list_entries = vec![
        ListEntry {
            value: "first".to_string(),
            timestamp: 10,
        },
        ListEntry {
            value: "second".to_string(),
            timestamp: 20,
        },
    ];

    handle.set("local:agent:session:s1:title", "draft").unwrap();
    handle
        .set("local:agent:session:s1:turns", list_entries.clone())
        .unwrap();
    handle
        .set_add("local:agent:session:s1:tags", "review")
        .unwrap();

    handle
        .publish_local([LocalPublish::key_prefix(
            "local:agent:session:s1",
            "agent:session:s1",
        )])
        .unwrap();

    assert_eq!(
        handle.get_value("agent:session:s1:title").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert_eq!(
        handle.get_value("agent:session:s1:turns").unwrap(),
        Some(MetaValue::List(list_entries))
    );
    let Some(MetaValue::Set(tags)) = handle.get_value("agent:session:s1:tags").unwrap() else {
        panic!("expected published set");
    };
    assert!(tags.contains("review"));
    assert!(handle
        .get_value("local:agent:session:s1:title")
        .unwrap()
        .is_none());
}

#[test]
fn publish_local_publishes_exact_prefix_and_child_keys_without_siblings() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());

    handle.set("local:agent:session:s1", "root").unwrap();
    handle.set("local:agent:session:s1:title", "draft").unwrap();
    handle
        .set("local:agent:session:s10:title", "sibling")
        .unwrap();

    handle
        .publish_local([LocalPublish::key_prefix(
            "local:agent:session:s1",
            "agent:session:s1",
        )])
        .unwrap();

    assert_eq!(
        handle.get_value("agent:session:s1").unwrap(),
        Some(MetaValue::String("root".to_string()))
    );
    assert_eq!(
        handle.get_value("agent:session:s1:title").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert_eq!(
        handle.get_value("local:agent:session:s10:title").unwrap(),
        Some(MetaValue::String("sibling".to_string()))
    );
    assert!(handle
        .get_value("agent:session:s10:title")
        .unwrap()
        .is_none());
}

#[test]
fn publish_local_publishes_selected_set_members() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());

    handle.set_add("local:agent:sessions", "s1").unwrap();
    handle.set_add("local:agent:sessions", "s2").unwrap();

    let members = vec!["s1".to_string(), "s1".to_string()];
    handle
        .publish_local([LocalPublish::set_members(
            "local:agent:sessions",
            "agent:sessions",
            &members,
        )])
        .unwrap();

    let Some(MetaValue::Set(local)) = handle.get_value("local:agent:sessions").unwrap() else {
        panic!("expected source set");
    };
    assert!(!local.contains("s1"));
    assert!(local.contains("s2"));
    let Some(MetaValue::Set(public)) = handle.get_value("agent:sessions").unwrap() else {
        panic!("expected destination set");
    };
    assert!(public.contains("s1"));
    assert!(!public.contains("s2"));
}

#[test]
fn publish_local_requires_local_source_and_published_destination() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());

    assert!(
        handle
            .publish_local([LocalPublish::key_prefix(
                "agent:session:s1",
                "agent:session:s2"
            )])
            .is_err(),
        "source must be local-only"
    );
    assert!(
        handle
            .publish_local([LocalPublish::key_prefix(
                "local:agent:session:s1",
                "local:agent:session:s2"
            )])
            .is_err(),
        "destination must be published"
    );
}

#[test]
fn publish_local_rolls_back_batch_when_a_publish_fails() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());

    handle.set("local:agent:session:s1:title", "draft").unwrap();
    let members = vec!["s1".to_string()];

    let result = handle.publish_local([
        LocalPublish::key_prefix("local:agent:session:s1", "agent:session:s1"),
        LocalPublish::set_members("local:agent:missing", "agent:sessions", &members),
    ]);

    assert!(
        result.is_err(),
        "batch should fail on the missing source set"
    );
    assert_eq!(
        handle.get_value("local:agent:session:s1:title").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert!(handle
        .get_value("agent:session:s1:title")
        .unwrap()
        .is_none());
}

#[test]
fn publish_local_rolls_back_when_destination_key_exists() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);
    let handle = session.target(&Target::project());

    handle.set("local:agent:session:s1:title", "draft").unwrap();
    handle.set("agent:session:s1:title", "existing").unwrap();

    let result = handle.publish_local([LocalPublish::key_prefix(
        "local:agent:session:s1",
        "agent:session:s1",
    )]);

    assert!(
        result.is_err(),
        "batch should fail on destination collision"
    );
    assert_eq!(
        handle.get_value("local:agent:session:s1:title").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert_eq!(
        handle.get_value("agent:session:s1:title").unwrap(),
        Some(MetaValue::String("existing".to_string()))
    );
}

#[test]
fn all_target_types() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Commit target
    let commit_target = Target::commit(&sha).unwrap();
    session
        .target(&commit_target)
        .set("provenance", "ai-generated")
        .unwrap();

    // Path target
    let path_target = Target::path("src/main.rs");
    session.target(&path_target).set("owner", "teamA").unwrap();

    // Branch target
    let branch_target = Target::branch("feature-branch");
    session
        .target(&branch_target)
        .set("ci:status", "green")
        .unwrap();

    // Project target
    let project_target = Target::project();
    session
        .target(&project_target)
        .set("version", "1.0.0")
        .unwrap();

    // Change-id target
    let change_target = Target::change_id("jj-change-abc123");
    session
        .target(&change_target)
        .set("review:status", "approved")
        .unwrap();

    // Verify each independently
    assert_eq!(
        session
            .target(&commit_target)
            .get_value("provenance")
            .unwrap(),
        Some(MetaValue::String("ai-generated".to_string()))
    );
    assert_eq!(
        session.target(&path_target).get_value("owner").unwrap(),
        Some(MetaValue::String("teamA".to_string()))
    );
    assert_eq!(
        session
            .target(&branch_target)
            .get_value("ci:status")
            .unwrap(),
        Some(MetaValue::String("green".to_string()))
    );
    assert_eq!(
        session
            .target(&project_target)
            .get_value("version")
            .unwrap(),
        Some(MetaValue::String("1.0.0".to_string()))
    );
    assert_eq!(
        session
            .target(&change_target)
            .get_value("review:status")
            .unwrap(),
        Some(MetaValue::String("approved".to_string()))
    );
}
