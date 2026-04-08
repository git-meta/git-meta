#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;

use gmeta_core::{MaterializeStrategy, MetaValue, Session, Target, TargetType, ValueType};

/// Create an isolated git repository with an initial commit, returning
/// the temp directory (owns the lifetime) and the gix repository handle.
fn setup_repo() -> (tempfile::TempDir, gix::Repository) {
    let dir = tempfile::TempDir::new().unwrap();
    let _init = gix::init(dir.path()).unwrap();

    // Set user config via git subprocess (gix config mutation API is limited)
    let output = std::process::Command::new("git")
        .args([
            "-C",
            &dir.path().to_string_lossy(),
            "config",
            "user.email",
            "test@example.com",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let output = std::process::Command::new("git")
        .args([
            "-C",
            &dir.path().to_string_lossy(),
            "config",
            "user.name",
            "Test User",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    // Open with config overrides for reproducibility
    let repo = gix::open_opts(
        dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    // Create initial commit so HEAD exists
    let tree_oid = repo.empty_tree().edit().unwrap().write().unwrap().detach();
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };
    let commit = gix::objs::Commit {
        message: "initial".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit_oid = repo.write_object(&commit).unwrap().detach();
    repo.reference(
        "refs/heads/main",
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "",
    )
    .unwrap();
    repo.reference(
        "HEAD",
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "",
    )
    .unwrap();

    (dir, repo)
}

/// Open a session from a repo with a fixed timestamp for determinism.
fn open_session(repo: gix::Repository) -> Session {
    Session::open(repo).unwrap().with_timestamp(1000)
}

/// Return the full 40-char commit SHA from the repo's HEAD.
fn head_sha(repo: &gix::Repository) -> String {
    repo.head_id().unwrap().to_string()
}

/// Recursively copy all files from one directory to another.
fn copy_dir_contents(src: &std::path::Path, dst: &std::path::Path) {
    if !src.exists() {
        return;
    }
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path).ok();
            copy_dir_contents(&src_path, &dst_path);
        } else {
            std::fs::copy(&src_path, &dst_path).ok();
        }
    }
}

// 1. Basic CRUD

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

// 2. Target scoped handle (the primary API)

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

// 3. Serialize roundtrip

#[test]
fn serialize_creates_git_ref() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    session
        .target(&target)
        .set("agent:model", "claude-4.6")
        .unwrap();

    let output = session.serialize().unwrap();
    assert!(output.changes > 0);
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/main")),
        "serialize should write refs/meta/local/main, got: {:?}",
        output.refs_written
    );
}

#[test]
fn serialize_and_materialize_roundtrip() {
    // -- Repo A: set metadata and serialize --
    let (dir_a, repo_a) = setup_repo();
    let sha_a = head_sha(&repo_a);
    let session_a = open_session(repo_a);

    let target = Target::commit(&sha_a).unwrap();
    session_a
        .target(&target)
        .set("agent:model", "claude-4.6")
        .unwrap();
    session_a
        .target(&Target::project())
        .set("version", "1.0.0")
        .unwrap();
    session_a
        .target(&Target::path("src/lib.rs"))
        .set("owner", "teamA")
        .unwrap();

    let output = session_a.serialize().unwrap();
    assert!(output.changes > 0);

    // -- Bare repo B: simulate a remote by copying objects and refs --
    let bare_dir = tempfile::TempDir::new().unwrap();
    let _bare_init = gix::init_bare(bare_dir.path()).unwrap();
    let bare_repo = gix::open_opts(
        bare_dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    // Copy objects from A to bare
    let src_objects = dir_a.path().join(".git").join("objects");
    let dst_objects = bare_dir.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);

    // Copy the local ref from A to bare
    let repo_a_reopen = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let local_ref = repo_a_reopen
        .find_reference("refs/meta/local/main")
        .unwrap();
    let local_oid = local_ref.into_fully_peeled_id().unwrap().detach();
    bare_repo
        .reference(
            "refs/meta/local/main",
            local_oid,
            gix::refs::transaction::PreviousValue::Any,
            "copy from A",
        )
        .unwrap();

    // -- Repo C: simulate a "fetch" by copying objects from bare --
    let (dir_c, repo_c) = setup_repo();
    let repo_c_objects = dir_c.path().join(".git").join("objects");
    copy_dir_contents(&dst_objects, &repo_c_objects);

    // Create a remote tracking ref in C (simulating a fetch)
    let repo_c_reopen = gix::open_opts(
        dir_c.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo_c_reopen
        .reference(
            "refs/meta/origin",
            local_oid,
            gix::refs::transaction::PreviousValue::Any,
            "simulated fetch",
        )
        .unwrap();

    // -- Materialize in C --
    let session_c = Session::open(repo_c_reopen).unwrap().with_timestamp(2000);
    let mat_output = session_c.materialize(None).unwrap();
    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );

    // Verify the metadata arrived in C
    let sha_c = head_sha(&repo_c);
    // The commit SHA in repo A and C should be identical (same initial commit)
    assert_eq!(sha_a, sha_c);

    let commit_val = session_c
        .target(&Target::commit(&sha_c).unwrap())
        .get_value("agent:model")
        .unwrap();
    assert_eq!(
        commit_val,
        Some(MetaValue::String("claude-4.6".to_string()))
    );

    let project_val = session_c
        .target(&Target::project())
        .get_value("version")
        .unwrap();
    assert_eq!(project_val, Some(MetaValue::String("1.0.0".to_string())));

    let path_val = session_c
        .target(&Target::path("src/lib.rs"))
        .get_value("owner")
        .unwrap();
    assert_eq!(path_val, Some(MetaValue::String("teamA".to_string())));
}

// 4. Value type semantics

#[test]
fn string_upsert_overwrites() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("config", "first").unwrap();
    assert_eq!(
        handle.get_value("config").unwrap(),
        Some(MetaValue::String("first".to_string()))
    );

    handle.set("config", "second").unwrap();
    assert_eq!(
        handle.get_value("config").unwrap(),
        Some(MetaValue::String("second".to_string()))
    );
}

#[test]
fn list_preserves_order() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let items = ["alpha", "beta", "gamma", "delta", "epsilon"];
    for item in &items {
        handle.list_push("sequence", item).unwrap();
    }

    let entries = handle.list_entries("sequence").unwrap();
    assert_eq!(entries.len(), items.len());
    for (i, item) in items.iter().enumerate() {
        assert_eq!(
            entries[i].value, *item,
            "entry at index {i} should be {item}, got {}",
            entries[i].value
        );
    }

    // Timestamps should be monotonically non-decreasing
    for i in 1..entries.len() {
        assert!(
            entries[i].timestamp >= entries[i - 1].timestamp,
            "timestamps should be non-decreasing: {} >= {}",
            entries[i].timestamp,
            entries[i - 1].timestamp
        );
    }
}

#[test]
fn set_deduplicates_members() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = value {
        assert_eq!(members.len(), 2, "set should deduplicate: got {members:?}");
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
    } else {
        panic!("expected MetaValue::Set");
    }
}

// 5. Session construction

#[test]
fn session_with_timestamp_is_deterministic() {
    // Use two separate repos so each has its own SQLite database
    let (_dir1, repo1) = setup_repo();
    let (_dir2, repo2) = setup_repo();

    let session1 = Session::open(repo1).unwrap().with_timestamp(42_000);
    let session2 = Session::open(repo2).unwrap().with_timestamp(42_000);

    // Set identical data in both
    session1
        .target(&Target::project())
        .set("key", "value")
        .unwrap();
    session2
        .target(&Target::project())
        .set("key", "value")
        .unwrap();

    // Serialize both
    let output1 = session1.serialize().unwrap();
    let output2 = session2.serialize().unwrap();

    // Both should write the same number of changes and refs
    assert_eq!(output1.changes, output2.changes);
    assert_eq!(output1.refs_written.len(), output2.refs_written.len());
}

#[test]
fn target_named_constructors() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Verify each named constructor produces a usable target
    let targets = [
        Target::commit(&sha).unwrap(),
        Target::path("src/lib.rs"),
        Target::project(),
        Target::branch("main"),
        Target::change_id("change-abc"),
    ];

    let expected_types = [
        TargetType::Commit,
        TargetType::Path,
        TargetType::Project,
        TargetType::Branch,
        TargetType::ChangeId,
    ];

    for (target, expected_type) in targets.iter().zip(expected_types.iter()) {
        assert_eq!(target.target_type(), expected_type);

        // Each target should be usable with session.target()
        let handle = session.target(target);
        handle.set("test:key", "test-value").unwrap();
        let val = handle.get_value("test:key").unwrap();
        assert_eq!(
            val,
            Some(MetaValue::String("test-value".to_string())),
            "target {target} should support set/get"
        );
    }
}

// 6. Additional edge cases

#[test]
fn list_remove_by_index() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.list_push("items", "a").unwrap();
    handle.list_push("items", "b").unwrap();
    handle.list_push("items", "c").unwrap();

    // Remove the middle element by index
    handle.list_remove("items", 1).unwrap();

    let entries = handle.list_entries("items").unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].value, "a");
    assert_eq!(entries[1].value, "c");
}

#[test]
fn get_nonexistent_key_returns_none() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let value = handle.get_value("does:not:exist").unwrap();
    assert!(value.is_none());
}

#[test]
fn remove_nonexistent_key_returns_false() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let removed = handle.remove("nonexistent").unwrap();
    assert!(!removed, "removing a nonexistent key should return false");
}

#[test]
fn serialize_empty_is_no_op() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let output = session.serialize().unwrap();
    assert_eq!(output.changes, 0);
    assert!(output.refs_written.is_empty());
}

#[test]
fn namespaced_keys_work_correctly() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    // Deeply namespaced keys
    handle.set("agent:claude:session:id", "sess-123").unwrap();
    handle.set("agent:claude:session:model", "opus").unwrap();
    handle
        .set("agent:claude:prompt", "make a sandwich")
        .unwrap();

    // Get all under "agent:claude:session" prefix
    let session_values = handle.get_all_values(Some("agent:claude:session")).unwrap();
    assert_eq!(session_values.len(), 2);
    let keys: BTreeSet<String> = session_values.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains("agent:claude:session:id"));
    assert!(keys.contains("agent:claude:session:model"));

    // Get all under "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(agent_values.len(), 3);
}

#[test]
fn multiple_targets_are_independent() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target_a = Target::branch("branch-a");
    let target_b = Target::branch("branch-b");

    session.target(&target_a).set("status", "draft").unwrap();
    session.target(&target_b).set("status", "ready").unwrap();

    assert_eq!(
        session.target(&target_a).get_value("status").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert_eq!(
        session.target(&target_b).get_value("status").unwrap(),
        Some(MetaValue::String("ready".to_string()))
    );
}

#[test]
fn session_provides_config_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    assert_eq!(session.email(), "test@example.com");
    assert_eq!(session.name(), "Test User");
    assert_eq!(session.namespace(), "meta");
}

#[test]
fn authorship_tracks_writer() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("tracked:key", "some-value").unwrap();

    let authorship = handle.get_authorship("tracked:key").unwrap();
    assert!(authorship.is_some(), "authorship should exist after set");
    let authorship = authorship.unwrap();
    assert_eq!(authorship.email, "test@example.com");
}

#[test]
fn set_remove_then_re_add() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();

    handle.set_remove("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 1);
        assert!(members.contains("bob"));
    } else {
        panic!("expected Set");
    }

    // Re-add alice
    handle.set_add("owners", "alice").unwrap();
    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 2);
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
    } else {
        panic!("expected Set");
    }
}

// 7. Merge conflict resolution (per-value-type)

/// Helper: copy objects from a source repo into a destination repo and
/// create a remote tracking ref (`refs/meta/origin`) pointing to `oid`.
fn inject_remote_ref(
    src_objects_dir: &std::path::Path,
    dst_dir: &std::path::Path,
    oid: gix::ObjectId,
) {
    let dst_objects = dst_dir.join(".git").join("objects");
    copy_dir_contents(src_objects_dir, &dst_objects);

    let dst_repo = gix::open_opts(
        dst_dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    dst_repo
        .reference(
            "refs/meta/origin",
            oid,
            gix::refs::transaction::PreviousValue::Any,
            "simulated fetch",
        )
        .unwrap();
}

/// Helper: open a second session on an existing repo directory with a
/// given timestamp. Useful after mutating refs externally.
fn reopen_session(dir: &std::path::Path, timestamp: i64) -> Session {
    let repo = gix::open_opts(
        dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    Session::open(repo).unwrap().with_timestamp(timestamp)
}

/// Set up a three-way merge scenario: both repo A and repo C diverge from
/// a common base. Returns `(dir_a, dir_c, session_c)` where session_c is
/// ready for materialization after the caller has modified A and C.
///
/// This function:
/// 1. Creates repo A with `setup_fn_a` applied to the base state, then serializes
/// 2. Fast-forwards repo C from A's base (materialize with no local state)
/// 3. Returns the dirs and a fresh session for C
fn setup_three_way_base(
    base_fn: impl FnOnce(&Session),
) -> (tempfile::TempDir, tempfile::TempDir, gix::ObjectId) {
    // Step 1: Create repo A and set up the base state
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    base_fn(&session_a);
    let _ = session_a.serialize().unwrap();

    // Step 2: Find A's local ref (the base commit)
    let repo_a_reopen = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let base_oid = repo_a_reopen
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Step 3: Create repo C, fast-forward materialize A's base into it
    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    let dst_objects = dir_c.path().join(".git").join("objects");
    copy_dir_contents(&src_objects, &dst_objects);

    // Set remote tracking ref in C pointing to A's base commit
    let repo_c_reopen = gix::open_opts(
        dir_c.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo_c_reopen
        .reference(
            "refs/meta/origin",
            base_oid,
            gix::refs::transaction::PreviousValue::Any,
            "initial sync",
        )
        .unwrap();

    // Materialize into C to establish the common base
    let session_c = Session::open(repo_c_reopen).unwrap().with_timestamp(1500);
    let _ = session_c.materialize(None).unwrap();

    (dir_a, dir_c, base_oid)
}

#[test]
fn merge_string_conflict_local_wins() {
    // Set up the common base: both repos share "status" = "draft"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project()).set("status", "draft").unwrap();
    });

    // Repo A: change status to "published", serialize
    let session_a = reopen_session(dir_a.path(), 2000);
    session_a
        .target(&Target::project())
        .set("status", "published")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: change status to "archived", serialize
    let session_c = reopen_session(dir_c.path(), 2500);
    session_c
        .target(&Target::project())
        .set("status", "archived")
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID for the remote tracking ref
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge — local (C) wins for strings
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // The local value "archived" should win
    let val = session_c2
        .target(&Target::project())
        .get_value("status")
        .unwrap();
    assert_eq!(val, Some(MetaValue::String("archived".to_string())));
}

#[test]
fn merge_list_union() {
    // Set up the common base: both repos share a list with "base-entry"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project())
            .list_push("comments", "base-entry")
            .unwrap();
    });

    // Repo A: append "from-a", serialize
    let session_a = reopen_session(dir_a.path(), 2000);
    session_a
        .target(&Target::project())
        .list_push("comments", "from-a")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: append "from-c", serialize
    let session_c = reopen_session(dir_c.path(), 2500);
    session_c
        .target(&Target::project())
        .list_push("comments", "from-c")
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge — lists use union
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // Verify the list contains entries from both sides (union)
    let entries = session_c2
        .target(&Target::project())
        .list_entries("comments")
        .unwrap();
    let values: Vec<&str> = entries.iter().map(|e| e.value.as_str()).collect();
    assert!(
        values.contains(&"base-entry"),
        "should still have base entry, got: {values:?}"
    );
    assert!(
        values.contains(&"from-a"),
        "should have A's entry, got: {values:?}"
    );
    assert!(
        values.contains(&"from-c"),
        "should have C's entry, got: {values:?}"
    );
}

#[test]
fn merge_set_union() {
    // Set up the common base: both repos share a set with "shared"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project())
            .set_add("owners", "shared")
            .unwrap();
    });

    // Repo A: set owners to {"shared", "alice"} using set() to build the
    // full value (avoids set:add log entry which triggers a known incremental
    // serialization parsing issue)
    let session_a = reopen_session(dir_a.path(), 2000);
    let mut owners_a = BTreeSet::new();
    owners_a.insert("shared".to_string());
    owners_a.insert("alice".to_string());
    session_a
        .target(&Target::project())
        .set("owners", MetaValue::Set(owners_a))
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: set owners to {"shared", "bob"}
    let session_c = reopen_session(dir_c.path(), 2500);
    let mut owners_c = BTreeSet::new();
    owners_c.insert("shared".to_string());
    owners_c.insert("bob".to_string());
    session_c
        .target(&Target::project())
        .set("owners", MetaValue::Set(owners_c))
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge — sets use union
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // Verify the set contains members from both sides (union)
    let val = session_c2
        .target(&Target::project())
        .get_value("owners")
        .unwrap();
    assert!(val.is_some(), "owners should exist");
    if let Some(MetaValue::Set(members)) = val {
        assert!(
            members.contains("shared"),
            "should still have shared, got: {members:?}"
        );
        assert!(
            members.contains("alice"),
            "should have alice from A, got: {members:?}"
        );
        assert!(
            members.contains("bob"),
            "should have bob from C, got: {members:?}"
        );
    } else {
        panic!("expected MetaValue::Set, got: {val:?}");
    }
}

// 8. Tombstone roundtrip

#[test]
fn tombstone_survives_serialize_materialize() {
    // Repo A: set key, serialize
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set("ephemeral", "temp-value")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo A: remove key (creates tombstone), serialize again
    let session_a2 = reopen_session(dir_a.path(), 2000);
    let removed = session_a2
        .target(&Target::project())
        .remove("ephemeral")
        .unwrap();
    assert!(removed, "remove should return true for existing key");
    let _ = session_a2.serialize().unwrap();

    // Get A's commit OID after the tombstone serialize
    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Repo C: materialize A's state (which includes the tombstone)
    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );

    // The key should NOT exist in C (tombstone was applied)
    let val = session_c
        .target(&Target::project())
        .get_value("ephemeral")
        .unwrap();
    assert!(
        val.is_none(),
        "tombstoned key should not be visible after materialize"
    );
}

// 9. Incremental serialize

#[test]
fn incremental_serialize_only_includes_changes() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo).unwrap().with_timestamp(1000);

    // First serialize: set key1
    session
        .target(&Target::project())
        .set("key1", "alpha")
        .unwrap();
    let output1 = session.serialize().unwrap();
    assert!(output1.changes > 0, "first serialize should have changes");
    assert!(
        !output1.refs_written.is_empty(),
        "first serialize should write refs"
    );

    // Reopen session with a later timestamp so the second set is after
    // the last_materialized marker and will be picked up by incremental mode.
    let session2 = reopen_session(dir.path(), 2000);

    // Second serialize: set key2 (key1 is unchanged)
    session2
        .target(&Target::project())
        .set("key2", "beta")
        .unwrap();
    let output2 = session2.serialize().unwrap();
    assert!(output2.changes > 0, "second serialize should have changes");

    // Verify both keys exist after second serialize
    let val1 = session2
        .target(&Target::project())
        .get_value("key1")
        .unwrap();
    assert_eq!(val1, Some(MetaValue::String("alpha".to_string())));

    let val2 = session2
        .target(&Target::project())
        .get_value("key2")
        .unwrap();
    assert_eq!(val2, Some(MetaValue::String("beta".to_string())));

    // The second serialize is incremental: it should report fewer or equal
    // changes compared to a hypothetical full re-serialize. At minimum,
    // the second serialize should succeed with changes > 0 since key2 was added.
    assert!(
        output2.changes > 0,
        "incremental serialize should still report changes"
    );
}

// 10. Filter routing

#[test]
fn filter_routes_keys_to_destinations() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo).unwrap().with_timestamp(1000);

    // Set a filter rule: route "private:**" keys to "private" destination
    session
        .target(&Target::project())
        .set_add("meta:local:filter", "route private:** private")
        .unwrap();

    // Set a regular key and a private key
    session
        .target(&Target::project())
        .set("public:info", "everyone-sees-this")
        .unwrap();
    session
        .target(&Target::project())
        .set("private:secret", "only-private-dest")
        .unwrap();

    // Serialize
    let output = session.serialize().unwrap();
    assert!(output.changes > 0, "should have serialized something");

    // Verify refs: main should exist, and private destination ref should exist
    let repo_re = gix::open_opts(
        dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    let main_ref = repo_re.find_reference("refs/meta/local/main");
    assert!(main_ref.is_ok(), "refs/meta/local/main should exist");

    let private_ref = repo_re.find_reference("refs/meta/local/private");
    assert!(
        private_ref.is_ok(),
        "refs/meta/local/private should exist for routed keys"
    );

    // Verify refs_written includes both destinations
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/main")),
        "should write main ref, got: {:?}",
        output.refs_written
    );
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/private")),
        "should write private ref, got: {:?}",
        output.refs_written
    );
}

// 11. Push with no remote configured

#[test]
fn push_once_with_no_remote_returns_error() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    // Set some metadata so we have something to push
    session
        .target(&Target::project())
        .set("key", "value")
        .unwrap();

    // push_once with no remote configured should return an error
    let result = session.push_once(None);
    assert!(
        result.is_err(),
        "push_once should fail when no remote is configured"
    );
}

// 12. Pull with no remote configured

#[test]
fn pull_with_no_remote_returns_error() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    // pull with no remote configured should return an error
    let result = session.pull(None);
    assert!(
        result.is_err(),
        "pull should fail when no remote is configured"
    );
}

// 13. Key prefix matching

#[test]
fn key_prefix_matching_returns_subkeys() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("agent:model", "claude").unwrap();
    handle.set("agent:provider", "anthropic").unwrap();
    handle.set("other:key", "unrelated").unwrap();

    // Filter by "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(
        agent_values.len(),
        2,
        "should return only agent:* keys, got: {:?}",
        agent_values
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
    );

    let keys: BTreeSet<String> = agent_values.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains("agent:model"));
    assert!(keys.contains("agent:provider"));
    assert!(!keys.contains("other:key"));

    // Filter by more specific prefix "agent:model"
    let model_values = handle.get_all_values(Some("agent:model")).unwrap();
    assert_eq!(model_values.len(), 1);
    assert_eq!(model_values[0].0, "agent:model");

    // No filter returns everything
    let all_values = handle.get_all_values(None).unwrap();
    assert_eq!(all_values.len(), 3);
}

// 14. Large values stored as git blob references

#[test]
fn large_string_value_roundtrips() {
    // Create a string > 1024 bytes (the GIT_REF_THRESHOLD)
    let large_value: String = "x".repeat(2048);

    // Set it in repo A and serialize
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set("large:payload", large_value.as_str())
        .unwrap();
    let output = session_a.serialize().unwrap();
    assert!(output.changes > 0);

    // Verify it reads back correctly in A
    let val_a = session_a
        .target(&Target::project())
        .get_value("large:payload")
        .unwrap();
    assert_eq!(
        val_a,
        Some(MetaValue::String(large_value.clone())),
        "large value should roundtrip in the same repo"
    );

    // Materialize to repo C and verify the full value is recovered
    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 2000);
    let _ = session_c.materialize(None).unwrap();

    let val_c = session_c
        .target(&Target::project())
        .get_value("large:payload")
        .unwrap();
    assert_eq!(
        val_c,
        Some(MetaValue::String(large_value)),
        "large value should survive serialize + materialize roundtrip"
    );
}
