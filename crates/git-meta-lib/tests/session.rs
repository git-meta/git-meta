#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

#[test]
fn session_with_timestamp_is_deterministic() {
    // Use two separate repos so each has its own SQLite database
    let (_dir1, repo1) = setup_repo();
    let (_dir2, repo2) = setup_repo();

    let session1 = Session::open(repo1.path()).unwrap().with_timestamp(42_000);
    let session2 = Session::open(repo2.path()).unwrap().with_timestamp(42_000);

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

#[test]
fn session_provides_config_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    assert_eq!(session.email(), "test@example.com");
    assert_eq!(session.name(), "Test User");
    assert_eq!(session.namespace(), "meta");
}

#[test]
fn session_open_supports_bare_repositories() {
    let dir = tempfile::TempDir::new().unwrap();
    let _repo = gix::init_bare(dir.path()).unwrap();

    let session = Session::open(dir.path()).unwrap();
    let target = Target::project();
    session.target(&target).set("key", "value").unwrap();

    assert_eq!(
        session.target(&target).get_value("key").unwrap(),
        Some(MetaValue::String("value".to_owned()))
    );
    assert!(dir.path().join("git-meta.sqlite").exists());
}

#[test]
fn session_from_repo_uses_preopened_repository() {
    let dir = tempfile::TempDir::new().unwrap();
    let _repo = gix::init_bare(dir.path()).unwrap();
    let repo = gix::open_opts(
        dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Hosted Reader", "user.email=reader@example.com"]),
    )
    .unwrap();

    let session = Session::from_repo(repo).unwrap();

    assert_eq!(session.email(), "reader@example.com");
    assert_eq!(session.name(), "Hosted Reader");
}
