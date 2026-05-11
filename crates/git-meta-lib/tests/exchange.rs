#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

fn agentlog_scope() -> MetadataScope {
    MetadataScope::new(
        "agentlog",
        [
            MetadataKeyMatch::exact("gitbutler:agent-sessions"),
            MetadataKeyMatch::prefix("gitbutler:agent-session:"),
        ],
    )
    .unwrap()
}

fn ref_oid(repo_dir: &std::path::Path, ref_name: &str) -> gix::ObjectId {
    let repo = gix::open_opts(
        repo_dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo.find_reference(ref_name)
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach()
}

fn inject_scoped_tracking_ref(
    source_repo_dir: &std::path::Path,
    destination_repo_dir: &std::path::Path,
    oid: gix::ObjectId,
) {
    copy_dir_contents(
        &source_repo_dir.join(".git").join("objects"),
        &destination_repo_dir.join(".git").join("objects"),
    );
    let repo = gix::open_opts(
        destination_repo_dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo.reference(
        "refs/meta/remotes/agentlog",
        oid,
        gix::refs::transaction::PreviousValue::Any,
        "test fetch",
    )
    .unwrap();
}

fn setup_bare_remote() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();
    let status = std::process::Command::new("git")
        .args(["init", "--bare"])
        .current_dir(dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    dir
}

fn add_origin(repo_dir: &std::path::Path, remote_dir: &std::path::Path) {
    let status = std::process::Command::new("git")
        .args(["remote", "add", "origin", &remote_dir.to_string_lossy()])
        .current_dir(repo_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn tombstone_survives_serialize_materialize() {
    // Repo A: set key, serialize
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
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

#[test]
fn fetch_scope_reports_missing_remote_ref() {
    let remote = setup_bare_remote();
    let (dir, repo) = setup_repo();
    add_origin(dir.path(), remote.path());

    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);
    let fetched = session.fetch_scope(&agentlog_scope(), "origin").unwrap();

    assert!(!fetched);
}

#[test]
fn push_and_fetch_scope_exchange_matching_keys() {
    let remote = setup_bare_remote();
    let (dir_a, repo_a) = setup_repo();
    add_origin(dir_a.path(), remote.path());
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set_add("gitbutler:agent-sessions", "session-a")
        .unwrap();
    let _ = session_a.serialize_scope(&agentlog_scope()).unwrap();

    let pushed = session_a
        .push_scope_once(&agentlog_scope(), "origin")
        .unwrap();
    assert!(pushed.success);
    assert!(!pushed.non_fast_forward);

    let (dir_b, repo_b) = setup_repo();
    add_origin(dir_b.path(), remote.path());
    let session_b = Session::open(repo_b.path()).unwrap().with_timestamp(2000);
    let fetched = session_b.fetch_scope(&agentlog_scope(), "origin").unwrap();
    assert!(fetched);
    let _ = session_b.materialize_scope(&agentlog_scope()).unwrap();

    assert_eq!(
        session_b
            .target(&Target::project())
            .get_value("gitbutler:agent-sessions")
            .unwrap(),
        Some(MetaValue::Set(["session-a".to_string()].into()))
    );
}

#[test]
fn scoped_materialize_merges_matching_keys_and_preserves_unmatched() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set_add("gitbutler:agent-sessions", "session-a")
        .unwrap();
    session_a
        .target(&Target::project())
        .set("gitbutler:agent-session:session-a:schema", "v1")
        .unwrap();
    session_a
        .target(&Target::project())
        .set("review:status", "private-local")
        .unwrap();
    let _ = session_a.serialize_scope(&agentlog_scope()).unwrap();
    let oid_a = ref_oid(dir_a.path(), "refs/meta/local/agentlog");

    let (dir_b, repo_b) = setup_repo();
    let session_b = Session::open(repo_b.path()).unwrap().with_timestamp(2000);
    session_b
        .target(&Target::project())
        .set_add("gitbutler:agent-sessions", "session-b")
        .unwrap();
    session_b
        .target(&Target::project())
        .set("gitbutler:agent-session:session-b:schema", "v1")
        .unwrap();
    session_b
        .target(&Target::project())
        .set("review:status", "keep-me")
        .unwrap();
    let _ = session_b.serialize_scope(&agentlog_scope()).unwrap();
    inject_scoped_tracking_ref(dir_a.path(), dir_b.path(), oid_a);

    let output = session_b.materialize_scope(&agentlog_scope()).unwrap();
    assert!(!output.results.is_empty());

    let sessions = session_b
        .target(&Target::project())
        .get_value("gitbutler:agent-sessions")
        .unwrap();
    assert_eq!(
        sessions,
        Some(MetaValue::Set(
            ["session-a".to_string(), "session-b".to_string()].into()
        ))
    );
    let unrelated = session_b
        .target(&Target::project())
        .get_value("review:status")
        .unwrap();
    assert_eq!(unrelated, Some(MetaValue::String("keep-me".to_string())));
}

#[test]
fn scoped_set_member_tombstones_materialize() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    let handle_a = session_a.target(&Target::project());
    handle_a
        .set_add("gitbutler:agent-sessions", "session-a")
        .unwrap();
    let _ = session_a.serialize_scope(&agentlog_scope()).unwrap();
    let added_oid = ref_oid(dir_a.path(), "refs/meta/local/agentlog");

    let (dir_b, repo_b) = setup_repo();
    inject_scoped_tracking_ref(dir_a.path(), dir_b.path(), added_oid);
    let session_b = Session::open(repo_b.path()).unwrap().with_timestamp(2000);
    let _ = session_b.materialize_scope(&agentlog_scope()).unwrap();
    assert_eq!(
        session_b
            .target(&Target::project())
            .get_value("gitbutler:agent-sessions")
            .unwrap(),
        Some(MetaValue::Set(["session-a".to_string()].into()))
    );

    let session_a = reopen_session(dir_a.path(), 3000);
    let handle_a = session_a.target(&Target::project());
    handle_a
        .set_remove("gitbutler:agent-sessions", "session-a")
        .unwrap();
    let _ = session_a.serialize_scope(&agentlog_scope()).unwrap();
    let removed_oid = ref_oid(dir_a.path(), "refs/meta/local/agentlog");

    inject_scoped_tracking_ref(dir_a.path(), dir_b.path(), removed_oid);
    let session_b = reopen_session(dir_b.path(), 4000);
    let output = session_b.materialize_scope(&agentlog_scope()).unwrap();
    assert_eq!(output.results[0].strategy, MaterializeStrategy::FastForward);
    let sessions = session_b
        .target(&Target::project())
        .get_value("gitbutler:agent-sessions")
        .unwrap();
    assert!(!matches!(sessions, Some(MetaValue::Set(members)) if members.contains("session-a")));
}

#[test]
fn scoped_serialize_does_not_hide_default_serialize_work() {
    let (_dir, repo) = setup_repo();
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);
    session
        .target(&Target::project())
        .set_add("gitbutler:agent-sessions", "session-a")
        .unwrap();

    let output = session.serialize_scope(&agentlog_scope()).unwrap();
    assert_eq!(
        output.refs_written,
        vec!["refs/meta/local/agentlog".to_string()]
    );

    let output = session.serialize().unwrap();
    assert_eq!(
        output.refs_written,
        vec!["refs/meta/local/main".to_string()]
    );
}

#[test]
fn scoped_serialize_skips_meta_local_keys() {
    let (_dir, repo) = setup_repo();
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);
    session
        .target(&Target::project())
        .set("meta:local:agentlog", "secret")
        .unwrap();
    let scope = MetadataScope::new("scratch", [MetadataKeyMatch::prefix("meta:local:")]).unwrap();

    let output = session.serialize_scope(&scope).unwrap();

    assert_eq!(output.changes, 0);
    assert!(output.refs_written.is_empty());
}

#[test]
fn filter_routes_keys_to_destinations() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);

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
