#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod helpers;

use git_meta_lib::*;
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use helpers::*;

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
fn filter_routes_keys_to_destinations() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);

    // Set a filter rule: route "private:**" keys to "private" destination
    session
        .target(&Target::project())
        .set_add("local:meta:filter", "route private:** private")
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

    let main_entries = ref_tree_entries(&repo_re, "refs/meta/local/main");
    assert!(
        main_entries
            .iter()
            .any(|(_path, content)| content == "everyone-sees-this"),
        "main ref should include public key, got: {main_entries:?}"
    );
    assert!(
        main_entries
            .iter()
            .all(|(_path, content)| content != "only-private-dest"),
        "main ref should not include routed private key, got: {main_entries:?}"
    );
    assert!(
        main_entries
            .iter()
            .all(|(_path, content)| !content.contains("route private:** private")),
        "main ref should not serialize local filter config, got: {main_entries:?}"
    );

    let private_entries = ref_tree_entries(&repo_re, "refs/meta/local/private");
    assert!(
        private_entries
            .iter()
            .any(|(_path, content)| content == "only-private-dest"),
        "private ref should include routed key, got: {private_entries:?}"
    );
    assert!(
        private_entries
            .iter()
            .all(|(_path, content)| content != "everyone-sees-this"),
        "private ref should not include public key, got: {private_entries:?}"
    );
}

#[test]
fn local_prefix_keys_are_never_serialized_on_any_target() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);

    session
        .target(&Target::project())
        .set_add("local:meta:filter", "route local:** private")
        .unwrap();
    session
        .target(&Target::project())
        .set("public:project", "public-project")
        .unwrap();
    session
        .target(&Target::project())
        .set("local:project", "local-project")
        .unwrap();
    session
        .target(&Target::commit(&sha).unwrap())
        .set("local:commit", "local-commit")
        .unwrap();
    session
        .target(&Target::branch("main"))
        .set("local:branch", "local-branch")
        .unwrap();
    session
        .target(&Target::path("src/lib.rs"))
        .set("local:path", "local-path")
        .unwrap();

    let output = session.serialize().unwrap();
    assert_eq!(
        output.refs_written,
        vec!["refs/meta/local/main"],
        "local-prefixed keys must not create routed destination refs"
    );

    let repo_re = gix::open_opts(
        repo.workdir().unwrap(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    assert!(
        repo_re.find_reference("refs/meta/local/private").is_err(),
        "route rules must not override the local: hard exclusion"
    );

    let entries = ref_tree_entries(&repo_re, "refs/meta/local/main");
    assert!(
        entries
            .iter()
            .any(|(_path, content)| content == "public-project"),
        "sanity check: public key should serialize, got: {entries:?}"
    );
    for local_value in [
        "local-project",
        "local-commit",
        "local-branch",
        "local-path",
        "route local:** private",
    ] {
        assert!(
            entries
                .iter()
                .all(|(_path, content)| content != local_value),
            "local-prefixed value {local_value:?} should not serialize, got: {entries:?}"
        );
    }
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

fn ref_tree_entries(repo: &gix::Repository, ref_name: &str) -> Vec<(String, String)> {
    let commit_oid = repo
        .find_reference(ref_name)
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();
    let commit = commit_oid.attach(repo).object().unwrap().into_commit();
    let tree = commit.tree().unwrap();
    let mut entries = Vec::new();
    walk_tree(repo, tree.id, "", &mut entries);
    entries
}

fn walk_tree(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
    entries: &mut Vec<(String, String)>,
) {
    let tree = tree_id.attach(repo).object().unwrap().into_tree();
    for entry in tree.iter() {
        let entry = entry.unwrap();
        let name = entry.filename().to_str().unwrap();
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        if entry.mode().is_tree() {
            walk_tree(repo, entry.object_id(), &path, entries);
        } else {
            let blob = entry.object().unwrap();
            let content = std::str::from_utf8(blob.data.as_ref())
                .unwrap_or("")
                .to_string();
            entries.push((path, content));
        }
    }
}
