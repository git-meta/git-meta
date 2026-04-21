use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness::{self, open_repo, ref_to_commit_oid, setup_bare_with_meta, setup_repo};

#[test]
fn remote_add_no_meta_refs() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    {
        let _ = gix::init_bare(bare_dir.path()).unwrap();
        harness::open_repo(bare_dir.path())
    };

    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no metadata refs found"))
        .stderr(predicate::str::contains("--init"));
}

#[test]
fn remote_add_init_creates_ref_and_pushes_readme() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path, "--init"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added meta remote"))
        .stderr(predicate::str::contains(
            "Created refs/meta/local/main with initial README commit",
        ))
        .stderr(predicate::str::contains(
            "Initializing refs/meta/main on meta",
        ));

    let local = open_repo(dir.path());
    let local_tip = ref_to_commit_oid(&local, "refs/meta/local/main");
    let tracking_tip = ref_to_commit_oid(&local, "refs/meta/remotes/main");
    assert_eq!(
        local_tip, tracking_tip,
        "remote tracking ref should match the local ref we just pushed"
    );

    let bare = open_repo(bare_dir.path());
    let bare_tip = ref_to_commit_oid(&bare, "refs/meta/main");
    assert_eq!(
        bare_tip, local_tip,
        "bare remote should now have refs/meta/main pointing at the README commit"
    );

    let commit = bare
        .find_object(bare_tip)
        .expect("commit exists")
        .into_commit();
    let tree_id = commit.tree_id().expect("commit has a tree").detach();
    let tree = bare.find_object(tree_id).expect("tree exists").into_tree();
    let entry_names: Vec<String> = tree
        .iter()
        .filter_map(std::result::Result::ok)
        .map(|e| e.filename().to_string())
        .collect();
    assert_eq!(
        entry_names,
        vec!["README.md".to_string()],
        "initial commit should contain only README.md"
    );
}

/// `CLICOLOR_FORCE=1` must surface ANSI escape sequences in the
/// progress output so users on a terminal can visually scan the
/// pipeline. We assert the specific SGR sequences for each role:
/// step labels render bold cyan, success labels render bold green, and
/// the dim helper renders the OID detail in dim.
#[test]
fn remote_add_init_emits_ansi_color_when_forced() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .args(["remote", "add", bare_path, "--init"])
        .assert()
        .success()
        .stderr(predicate::str::contains("\x1b[1;36mChecking\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;32mCreated\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;36mInitializing\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;36mFetching\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;32mdone.\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;36mHydrating\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;36mSerializing\x1b[0m"))
        .stderr(predicate::str::contains("\x1b[1;36mMaterializing\x1b[0m"))
        .stdout(predicate::str::contains("\x1b[1;32mAdded\x1b[0m"));
}

/// Mirror of [`remote_add_init_emits_ansi_color_when_forced`] for the
/// default capture-mode case: `assert_cmd` pipes stdout/stderr, so the
/// TTY check fails and no ANSI sequences should appear.
#[test]
fn remote_add_init_omits_ansi_color_when_not_a_tty() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .env_remove("CLICOLOR_FORCE")
        .args(["remote", "add", bare_path, "--init"])
        .assert()
        .success()
        .stderr(predicate::str::contains("\x1b[").not())
        .stdout(predicate::str::contains("\x1b[").not());
}

#[test]
fn remote_add_init_with_namespace_uses_that_namespace() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path, "--namespace=altmeta", "--init"])
        .assert()
        .success();

    let local = open_repo(dir.path());
    let _ = ref_to_commit_oid(&local, "refs/altmeta/local/main");
    let _ = ref_to_commit_oid(&local, "refs/altmeta/remotes/main");

    let bare = open_repo(bare_dir.path());
    let _ = ref_to_commit_oid(&bare, "refs/altmeta/main");
}

#[test]
fn remote_add_init_reuses_existing_local_ref() {
    let (dir, _sha) = setup_repo();

    // Pre-seed refs/meta/local/main with an arbitrary commit, simulating a
    // checkout that already has local metadata history (e.g. from a prior
    // `git meta serialize` against a different remote).
    let local = open_repo(dir.path());
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };
    let blob_oid = local.write_blob(b"pre-existing").unwrap().detach();
    let mut editor = local.empty_tree().edit().unwrap();
    editor
        .upsert("seed.txt", gix::objs::tree::EntryKind::Blob, blob_oid)
        .unwrap();
    let tree_oid = editor.write().unwrap().detach();
    let commit = gix::objs::Commit {
        message: "pre-existing local meta".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let preseeded_tip = local.write_object(&commit).unwrap().detach();
    local
        .reference(
            "refs/meta/local/main",
            preseeded_tip,
            gix::refs::transaction::PreviousValue::Any,
            "pre-seed for test",
        )
        .unwrap();

    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path, "--init"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Reusing existing refs/meta/local/main",
        ));

    let local = open_repo(dir.path());
    let after_tip = ref_to_commit_oid(&local, "refs/meta/local/main");
    assert_eq!(
        after_tip, preseeded_tip,
        "--init must not rewrite an existing local ref"
    );

    let bare = open_repo(bare_dir.path());
    let bare_tip = ref_to_commit_oid(&bare, "refs/meta/main");
    assert_eq!(
        bare_tip, preseeded_tip,
        "bare remote should receive whatever the local ref already pointed at"
    );
}

#[test]
fn remote_add_meta_refs_in_different_namespace() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("refs/altmeta/main"))
        .stderr(predicate::str::contains("--namespace=altmeta"));
}

#[test]
fn remote_add_with_namespace_override() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path, "--namespace=altmeta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added meta remote"));

    let repo = open_repo(dir.path());
    let config = repo.config_snapshot();
    let fetch = config
        .string("remote.meta.fetch")
        .expect("fetch refspec should exist");
    let fetch_str = fetch.to_string();
    assert!(
        fetch_str.contains("refs/altmeta/"),
        "fetch refspec should use altmeta namespace, got: {fetch_str}"
    );
    let meta_ns = config
        .string("remote.meta.metanamespace")
        .expect("metanamespace should exist");
    assert_eq!(meta_ns.to_string(), "altmeta");
}

#[test]
fn remote_add_shorthand_url_expansion() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["remote", "add", "nonexistent-user-xyz/nonexistent-repo-xyz"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git",
        ));

    let repo = open_repo(dir.path());
    let config = repo.config_snapshot();
    let url = config
        .string("remote.meta.url")
        .expect("remote URL should exist");
    assert_eq!(
        url.to_string(),
        "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git"
    );
}

#[test]
fn remote_list_and_remove() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("meta\t"))
        .stdout(predicate::str::contains(bare_path));

    harness::git_meta(dir.path())
        .args(["remote", "remove", "meta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed meta remote"));

    harness::git_meta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No metadata remotes configured"));
}
