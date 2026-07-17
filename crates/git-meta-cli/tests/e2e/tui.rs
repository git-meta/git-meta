//! End-to-end tests for `git meta tui`.
//!
//! The interactive UI itself is covered by unit tests against the state
//! machine and a ratatui `TestBackend`; here we only verify the CLI
//! boundary: a non-interactive invocation must fail with a pointer to
//! `git meta inspect` instead of corrupting the (piped) output stream.

use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness;

#[test]
fn tui_refuses_to_run_without_a_terminal() {
    let dir = TempDir::new().unwrap();

    // Under the test harness stdin/stdout are pipes, so the TTY guard
    // fires before any repository discovery happens.
    harness::git_meta(dir.path())
        .arg("tui")
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires an interactive terminal"))
        .stderr(predicate::str::contains("git meta inspect"));
}
