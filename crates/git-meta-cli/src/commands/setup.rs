//! `git meta setup` — alias for `git meta remote add <url> --init` that
//! reads the remote URL from a project-local `.git-meta` file.
//!
//! The intent is to let projects pin a recommended metadata remote in their
//! source tree so a fresh checkout can opt in to metadata exchange with a
//! single command instead of having to remember the URL.

use std::io::IsTerminal;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use dialoguer::Confirm;
use serde::Deserialize;

use crate::commands::remote;
use crate::context::CommandContext;
use crate::style::Style;

/// Filename, relative to the repo work tree, that holds the recommended
/// metadata remote URL.
const SETUP_FILE: &str = ".git-meta";

/// Default remote name used by `git meta setup`. Matches the default of
/// `git meta remote add --name`.
const DEFAULT_REMOTE_NAME: &str = "meta";

/// Project-local setup configuration read from `.git-meta`.
#[derive(Debug, Deserialize)]
struct SetupConfig {
    /// Metadata remote URL used by `git meta setup`.
    url: String,
}

/// Run `git meta setup`.
///
/// If the repository has a `.git-meta` file, its metadata remote URL is used
/// and delegated to [`remote::run_add`] with `--init` enabled.
///
/// If `.git-meta` is missing and stdin is an interactive terminal, the user
/// is prompted to configure metadata exchange against the project's `origin`
/// remote (storing metadata under `refs/{namespace}/main`). On confirmation a
/// `.git-meta` file is written and the remote is initialized.
///
/// # Errors
///
/// Returns an error if the repository is bare, if `.git-meta` is missing in a
/// non-interactive context, if `.git-meta` contains no usable URL, or if the
/// underlying `remote add` fails (e.g. the remote name is already configured
/// or the push to the new remote fails).
pub(crate) fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();

    let workdir = repo
        .workdir()
        .ok_or_else(|| anyhow!("git meta setup requires a non-bare repository"))?;
    let setup_path = workdir.join(SETUP_FILE);

    if !setup_path.exists() {
        return interactive_setup(&ctx, &setup_path);
    }

    let url = read_setup_url(&setup_path)?;

    let s = Style::detect_stderr();
    eprintln!(
        "{} metadata remote URL from {}",
        s.step("Using"),
        s.dim(&setup_path.display().to_string()),
    );
    remote::run_add(&url, DEFAULT_REMOTE_NAME, None, true)
}

/// Interactively configure metadata exchange when no `.git-meta` file exists.
///
/// When stdin is not a terminal, falls back to the actionable error explaining
/// how to create `.git-meta` manually. Otherwise, offers to reuse the
/// project's `origin` remote for metadata storage under `refs/{namespace}/main`
/// and, on confirmation, writes `.git-meta` and initializes the remote.
fn interactive_setup(ctx: &CommandContext, setup_path: &Path) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        return Err(missing_setup_file_error(setup_path));
    }

    let repo = ctx.session.repo();
    let ns = ctx.session.namespace().to_string();
    let s = Style::detect_stderr();

    let config = repo.config_snapshot();
    let origin_url = config.string("remote.origin.url").map(|s| s.to_string());

    let Some(origin_url) = origin_url else {
        eprintln!(
            "{}: this repository has no 'origin' remote configured.",
            s.warn("Warning"),
        );
        eprintln!(
            "git meta needs a remote to store metadata under refs/{ns}/main.\n\
             Add a remote to your project (e.g. `git remote add origin <url>`) and re-run\n\
             `git meta setup`, or configure a metadata remote directly:\n  \
             git meta remote add <url> --init",
        );
        return Ok(());
    };

    eprintln!("git meta has not been set up for this repository yet.");
    eprintln!("Your 'origin' remote is {}.", s.dim(&origin_url),);
    let accept = Confirm::new()
        .with_prompt(format!(
            "Use this remote to store metadata under refs/{ns}/main?"
        ))
        .default(true)
        .interact()
        .unwrap_or(false);

    if !accept {
        eprintln!(
            "No metadata remote configured. You can set one up later with:\n  \
             git meta remote add <url> --init",
        );
        return Ok(());
    }

    let contents = format!("url: {origin_url}\n");
    std::fs::write(setup_path, &contents)
        .with_context(|| format!("write {SETUP_FILE} at {}", setup_path.display()))?;
    eprintln!(
        "{} {} with metadata remote URL.",
        s.ok("Wrote"),
        s.dim(&setup_path.display().to_string()),
    );

    remote::run_add(&origin_url, DEFAULT_REMOTE_NAME, None, true)
}

/// Build the actionable error returned when `.git-meta` is missing and the
/// process cannot prompt the user (non-interactive stdin).
fn missing_setup_file_error(path: &Path) -> anyhow::Error {
    anyhow!(
        "no {SETUP_FILE} file found at {display}\n\n\
         Create one with the metadata remote URL in YAML, e.g.:\n  \
         printf 'url: git@github.com:org/project-meta.git\\n' > {display}\n\n\
         Or run `git meta remote add <url> --init` directly to skip the alias.",
        display = path.display(),
    )
}

/// Read the metadata remote URL from a `.git-meta` file.
///
/// The file is YAML with a required `url` key. Unknown keys are ignored so
/// future versions can add more project-local setup fields.
///
/// # Errors
///
/// Returns an error if the file does not exist, cannot be read, or contains
/// invalid YAML or no usable URL.
fn read_setup_url(path: &Path) -> Result<String> {
    if !path.exists() {
        return Err(missing_setup_file_error(path));
    }

    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read {SETUP_FILE} at {}", path.display()))?;
    parse_setup_url(&raw).with_context(|| format!("parse {SETUP_FILE} at {}", path.display()))
}

/// Pure parser used by [`read_setup_url`] and unit-tested in isolation.
///
/// Returns the `url` value trimmed of surrounding whitespace.
fn parse_setup_url(contents: &str) -> Result<String> {
    let config = serde_yml::from_str::<Option<SetupConfig>>(contents)?
        .ok_or_else(|| anyhow!(".git-meta is empty or contains no metadata remote URL"))?;
    let url = config.url.trim();
    if url.is_empty() {
        bail!(".git-meta contains an empty url value");
    }
    Ok(strip_optional_trailing_slash_owned(url.to_string()))
}

/// Trim a single trailing slash from the URL, so users can paste either
/// `https://example.com/foo/bar` or `https://example.com/foo/bar/` and end
/// up with the same configured remote URL.
fn strip_optional_trailing_slash_owned(url: String) -> String {
    if url.len() > 1 && url.ends_with('/') {
        let mut s = url;
        s.pop();
        s
    } else {
        url
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_url_key() {
        assert_eq!(
            parse_setup_url("url: git@github.com:org/repo.git\n").unwrap(),
            "git@github.com:org/repo.git",
        );
    }

    #[test]
    fn ignores_comments_and_unknown_keys() {
        let input = "\n\
                     # this is a comment\n\
                     \n\
                        # indented comment\n\
                     url: git@github.com:org/repo.git\n\
                     future-key: ignored\n\
                     # trailing notes ignored\n";
        assert_eq!(
            parse_setup_url(input).unwrap(),
            "git@github.com:org/repo.git"
        );
    }

    #[test]
    fn errors_for_empty_input() {
        let err = parse_setup_url("").unwrap_err();
        assert!(
            err.to_string()
                .contains("empty or contains no metadata remote URL"),
            "got: {err}"
        );
    }

    #[test]
    fn errors_for_only_comments() {
        let err = parse_setup_url("# only a comment\n\n# another\n").unwrap_err();
        assert!(
            err.to_string()
                .contains("empty or contains no metadata remote URL"),
            "got: {err}"
        );
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(
            parse_setup_url("url: '   git@github.com:org/repo.git   '\n").unwrap(),
            "git@github.com:org/repo.git",
        );
    }

    #[test]
    fn errors_when_url_key_is_missing() {
        let input = "\
                     remote: https://example.com/first.git\n\
                     note: https://example.com/second.git\n";
        let err = parse_setup_url(input).unwrap_err();
        assert!(
            err.to_string().contains("missing field `url`"),
            "got: {err}"
        );
    }

    #[test]
    fn strips_single_trailing_slash() {
        assert_eq!(
            strip_optional_trailing_slash_owned("https://example.com/foo/".to_string()),
            "https://example.com/foo"
        );
    }

    #[test]
    fn keeps_lone_slash() {
        assert_eq!(strip_optional_trailing_slash_owned("/".to_string()), "/");
    }

    #[test]
    fn read_setup_url_missing_file_errors_helpfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".git-meta");
        let err = read_setup_url(&path).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("no .git-meta file found"), "got: {msg}");
        assert!(msg.contains("--init"), "got: {msg}");
    }

    #[test]
    fn read_setup_url_empty_file_errors_helpfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".git-meta");
        std::fs::write(&path, "# only a comment\n\n").unwrap();
        let err = read_setup_url(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("empty or contains no metadata remote URL"),
            "got: {msg}"
        );
    }

    #[test]
    fn read_setup_url_invalid_yaml_errors_helpfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".git-meta");
        std::fs::write(&path, "url: [").unwrap();
        let err = read_setup_url(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("parse .git-meta"), "got: {msg}");
    }
}
