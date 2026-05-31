//! `git meta log` — walk the commit history and print metadata for each commit,
//! matching the output of scripts/log.rb.
//!
//! Output is funnelled through [`crate::pager::Pager`], which transparently
//! pipes through `$PAGER` (or `core.pager`) when stdout is a terminal —
//! mirroring `git log`'s pager behaviour. When stdout is not a terminal
//! (pipes, redirects, the test harness, …) the pager is skipped and we
//! write straight to stdout instead.

use std::io::Write;

use anyhow::{Context, Result};

use crate::context::CommandContext;
use crate::pager::Pager;
use git_meta_lib::types::{Target, TargetType};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const BLUE: &str = "\x1b[34m";

pub(crate) fn run(
    start_ref: Option<&str>, // commit-ish to start from (default HEAD)
    count: usize,            // max commits to show
    metadata_only: bool,     // skip commits with no metadata
) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    // Set up the pager *before* writing anything. When stdout is a
    // terminal this hooks up the pager pipe; otherwise it falls back
    // to stdout. The `Pager` is dropped at end-of-fn, which closes
    // the pipe and reaps the child process.
    let mut out = Pager::start(ctx.session.core_pager());

    write_log(&mut out, &ctx, start_ref, count, metadata_only)
}

/// Walk commits and write the formatted log to `out`.
///
/// Split out from [`run`] so the rendering can be tested independently
/// of pager spawning, and so the `Pager` cleanup in [`run`] runs even
/// when this function returns an error mid-walk.
fn write_log<W: Write>(
    out: &mut W,
    ctx: &CommandContext,
    start_ref: Option<&str>,
    count: usize,
    metadata_only: bool,
) -> Result<()> {
    let start_oid = resolve_start(ctx, start_ref)?;
    let oids = ctx.session.rev_walk_oids(&start_oid)?;

    let mut printed = 0usize;

    for sha in oids {
        if printed >= count {
            break;
        }

        let commit_info = ctx.session.commit_info(&sha)?;

        // Fetch metadata before deciding whether to print the commit
        let entries = ctx
            .session
            .store()
            .get_all(
                &Target::from_parts(TargetType::Commit, Some(sha.clone())),
                None,
            )
            .unwrap_or_default();
        // get_all returns (key, value, value_type, is_git_ref)
        // value is a JSON-encoded string for string types
        let meta: Vec<(String, String)> = entries
            .into_iter()
            .map(|entry| {
                // Decode JSON string wrapper -> raw value for display
                let raw = serde_json::from_str::<String>(&entry.value).unwrap_or(entry.value);
                (entry.key, raw)
            })
            .collect();

        if metadata_only && meta.is_empty() {
            continue;
        }

        // Blank line between entries
        if printed > 0 {
            writeln!(out)?;
        }
        printed += 1;

        let short_sha = &sha[..10];

        writeln!(
            out,
            "{YELLOW}commit {short_sha}{RESET} {DIM}---{RESET} \
             {GREEN}{}{RESET} {DIM}<{}>{RESET}",
            commit_info.author_name, commit_info.author_email
        )?;

        let message = commit_info.message.trim().to_string();
        let nonempty_lines: Vec<&str> = message.lines().filter(|l| !l.trim().is_empty()).collect();
        let shown = nonempty_lines.len().min(4);
        for line in &nonempty_lines[..shown] {
            writeln!(out, "  {line}")?;
        }
        if nonempty_lines.len() > 4 {
            let extra = nonempty_lines.len() - 4;
            writeln!(out, "  {DIM}... ({extra} more lines){RESET}")?;
        }

        if !meta.is_empty() {
            writeln!(out, "  {CYAN}--- metadata ---{RESET}")?;
            for (key, value) in &meta {
                let preview = format_value_preview(value);
                writeln!(out, "  {BLUE}|{RESET} {BOLD}{key}{RESET}  {preview}")?;
            }
            writeln!(out, "  {BLUE}.{RESET}")?;
        }
    }

    if printed == 0 {
        if metadata_only {
            writeln!(out, "No commits with metadata found.")?;
        } else {
            writeln!(out, "No commits found.")?;
        }
    }

    Ok(())
}
/// Resolve a ref name or commit-ish to an OID.  Falls back to HEAD.
fn resolve_start(ctx: &CommandContext, start_ref: Option<&str>) -> Result<String> {
    let spec = start_ref.unwrap_or("HEAD");
    ctx.session
        .resolve_commitish(spec)
        .with_context(|| format!("could not resolve ref '{spec}'"))
}

/// Format a raw (already-decoded) metadata value for display.
/// Mirrors the Ruby format_value_preview function:
///   - JSON arrays   -> "[list: N items]"
///   - JSON objects  -> "{object: N keys}"
///   - strings       -> first line, truncated to 50 chars; " ..." appended
///     if there are more lines
fn format_value_preview(value: &str) -> String {
    // Try JSON parse for arrays/objects
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
        if let Some(arr) = json.as_array() {
            return format!("{CYAN}[list: {} items]{RESET}", arr.len());
        }
        if let Some(obj) = json.as_object() {
            return format!("{CYAN}{{object: {} keys}}{RESET}", obj.len());
        }
    }

    // Plain string
    let first_line = value.lines().next().unwrap_or("");
    let has_more_lines = value.contains('\n') && value.trim_end_matches('\n') != first_line;

    let mut preview = if first_line.len() > 50 {
        format!("{}...", &first_line[..50])
    } else {
        first_line.to_string()
    };

    if has_more_lines && first_line.len() <= 50 {
        preview.push_str(" ...");
    }

    format!("{DIM}{preview}{RESET}")
}
