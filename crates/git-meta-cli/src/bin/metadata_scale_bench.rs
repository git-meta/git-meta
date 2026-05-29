use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use git_meta_lib::db::Store;
use git_meta_lib::serialize::{self, SerializeProgress};
use git_meta_lib::sync;
use git_meta_lib::tree::format::parse_tree;
use git_meta_lib::types::{Target, ValueType};
use git_meta_lib::Session;
use gix::prelude::ObjectIdExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const DEFAULT_BASELINE_YEARS: f64 = 3.0;
const DEFAULT_BASELINE_PULL_REQUESTS: f64 = 15_000.0;
const DEFAULT_BASELINE_ISSUES: f64 = 3_000.0;
const START_TIMESTAMP_MS: i64 = 1_704_067_200_000;
const GIT_META_REF: &str = "refs/meta/local/main";
const CLONE_META_REF: &str = "refs/meta/remotes/origin/main";

#[derive(Debug, Parser)]
#[command(
    name = "metadata-scale-bench",
    about = "Generate and measure large git-meta metadata histories"
)]
struct Args {
    #[arg(long, default_value_t = DEFAULT_BASELINE_YEARS)]
    years: f64,

    #[arg(long, default_value_t = 30)]
    team_size: usize,

    #[arg(long)]
    pull_requests: Option<usize>,

    #[arg(long)]
    issues: Option<usize>,

    #[arg(long, default_value = "gitbutlerapp/gitbutler")]
    github_repo: String,

    #[arg(long, default_value_t = 100)]
    github_sample: usize,

    #[arg(long)]
    no_github_sample: bool,

    #[arg(long)]
    max_events: Option<usize>,

    #[arg(long, default_value_t = 1_000)]
    checkpoint_every: usize,

    #[arg(long)]
    keep: bool,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Serialize)]
struct WorkloadProfile {
    source: String,
    sampled_pull_requests: usize,
    sampled_issues: usize,
    avg_pr_comments: f64,
    avg_pr_review_comments: f64,
    avg_pr_reviews: f64,
    avg_pr_commits: f64,
    avg_issue_comments: f64,
}

#[derive(Debug, Clone, Serialize)]
struct WorkloadPlan {
    years: f64,
    team_size: usize,
    pull_requests: usize,
    issues: usize,
    modeled_events: usize,
    expected_metadata_commits: usize,
    events_per_pull_request: usize,
    events_per_issue: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
struct GenerationMetrics {
    events_written: usize,
    metadata_commits: usize,
    serialize_changes: usize,
    serialize_wall_ms: u128,
    checkpoints: Vec<Checkpoint>,
}

#[derive(Debug, Clone, Serialize)]
struct Checkpoint {
    events: usize,
    metadata_commits: usize,
    elapsed_ms: u128,
    last_serialize_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
struct CloneMetrics {
    source_git_bytes: u64,
    bare_remote_bytes: u64,
    blobless_clone_git_bytes: u64,
    fetch_ms: u128,
    object_counts: BTreeMap<String, String>,
    missing_objects_reported: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ProcessingMetrics {
    tip_key_count: usize,
    tip_tree_extract_ms: u128,
    history_promisor_entries: usize,
    history_index_ms: u128,
    full_tip_values: usize,
    full_tip_tombstones: usize,
    full_tip_parse_ms: u128,
    force_full_serialize_ms: u128,
    force_full_changes: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BenchReport {
    workspace: PathBuf,
    profile: WorkloadProfile,
    plan: WorkloadPlan,
    generation: GenerationMetrics,
    clone: CloneMetrics,
    processing: ProcessingMetrics,
    example_data: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GhIssueItem {
    #[serde(default)]
    pull_request: Option<serde_json::Value>,
    #[serde(default)]
    comments: u64,
}

#[derive(Debug, Deserialize)]
struct GhPullItem {
    number: u64,
}

#[derive(Debug, Deserialize)]
struct GhPullDetail {
    #[serde(default)]
    comments: u64,
    #[serde(default)]
    review_comments: u64,
    #[serde(default)]
    commits: u64,
}

#[derive(Debug, Deserialize)]
struct GhReviewItem {
    #[serde(default)]
    id: u64,
}

#[derive(Debug)]
enum EventKind {
    Set { key: String, value: String },
    ListPush { key: String, value: String },
    SetAdd { key: String, value: String },
}

#[derive(Debug)]
struct Event {
    target: Target,
    kind: EventKind,
    actor: String,
    timestamp_ms: i64,
}

struct Workspace {
    path: PathBuf,
    keep: bool,
}

impl Workspace {
    fn create(keep: bool) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("git-meta-scale-{}", Uuid::new_v4()));
        fs::create_dir_all(&path)
            .with_context(|| format!("creating workspace {}", path.display()))?;
        Ok(Self { path, keep })
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let profile = load_profile(&args);
    let plan = build_plan(&args, &profile);
    let workspace = Workspace::create(args.keep)?;
    let source_path = workspace.path.join("source");

    eprintln!(
        "modeling {} PRs, {} issues, and {} metadata commits",
        plan.pull_requests, plan.issues, plan.expected_metadata_commits
    );

    initialize_source_repo(&source_path)?;
    let session = Session::open(&source_path)?;
    let generation = generate_history(&args, &session, &plan)?;
    let processing = measure_processing(&session, &workspace.path)?;
    let clone = measure_blobless_clone(&workspace.path, &source_path)?;
    let example_data = example_data_paths(session.repo())?;

    let report = BenchReport {
        workspace: workspace.path.clone(),
        profile,
        plan,
        generation,
        clone,
        processing,
        example_data,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report);
    }

    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.years <= 0.0 {
        bail!("--years must be greater than zero");
    }
    if args.team_size == 0 {
        bail!("--team-size must be greater than zero");
    }
    if args.checkpoint_every == 0 {
        bail!("--checkpoint-every must be greater than zero");
    }
    Ok(())
}

fn load_profile(args: &Args) -> WorkloadProfile {
    if args.no_github_sample || args.github_sample == 0 {
        return fallback_profile("fallback model (--no-github-sample)");
    }

    match sample_github(&args.github_repo, args.github_sample) {
        Ok(profile) => profile,
        Err(err) => {
            eprintln!(
                "warning: could not sample GitHub data from {}: {err}",
                args.github_repo
            );
            fallback_profile("fallback model (GitHub sampling failed)")
        }
    }
}

fn fallback_profile(source: &str) -> WorkloadProfile {
    WorkloadProfile {
        source: source.to_string(),
        sampled_pull_requests: 0,
        sampled_issues: 0,
        avg_pr_comments: 2.0,
        avg_pr_review_comments: 8.0,
        avg_pr_reviews: 2.0,
        avg_pr_commits: 4.0,
        avg_issue_comments: 3.0,
    }
}

fn sample_github(repo: &str, sample_size: usize) -> Result<WorkloadProfile> {
    let pull_numbers = fetch_pull_numbers(repo, sample_size)?;
    let issue_comments = fetch_issue_comment_counts(repo, sample_size)?;
    let mut pr_comments = Vec::new();
    let mut pr_review_comments = Vec::new();
    let mut pr_commits = Vec::new();
    let mut pr_reviews = Vec::new();

    for number in &pull_numbers {
        let detail: GhPullDetail = gh_json(&["api", &format!("repos/{repo}/pulls/{number}")])?;
        let reviews: Vec<GhReviewItem> =
            gh_json(&["api", &format!("repos/{repo}/pulls/{number}/reviews")])?;
        pr_comments.push(detail.comments);
        pr_review_comments.push(detail.review_comments);
        pr_commits.push(detail.commits);
        pr_reviews.push(count_distinct_reviews(&reviews));
    }

    Ok(WorkloadProfile {
        source: format!("GitHub sample from {repo}"),
        sampled_pull_requests: pull_numbers.len(),
        sampled_issues: issue_comments.len(),
        avg_pr_comments: average(&pr_comments),
        avg_pr_review_comments: average(&pr_review_comments),
        avg_pr_reviews: average(&pr_reviews),
        avg_pr_commits: average(&pr_commits),
        avg_issue_comments: average(&issue_comments),
    })
}

fn fetch_pull_numbers(repo: &str, sample_size: usize) -> Result<Vec<u64>> {
    let mut numbers = Vec::new();
    let mut page = 1usize;
    while numbers.len() < sample_size {
        let pulls: Vec<GhPullItem> = gh_json(&[
            "api",
            "-X",
            "GET",
            &format!("repos/{repo}/pulls"),
            "-f",
            "state=all",
            "-f",
            "per_page=100",
            "-f",
            &format!("page={page}"),
        ])?;
        if pulls.is_empty() {
            break;
        }
        numbers.extend(pulls.into_iter().map(|pull| pull.number));
        page += 1;
    }
    numbers.truncate(sample_size);
    Ok(numbers)
}

fn fetch_issue_comment_counts(repo: &str, sample_size: usize) -> Result<Vec<u64>> {
    let mut comments = Vec::new();
    let mut page = 1usize;
    while comments.len() < sample_size {
        let issues: Vec<GhIssueItem> = gh_json(&[
            "api",
            "-X",
            "GET",
            &format!("repos/{repo}/issues"),
            "-f",
            "state=all",
            "-f",
            "per_page=100",
            "-f",
            &format!("page={page}"),
        ])?;
        if issues.is_empty() {
            break;
        }
        comments.extend(
            issues
                .into_iter()
                .filter(|issue| issue.pull_request.is_none())
                .map(|issue| issue.comments),
        );
        page += 1;
    }
    comments.truncate(sample_size);
    Ok(comments)
}

fn gh_json<T: DeserializeOwned>(args: &[&str]) -> Result<T> {
    let output = Command::new("gh")
        .args(args)
        .output()
        .context("running gh")?;
    if !output.status.success() {
        bail!(
            "gh {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parsing gh JSON")
}

fn count_distinct_reviews(reviews: &[GhReviewItem]) -> u64 {
    reviews
        .iter()
        .map(|review| review.id)
        .collect::<std::collections::BTreeSet<_>>()
        .len() as u64
}

fn average(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

fn build_plan(args: &Args, profile: &WorkloadProfile) -> WorkloadPlan {
    let default_prs_per_year = DEFAULT_BASELINE_PULL_REQUESTS / DEFAULT_BASELINE_YEARS;
    let default_issues_per_year = DEFAULT_BASELINE_ISSUES / DEFAULT_BASELINE_YEARS;
    let pull_requests = args
        .pull_requests
        .unwrap_or_else(|| (default_prs_per_year * args.years).round() as usize);
    let issues = args
        .issues
        .unwrap_or_else(|| (default_issues_per_year * args.years).round() as usize);

    let events_per_pull_request = 4
        + ceil_to_usize(profile.avg_pr_comments)
        + ceil_to_usize(profile.avg_pr_review_comments)
        + ceil_to_usize(profile.avg_pr_reviews)
        + usize::from(profile.avg_pr_commits > 0.0);
    let events_per_issue = 5 + ceil_to_usize(profile.avg_issue_comments);
    let modeled_events = pull_requests * events_per_pull_request + issues * events_per_issue;
    let expected_metadata_commits = args
        .max_events
        .map_or(modeled_events, |limit| modeled_events.min(limit));

    WorkloadPlan {
        years: args.years,
        team_size: args.team_size,
        pull_requests,
        issues,
        modeled_events,
        expected_metadata_commits,
        events_per_pull_request,
        events_per_issue,
    }
}

fn ceil_to_usize(value: f64) -> usize {
    if value <= 0.0 {
        0
    } else {
        value.ceil() as usize
    }
}

fn initialize_source_repo(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    run_git(path, &["init", "-q"])?;
    run_git(path, &["config", "user.name", "Metadata Bench"])?;
    run_git(path, &["config", "user.email", "bench@example.com"])?;
    run_git(path, &["config", "meta.namespace", "meta"])?;
    fs::write(path.join("README.md"), "metadata scale benchmark\n")
        .with_context(|| format!("writing {}", path.join("README.md").display()))?;
    run_git(path, &["add", "README.md"])?;
    run_git(path, &["commit", "-q", "-m", "initial project"])?;
    Ok(())
}

fn generate_history(
    args: &Args,
    session: &Session,
    plan: &WorkloadPlan,
) -> Result<GenerationMetrics> {
    let start = Instant::now();
    let mut metrics = GenerationMetrics::default();
    let mut event_index = 0usize;

    for issue_number in 1..=plan.issues {
        for event in issue_events(
            issue_number,
            args.team_size,
            event_index,
            plan.events_per_issue,
        ) {
            if should_stop(args, metrics.events_written) {
                return Ok(metrics);
            }
            write_event(session, event)?;
            record_serialize(
                session,
                &mut metrics,
                &mut event_index,
                start,
                args.checkpoint_every,
            )?;
        }
    }

    for pr_number in 1..=plan.pull_requests {
        for event in pull_request_events(
            pr_number,
            args.team_size,
            event_index,
            plan.events_per_pull_request,
        ) {
            if should_stop(args, metrics.events_written) {
                return Ok(metrics);
            }
            write_event(session, event)?;
            record_serialize(
                session,
                &mut metrics,
                &mut event_index,
                start,
                args.checkpoint_every,
            )?;
        }
    }

    Ok(metrics)
}

fn should_stop(args: &Args, events_written: usize) -> bool {
    args.max_events
        .is_some_and(|max_events| events_written >= max_events)
}

fn issue_events(
    issue_number: usize,
    team_size: usize,
    offset: usize,
    events_per_issue: usize,
) -> Vec<Event> {
    let target = Target::change_id(&format!("github-issue-{issue_number}"));
    let comments = events_per_issue.saturating_sub(5);
    let mut events = vec![
        set_event(
            &target,
            "github:issue:title",
            &format!("Issue {issue_number}"),
            team_size,
            offset,
        ),
        set_event(&target, "github:issue:state", "open", team_size, offset + 1),
        set_event(
            &target,
            "github:issue:milestone",
            &format!("v{}", issue_number % 12),
            team_size,
            offset + 2,
        ),
        set_event(
            &target,
            "github:issue:assignee",
            &format!("dev{}", issue_number % team_size),
            team_size,
            offset + 3,
        ),
    ];
    for comment_index in 0..comments {
        events.push(list_event(
            &target,
            "github:issue:comments",
            &format!("Issue {issue_number} comment {comment_index}: modeled from GitButler issue activity."),
            team_size,
            offset + 4 + comment_index,
        ));
    }
    events.push(set_event(
        &target,
        "github:issue:state",
        "closed",
        team_size,
        offset + 4 + comments,
    ));
    events
}

fn pull_request_events(
    pr_number: usize,
    team_size: usize,
    offset: usize,
    events_per_pull_request: usize,
) -> Vec<Event> {
    let target = Target::branch(&format!("github/pr/{pr_number}"));
    let variable_events = events_per_pull_request.saturating_sub(5);
    let mut events = vec![
        set_event(
            &target,
            "github:pr:title",
            &format!("Pull request {pr_number}"),
            team_size,
            offset,
        ),
        set_event(
            &target,
            "github:pr:url",
            &format!("https://github.com/gitbutlerapp/gitbutler/pull/{pr_number}"),
            team_size,
            offset + 1,
        ),
        set_event(&target, "github:pr:state", "open", team_size, offset + 2),
        set_event(
            &target,
            "github:pr:head",
            &format!("feature/{pr_number}"),
            team_size,
            offset + 3,
        ),
    ];

    let mut next = offset + events.len();
    for index in 0..variable_events {
        match index % 4 {
            0 => events.push(list_event(
                &target,
                "github:pr:comments",
                &format!("PR {pr_number} discussion comment {index}: benchmark payload modeled from GitButler."),
                team_size,
                next,
            )),
            1 => events.push(list_event(
                &target,
                "github:pr:reviews",
                &format!("PR {pr_number} review {index}: CHANGES_REQUESTED or APPROVED with a short body."),
                team_size,
                next,
            )),
            2 => events.push(set_add_event(
                &target,
                "github:pr:reviewed-by",
                &format!("dev{}", (pr_number + index) % team_size),
                team_size,
                next,
            )),
            _ => events.push(set_event(
                &target,
                "github:pr:ci:status",
                "pending",
                team_size,
                next,
            )),
        }
        next += 1;
    }
    events.push(set_event(
        &target,
        "github:pr:state",
        "merged",
        team_size,
        next,
    ));
    events
}

fn set_event(target: &Target, key: &str, value: &str, team_size: usize, index: usize) -> Event {
    Event {
        target: target.clone(),
        kind: EventKind::Set {
            key: key.to_string(),
            value: value.to_string(),
        },
        actor: actor(team_size, index),
        timestamp_ms: timestamp(index),
    }
}

fn list_event(target: &Target, key: &str, value: &str, team_size: usize, index: usize) -> Event {
    Event {
        target: target.clone(),
        kind: EventKind::ListPush {
            key: key.to_string(),
            value: value.to_string(),
        },
        actor: actor(team_size, index),
        timestamp_ms: timestamp(index),
    }
}

fn set_add_event(target: &Target, key: &str, value: &str, team_size: usize, index: usize) -> Event {
    Event {
        target: target.clone(),
        kind: EventKind::SetAdd {
            key: key.to_string(),
            value: value.to_string(),
        },
        actor: actor(team_size, index),
        timestamp_ms: timestamp(index),
    }
}

fn actor(team_size: usize, index: usize) -> String {
    format!("dev{}@example.com", index % team_size)
}

fn timestamp(index: usize) -> i64 {
    START_TIMESTAMP_MS.saturating_add(i64::try_from(index).unwrap_or(i64::MAX))
}

fn write_event(session: &Session, event: Event) -> Result<()> {
    match event.kind {
        EventKind::Set { key, value } => session.store().set(
            &event.target,
            &key,
            &serde_json::to_string(&value)?,
            &ValueType::String,
            &event.actor,
            event.timestamp_ms,
        )?,
        EventKind::ListPush { key, value } => session.store().list_push_with_repo(
            Some(session.repo()),
            &event.target,
            &key,
            &value,
            &event.actor,
            event.timestamp_ms,
        )?,
        EventKind::SetAdd { key, value } => session.store().set_add(
            &event.target,
            &key,
            &value,
            &event.actor,
            event.timestamp_ms,
        )?,
    }
    Ok(())
}

fn record_serialize(
    session: &Session,
    metrics: &mut GenerationMetrics,
    event_index: &mut usize,
    start: Instant,
    checkpoint_every: usize,
) -> Result<()> {
    let serialize_start = Instant::now();
    let mut change_count = 0usize;
    let output =
        serialize::run_with_progress(session, timestamp(*event_index), false, |progress| {
            if let SerializeProgress::Read { changes, .. } = progress {
                change_count = changes;
            }
        })?;
    let elapsed = serialize_start.elapsed();
    metrics.events_written += 1;
    metrics.serialize_wall_ms += elapsed.as_millis();
    metrics.serialize_changes += change_count;
    metrics.metadata_commits += output.refs_written.len();
    *event_index += 1;

    if metrics.events_written.is_multiple_of(checkpoint_every) {
        metrics.checkpoints.push(Checkpoint {
            events: metrics.events_written,
            metadata_commits: metrics.metadata_commits,
            elapsed_ms: start.elapsed().as_millis(),
            last_serialize_ms: elapsed.as_millis(),
        });
        eprintln!(
            "checkpoint: {} events, {} metadata commits, elapsed {:?}",
            metrics.events_written,
            metrics.metadata_commits,
            start.elapsed()
        );
    }

    Ok(())
}

fn measure_processing(session: &Session, workspace: &Path) -> Result<ProcessingMetrics> {
    let tip_oid = peel_ref(session.repo(), GIT_META_REF)?;
    let tip_tree = commit_tree_id(session.repo(), tip_oid)?;

    let extract_start = Instant::now();
    let tip_keys = sync::extract_keys_from_tree(session.repo(), tip_tree)?;
    let tip_tree_extract_ms = extract_start.elapsed().as_millis();

    let index_store_path = workspace.join("history-index.sqlite");
    let index_store = Store::open(&index_store_path)?;
    let index_start = Instant::now();
    let history_promisor_entries =
        sync::insert_promisor_entries(session.repo(), &index_store, tip_oid, None)?;
    let history_index_ms = index_start.elapsed().as_millis();

    let parse_start = Instant::now();
    let parsed = parse_tree(session.repo(), tip_tree, "")?;
    let full_tip_parse_ms = parse_start.elapsed().as_millis();

    let full_start = Instant::now();
    let full = serialize::run(session, timestamp(usize::MAX / 2), true)?;
    let force_full_serialize_ms = full_start.elapsed().as_millis();

    Ok(ProcessingMetrics {
        tip_key_count: tip_keys.len(),
        tip_tree_extract_ms,
        history_promisor_entries,
        history_index_ms,
        full_tip_values: parsed.values.len(),
        full_tip_tombstones: parsed.tombstones.len(),
        full_tip_parse_ms,
        force_full_serialize_ms,
        force_full_changes: full.changes,
    })
}

fn measure_blobless_clone(workspace: &Path, source_path: &Path) -> Result<CloneMetrics> {
    let bare_path = workspace.join("remote.git");
    let clone_path = workspace.join("blobless");
    run_command(
        workspace,
        "git",
        &[
            "clone",
            "--mirror",
            source_path
                .to_str()
                .context("source path is not valid UTF-8")?,
            bare_path.to_str().context("bare path is not valid UTF-8")?,
        ],
    )?;
    run_git(&bare_path, &["config", "uploadpack.allowFilter", "true"])?;
    run_git(
        &bare_path,
        &["config", "uploadpack.allowAnySHA1InWant", "true"],
    )?;

    fs::create_dir_all(&clone_path)
        .with_context(|| format!("creating {}", clone_path.display()))?;
    run_git(&clone_path, &["init", "-q"])?;
    run_git(
        &clone_path,
        &[
            "remote",
            "add",
            "origin",
            &format!("file://{}", bare_path.display()),
        ],
    )?;
    let fetch_start = Instant::now();
    run_git(
        &clone_path,
        &[
            "fetch",
            "--filter=blob:none",
            "origin",
            &format!("{GIT_META_REF}:{CLONE_META_REF}"),
        ],
    )?;
    let fetch_ms = fetch_start.elapsed().as_millis();

    let missing = count_missing_objects(&clone_path)?;

    Ok(CloneMetrics {
        source_git_bytes: dir_size(&source_path.join(".git"))?,
        bare_remote_bytes: dir_size(&bare_path)?,
        blobless_clone_git_bytes: dir_size(&clone_path.join(".git"))?,
        fetch_ms,
        object_counts: git_count_objects(&clone_path)?,
        missing_objects_reported: missing,
    })
}

fn count_missing_objects(repo_path: &Path) -> Result<usize> {
    let output = run_git_output(
        repo_path,
        &["rev-list", "--objects", "--missing=print", CLONE_META_REF],
    )?;
    Ok(output.lines().filter(|line| line.starts_with('?')).count())
}

fn git_count_objects(repo_path: &Path) -> Result<BTreeMap<String, String>> {
    let output = run_git_output(repo_path, &["count-objects", "-v"])?;
    let mut counts = BTreeMap::new();
    for line in output.lines() {
        if let Some((key, value)) = line.split_once(':') {
            counts.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    Ok(counts)
}

fn example_data_paths(repo: &gix::Repository) -> Result<Vec<String>> {
    let tip_oid = peel_ref(repo, GIT_META_REF)?;
    let tree_id = commit_tree_id(repo, tip_oid)?;
    let keys = sync::extract_keys_from_tree(repo, tree_id)?;
    Ok(keys
        .into_iter()
        .take(12)
        .map(|(target_type, target_value, key)| {
            if target_value.is_empty() {
                format!("{target_type} {key}")
            } else {
                format!("{target_type}:{target_value} {key}")
            }
        })
        .collect())
}

fn peel_ref(repo: &gix::Repository, ref_name: &str) -> Result<gix::ObjectId> {
    repo.find_reference(ref_name)
        .with_context(|| format!("finding {ref_name}"))?
        .into_fully_peeled_id()
        .with_context(|| format!("peeling {ref_name}"))
        .map(gix::Id::detach)
}

fn commit_tree_id(repo: &gix::Repository, oid: gix::ObjectId) -> Result<gix::ObjectId> {
    let object = oid
        .attach(repo)
        .object()
        .with_context(|| format!("reading commit {oid}"))?;
    object
        .into_commit()
        .tree_id()
        .with_context(|| format!("reading tree for commit {oid}"))
        .map(gix::Id::detach)
}

fn run_git(repo_path: &Path, args: &[&str]) -> Result<()> {
    run_command(repo_path, "git", &prefix_git_dir_args(args))
}

fn run_git_output(repo_path: &Path, args: &[&str]) -> Result<String> {
    run_command_output(repo_path, "git", &prefix_git_dir_args(args))
}

fn prefix_git_dir_args(args: &[&str]) -> Vec<String> {
    let mut prefixed = vec!["-C".to_string(), ".".to_string()];
    prefixed.extend(args.iter().map(|arg| (*arg).to_string()));
    prefixed
}

fn run_command(cwd: &Path, program: &str, args: &[impl AsRef<str>]) -> Result<()> {
    let output = Command::new(program)
        .args(args.iter().map(AsRef::as_ref))
        .current_dir(cwd)
        .output()
        .with_context(|| format!("running {program}"))?;
    if !output.status.success() {
        bail!(
            "{} failed in {}: {}",
            program,
            cwd.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn run_command_output(cwd: &Path, program: &str, args: &[impl AsRef<str>]) -> Result<String> {
    let output = Command::new(program)
        .args(args.iter().map(AsRef::as_ref))
        .current_dir(cwd)
        .output()
        .with_context(|| format!("running {program}"))?;
    if !output.status.success() {
        bail!(
            "{} failed in {}: {}",
            program,
            cwd.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context("command output was not UTF-8")
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in fs::read_dir(&next).with_context(|| format!("reading {}", next.display()))? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

fn print_report(report: &BenchReport) {
    println!("metadata scale benchmark");
    println!("workspace: {}", report.workspace.display());
    println!("profile: {}", report.profile.source);
    println!(
        "sample: {} PRs, {} issues",
        report.profile.sampled_pull_requests, report.profile.sampled_issues
    );
    println!(
        "plan: {} PRs, {} issues, {} events -> {} expected metadata commits",
        report.plan.pull_requests,
        report.plan.issues,
        report.plan.modeled_events,
        report.plan.expected_metadata_commits
    );
    println!(
        "generated: {} events, {} commits, serialize wall {} ms",
        report.generation.events_written,
        report.generation.metadata_commits,
        report.generation.serialize_wall_ms
    );
    println!(
        "sizes: source .git {}, bare remote {}, blobless .git {}",
        human_bytes(report.clone.source_git_bytes),
        human_bytes(report.clone.bare_remote_bytes),
        human_bytes(report.clone.blobless_clone_git_bytes)
    );
    println!(
        "blobless fetch: {} ms, missing objects reported: {}",
        report.clone.fetch_ms, report.clone.missing_objects_reported
    );
    println!(
        "keyspace: {} tip keys in {} ms, {} historical promised keys in {} ms",
        report.processing.tip_key_count,
        report.processing.tip_tree_extract_ms,
        report.processing.history_promisor_entries,
        report.processing.history_index_ms
    );
    println!(
        "tree processing: parsed {} tip values in {} ms, force-full tree update {} ms",
        report.processing.full_tip_values,
        report.processing.full_tip_parse_ms,
        report.processing.force_full_serialize_ms
    );
    println!("example data:");
    for item in &report.example_data {
        println!("  {item}");
    }
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let as_f64 = bytes as f64;
    if as_f64 >= GIB {
        format!("{:.2} GiB", as_f64 / GIB)
    } else if as_f64 >= MIB {
        format!("{:.2} MiB", as_f64 / MIB)
    } else if as_f64 >= KIB {
        format!("{:.2} KiB", as_f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[allow(dead_code)]
fn duration_ms(duration: Duration) -> u128 {
    duration.as_millis()
}
