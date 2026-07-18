//! Data the TUI renders: an eagerly loaded snapshot of all metadata plus
//! lazily loaded per-key detail.
//!
//! The snapshot is loaded once at startup (the store is local SQLite, so
//! this is cheap) and every list view is derived from it on demand. Only
//! the detail view issues further queries, because value decoding, blob
//! resolution, and authorship lookups are per-key.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use time::OffsetDateTime;

use git_meta_lib::db::types::{Authorship, SerializableEntry};
use git_meta_lib::types::{MetaValue, Target, TargetType, ValueType};
use git_meta_lib::Session;

use crate::commands::get::resolve_git_ref;
use crate::commands::inspect::{decode_string_value, fuzzy_matches};

/// All non-promised metadata entries plus promised-key counts per type.
pub(super) struct MetaSnapshot {
    /// Sorted by (target_type, target_value, key) — the store's query order.
    pub(super) entries: Vec<SerializableEntry>,
    pub(super) promised_counts: BTreeMap<TargetType, u64>,
}

/// One overview row: aggregate stats for a target type.
pub(super) struct TypeRow {
    pub(super) target_type: TargetType,
    pub(super) key_count: u64,
    pub(super) target_count: usize,
    pub(super) promised: u64,
}

/// One target-list row: a target value with its key count and freshness.
pub(super) struct TargetRow {
    pub(super) target_value: String,
    pub(super) key_count: usize,
    pub(super) last_timestamp: i64,
}

/// One key-list row. Carries the raw stored value so the renderer can
/// format a preview at the actual terminal width.
pub(super) struct KeyRow {
    pub(super) key: String,
    pub(super) value: String,
    pub(super) value_type: ValueType,
    pub(super) is_git_ref: bool,
    pub(super) last_timestamp: i64,
}

impl MetaSnapshot {
    pub(super) fn load(session: &Session) -> Result<Self> {
        let entries = session.store().get_all_metadata()?;
        let mut promised_counts = BTreeMap::new();
        for (type_str, count) in session.store().count_promised_keys()? {
            if let Ok(target_type) = type_str.parse::<TargetType>() {
                promised_counts.insert(target_type, count);
            }
        }
        Ok(Self {
            entries,
            promised_counts,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.promised_counts.is_empty()
    }

    /// Aggregate key/target counts per target type, including types that
    /// only have promised (not-yet-fetched) keys.
    pub(super) fn type_rows(&self) -> Vec<TypeRow> {
        let mut stats: BTreeMap<TargetType, (u64, BTreeSet<&str>)> = BTreeMap::new();
        for e in &self.entries {
            let slot = stats.entry(e.target_type.clone()).or_default();
            slot.0 += 1;
            slot.1.insert(e.target_value.as_str());
        }
        for target_type in self.promised_counts.keys() {
            stats.entry(target_type.clone()).or_default();
        }
        stats
            .into_iter()
            .map(|(target_type, (key_count, targets))| TypeRow {
                promised: self.promised_counts.get(&target_type).copied().unwrap_or(0),
                target_count: targets.len(),
                key_count,
                target_type,
            })
            .collect()
    }

    /// Targets of one type, grouped with key counts, optionally fuzzy-filtered.
    ///
    /// A target matches the filter when its value or any of its keys does,
    /// mirroring `git meta inspect <type> <term>`.
    pub(super) fn target_rows(&self, target_type: &TargetType, filter: &str) -> Vec<TargetRow> {
        let term = filter.to_lowercase();
        let mut by_target: BTreeMap<&str, (usize, i64, bool)> = BTreeMap::new();
        for e in self
            .entries
            .iter()
            .filter(|e| &e.target_type == target_type)
        {
            let slot = by_target
                .entry(e.target_value.as_str())
                .or_insert((0, i64::MIN, false));
            slot.0 += 1;
            slot.1 = slot.1.max(e.last_timestamp);
            if !term.is_empty() && !slot.2 {
                slot.2 = fuzzy_matches(&term, &e.target_value) || fuzzy_matches(&term, &e.key);
            }
        }
        by_target
            .into_iter()
            .filter(|(_, (_, _, matched))| term.is_empty() || *matched)
            .map(|(target_value, (key_count, last_timestamp, _))| TargetRow {
                target_value: target_value.to_string(),
                key_count,
                last_timestamp,
            })
            .collect()
    }

    /// One level of a target's key namespace tree.
    ///
    /// Keys are colon-namespaced paths; this renders the level below
    /// `prefix` (empty = root): sibling keys become [`KeyTreeRow::Leaf`]
    /// rows, deeper keys are grouped by their next segment into
    /// [`KeyTreeRow::Namespace`] rows. A segment that is both a key and a
    /// namespace parent yields both rows.
    ///
    /// The filter fuzzy-matches the key path below `prefix` or (for
    /// string values) the value content, same as `git meta inspect`.
    pub(super) fn key_tree_rows(
        &self,
        target_type: &TargetType,
        target_value: &str,
        prefix: &str,
        filter: &str,
    ) -> Vec<KeyTreeRow> {
        let term = filter.to_lowercase();
        let prefix_colon = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}:")
        };

        #[derive(Default)]
        struct Slot {
            leaf: Option<KeyRow>,
            child_keys: usize,
            last_timestamp: i64,
            matched: bool,
        }
        let mut by_segment: BTreeMap<String, Slot> = BTreeMap::new();

        for e in self
            .entries
            .iter()
            .filter(|e| &e.target_type == target_type && e.target_value == target_value)
        {
            let Some(rest) = e.key.strip_prefix(&prefix_colon) else {
                continue;
            };
            let matched = term.is_empty()
                || fuzzy_matches(&term, rest)
                || (e.value_type == ValueType::String
                    && fuzzy_matches(&term, &decode_string_value(&e.value)));
            let (segment, is_leaf) = match rest.split_once(':') {
                Some((segment, _)) => (segment, false),
                None => (rest, true),
            };
            let slot = by_segment.entry(segment.to_string()).or_default();
            slot.matched |= matched;
            slot.last_timestamp = slot.last_timestamp.max(e.last_timestamp);
            if is_leaf {
                slot.leaf = Some(KeyRow {
                    key: e.key.clone(),
                    value: e.value.clone(),
                    value_type: e.value_type.clone(),
                    is_git_ref: e.is_git_ref,
                    last_timestamp: e.last_timestamp,
                });
            } else {
                slot.child_keys += 1;
            }
        }

        let mut rows = Vec::new();
        for (segment, slot) in by_segment {
            if !slot.matched {
                continue;
            }
            if let Some(row) = slot.leaf {
                rows.push(KeyTreeRow::Leaf {
                    segment: segment.clone(),
                    row,
                });
            }
            if slot.child_keys > 0 {
                rows.push(KeyTreeRow::Namespace {
                    segment,
                    key_count: slot.child_keys,
                    last_timestamp: slot.last_timestamp,
                });
            }
        }
        rows
    }
}

/// One row of a key-namespace level: a browsable namespace or a real key.
pub(super) enum KeyTreeRow {
    Namespace {
        segment: String,
        /// Number of keys anywhere below this namespace.
        key_count: usize,
        /// Most recent update among those keys.
        last_timestamp: i64,
    },
    Leaf {
        /// The key's final path segment, for display at this level.
        segment: String,
        row: KeyRow,
    },
}

/// Extend a key namespace prefix by one segment.
pub(super) fn join_prefix(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}:{segment}")
    }
}

/// Weeks covered by the overview activity sparkline.
pub(super) const ACTIVITY_WEEKS: usize = 12;

/// Aggregate statistics shown in the overview's statistics panel.
pub(super) struct OverviewStats {
    pub(super) total_keys: usize,
    pub(super) promised_keys: u64,
    /// Keys whose value is stored as a git blob reference.
    pub(super) git_ref_keys: usize,
    pub(super) strings: usize,
    pub(super) lists: usize,
    pub(super) sets: usize,
    pub(super) distinct_targets: usize,
    pub(super) target_types: usize,
    /// Total size of inline stored values.
    pub(super) value_bytes: usize,
    pub(super) last_update_ms: Option<i64>,
    /// Keys updated per week over [`ACTIVITY_WEEKS`], oldest first.
    pub(super) weekly: Vec<u64>,
    /// Top-level key namespace with the most keys.
    pub(super) top_namespace: Option<(String, usize)>,
    /// Distinct commits carrying commit-target metadata.
    pub(super) commits_with_meta: usize,
    /// Branch name and commit count of the repository's main history;
    /// filled in by the event loop because it needs the repository.
    pub(super) main_branch: Option<(String, usize)>,
}

impl OverviewStats {
    pub(super) fn compute(snapshot: &MetaSnapshot, now_ms: i64) -> Self {
        let week_ms = time::Duration::weeks(1).whole_milliseconds() as i64;
        let mut weekly = vec![0u64; ACTIVITY_WEEKS];
        let mut targets: BTreeSet<(&TargetType, &str)> = BTreeSet::new();
        let mut commits: BTreeSet<&str> = BTreeSet::new();
        let mut namespaces: BTreeMap<&str, usize> = BTreeMap::new();
        let (mut strings, mut lists, mut sets, mut git_refs, mut bytes) = (0, 0, 0, 0, 0);
        let mut last_update_ms = None;

        for e in &snapshot.entries {
            match e.value_type {
                ValueType::String => strings += 1,
                ValueType::List => lists += 1,
                ValueType::Set => sets += 1,
                _ => {}
            }
            if e.is_git_ref {
                git_refs += 1;
            }
            bytes += e.value.len();
            targets.insert((&e.target_type, e.target_value.as_str()));
            if e.target_type == TargetType::Commit {
                commits.insert(e.target_value.as_str());
            }
            if let Some(namespace) = e.key.split(':').next() {
                *namespaces.entry(namespace).or_default() += 1;
            }
            last_update_ms = last_update_ms.max(Some(e.last_timestamp));

            let weeks_ago = now_ms.saturating_sub(e.last_timestamp) / week_ms.max(1);
            if (weeks_ago as usize) < ACTIVITY_WEEKS {
                weekly[ACTIVITY_WEEKS - 1 - weeks_ago as usize] += 1;
            }
        }

        let target_types = targets
            .iter()
            .map(|(target_type, _)| *target_type)
            .collect::<BTreeSet<_>>()
            .len();
        let top_namespace = namespaces
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(namespace, count)| (namespace.to_string(), count));

        Self {
            total_keys: snapshot.entries.len(),
            promised_keys: snapshot.promised_counts.values().sum(),
            git_ref_keys: git_refs,
            strings,
            lists,
            sets,
            distinct_targets: targets.len(),
            target_types,
            value_bytes: bytes,
            last_update_ms,
            weekly,
            top_namespace,
            commits_with_meta: commits.len(),
            main_branch: None,
        }
    }
}

/// Human-readable size: `812 B`, `4.3 KB`, `1.2 MB`.
pub(super) fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// One global-search result: a key addressed by its full path.
pub(super) struct SearchRow {
    pub(super) target_type: TargetType,
    pub(super) target_value: String,
    pub(super) key: String,
    pub(super) is_git_ref: bool,
    pub(super) last_timestamp: i64,
    /// Display path the query matched against, e.g. `commit:abc123 agent:model`.
    pub(super) path: String,
}

/// The display path of a key: `type:value key`, or `project key` for the
/// implicit project target.
pub(super) fn key_path(target_type: &TargetType, target_value: &str, key: &str) -> String {
    if *target_type == TargetType::Project {
        format!("project {key}")
    } else {
        format!("{target_type}:{target_value} {key}")
    }
}

impl MetaSnapshot {
    /// All keys whose full path fuzzy-matches the query, in snapshot order.
    /// Whitespace splits the query into words that must each match
    /// independently (in any order), so `agent bbb` finds
    /// `commit:bbb222 agent:model`. An empty query matches everything.
    pub(super) fn search_rows(&self, query: &str) -> Vec<SearchRow> {
        let lowered = query.to_lowercase();
        let words: Vec<&str> = lowered.split_whitespace().collect();
        self.entries
            .iter()
            .filter_map(|e| {
                let path = key_path(&e.target_type, &e.target_value, &e.key);
                words
                    .iter()
                    .all(|word| fuzzy_matches(word, &path))
                    .then(|| SearchRow {
                        target_type: e.target_type.clone(),
                        target_value: e.target_value.clone(),
                        key: e.key.clone(),
                        is_git_ref: e.is_git_ref,
                        last_timestamp: e.last_timestamp,
                        path,
                    })
            })
            .collect()
    }
}

/// Fully decoded value plus provenance for the detail view.
pub(super) struct DetailData {
    pub(super) value: MetaValue,
    pub(super) last_timestamp: i64,
    pub(super) authorship: Option<Authorship>,
}

/// Load and decode one key's value for the detail view.
///
/// Git-blob-backed string values are resolved to their content; if the
/// blob is unavailable (e.g. a blobless clone) a placeholder is shown
/// instead of failing the whole view.
pub(super) fn load_detail(
    session: &Session,
    target_type: &TargetType,
    target_value: &str,
    key: &str,
    is_git_ref: bool,
    last_timestamp: i64,
) -> Result<DetailData> {
    let target = if *target_type == TargetType::Project {
        Target::project()
    } else {
        Target::from_parts(target_type.clone(), Some(target_value.to_string()))
    };
    let handle = session.target(&target);

    let mut value = handle
        .get_value(key)?
        .with_context(|| format!("key '{key}' not found"))?;
    if is_git_ref {
        if let MetaValue::String(sha) = &value {
            let content = resolve_git_ref(session.repo(), sha)
                .unwrap_or_else(|_| format!("[git blob {sha} unavailable]"));
            value = MetaValue::String(content);
        }
    }
    let authorship = handle.get_authorship(key)?;

    Ok(DetailData {
        value,
        last_timestamp,
        authorship,
    })
}

/// Absolute timestamp for the detail footer, e.g. `2026-07-17 14:32`.
pub(super) fn format_timestamp(ms: i64) -> String {
    let Ok(dt) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000) else {
        return "?".to_string();
    };
    time::format_description::parse("[year]-[month]-[day] [hour]:[minute]")
        .ok()
        .and_then(|fmt| dt.format(&fmt).ok())
        .unwrap_or_else(|| "?".to_string())
}

/// Compact relative timestamp for list rows, e.g. `3d ago`; falls back
/// to an absolute date beyond 30 days.
pub(super) fn format_relative(ms: i64, now_ms: i64) -> String {
    let delta = time::Duration::milliseconds(now_ms.saturating_sub(ms));
    if delta < time::Duration::minutes(1) {
        "just now".to_string()
    } else if delta < time::Duration::hours(1) {
        format!("{}m ago", delta.whole_minutes())
    } else if delta < time::Duration::days(1) {
        format!("{}h ago", delta.whole_hours())
    } else if delta < time::Duration::days(30) {
        format!("{}d ago", delta.whole_days())
    } else {
        let Ok(dt) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000) else {
            return "?".to_string();
        };
        time::format_description::parse("[year]-[month]-[day]")
            .ok()
            .and_then(|fmt| dt.format(&fmt).ok())
            .unwrap_or_else(|| "?".to_string())
    }
}

/// Test-only constructor for a plain (non-git-ref) entry, shared by the
/// state-machine and rendering tests in sibling modules.
#[cfg(test)]
pub(super) fn test_entry(
    target_type: TargetType,
    target_value: &str,
    key: &str,
    value: &str,
    value_type: ValueType,
    last_timestamp: i64,
) -> SerializableEntry {
    SerializableEntry {
        target_type,
        target_value: target_value.to_string(),
        key: key.to_string(),
        value: value.to_string(),
        value_type,
        last_timestamp,
        is_git_ref: false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use super::test_entry as entry;

    fn snapshot() -> MetaSnapshot {
        MetaSnapshot {
            entries: vec![
                entry(
                    TargetType::Commit,
                    "aaa111",
                    "agent:model",
                    "\"claude\"",
                    ValueType::String,
                    1_000,
                ),
                entry(
                    TargetType::Commit,
                    "aaa111",
                    "review:status",
                    "\"approved\"",
                    ValueType::String,
                    3_000,
                ),
                entry(
                    TargetType::Commit,
                    "bbb222",
                    "agent:model",
                    "\"codex\"",
                    ValueType::String,
                    2_000,
                ),
                entry(
                    TargetType::Project,
                    "",
                    "ci:url",
                    "\"https://ci.example\"",
                    ValueType::String,
                    500,
                ),
            ],
            promised_counts: BTreeMap::from([(TargetType::Branch, 4)]),
        }
    }

    #[test]
    fn type_rows_aggregate_counts_and_include_promised_only_types() {
        let rows = snapshot().type_rows();
        assert_eq!(rows.len(), 3);

        let commit = rows
            .iter()
            .find(|r| r.target_type == TargetType::Commit)
            .unwrap();
        assert_eq!(commit.key_count, 3);
        assert_eq!(commit.target_count, 2);
        assert_eq!(commit.promised, 0);

        let branch = rows
            .iter()
            .find(|r| r.target_type == TargetType::Branch)
            .unwrap();
        assert_eq!(branch.key_count, 0);
        assert_eq!(branch.promised, 4);
    }

    #[test]
    fn target_rows_group_and_track_latest_timestamp() {
        let rows = snapshot().target_rows(&TargetType::Commit, "");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].target_value, "aaa111");
        assert_eq!(rows[0].key_count, 2);
        assert_eq!(rows[0].last_timestamp, 3_000);
    }

    #[test]
    fn target_rows_filter_matches_target_value_or_key() {
        let snap = snapshot();
        // "bbb" matches only the second commit's target value.
        let rows = snap.target_rows(&TargetType::Commit, "bbb");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].target_value, "bbb222");

        // "review" matches a key that only aaa111 has.
        let rows = snap.target_rows(&TargetType::Commit, "review");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].target_value, "aaa111");
    }

    #[test]
    fn key_tree_groups_by_namespace_level() {
        let snap = snapshot();

        // Root of aaa111: two namespace groups, no leaves.
        let rows = snap.key_tree_rows(&TargetType::Commit, "aaa111", "", "");
        assert_eq!(rows.len(), 2);
        match &rows[0] {
            KeyTreeRow::Namespace {
                segment, key_count, ..
            } => {
                assert_eq!(segment, "agent");
                assert_eq!(*key_count, 1);
            }
            KeyTreeRow::Leaf { .. } => panic!("expected namespace row"),
        }

        // Drilling into a namespace exposes its leaf keys.
        let rows = snap.key_tree_rows(&TargetType::Commit, "aaa111", "agent", "");
        assert_eq!(rows.len(), 1);
        match &rows[0] {
            KeyTreeRow::Leaf { segment, row } => {
                assert_eq!(segment, "model");
                assert_eq!(row.key, "agent:model");
            }
            KeyTreeRow::Namespace { .. } => panic!("expected leaf row"),
        }
    }

    #[test]
    fn key_tree_segment_can_be_both_leaf_and_namespace() {
        let snap = MetaSnapshot {
            entries: vec![
                entry(
                    TargetType::Project,
                    "",
                    "ci",
                    "\"top\"",
                    ValueType::String,
                    1,
                ),
                entry(
                    TargetType::Project,
                    "",
                    "ci:url",
                    "\"https://ci.example\"",
                    ValueType::String,
                    2,
                ),
            ],
            promised_counts: BTreeMap::new(),
        };
        let rows = snap.key_tree_rows(&TargetType::Project, "", "", "");
        assert_eq!(rows.len(), 2);
        assert!(matches!(&rows[0], KeyTreeRow::Leaf { row, .. } if row.key == "ci"));
        assert!(
            matches!(&rows[1], KeyTreeRow::Namespace { segment, key_count, last_timestamp }
                if segment == "ci" && *key_count == 1 && *last_timestamp == 2)
        );
    }

    #[test]
    fn key_tree_filter_matches_key_path_or_string_value() {
        let snap = snapshot();

        // Matches the decoded string value "approved" of review:status.
        let rows = snap.key_tree_rows(&TargetType::Commit, "aaa111", "", "approved");
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0], KeyTreeRow::Namespace { segment, .. } if segment == "review"));

        // Matches the key path below the prefix.
        let rows = snap.key_tree_rows(&TargetType::Commit, "aaa111", "", "status");
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0], KeyTreeRow::Namespace { segment, .. } if segment == "review"));
    }

    #[test]
    fn search_rows_match_full_key_paths() {
        let snap = snapshot();
        assert_eq!(snap.search_rows("").len(), 4);

        // Words match independently, in any order.
        for query in ["aaa review", "review aaa"] {
            let rows = snap.search_rows(query);
            assert_eq!(rows.len(), 1, "query {query:?}");
            assert_eq!(rows[0].key, "review:status");
            assert_eq!(rows[0].path, "commit:aaa111 review:status");
        }

        // Project keys use the bare `project` prefix.
        let rows = snap.search_rows("proj ci");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].target_type, TargetType::Project);
        assert_eq!(rows[0].path, "project ci:url");

        assert!(snap.search_rows("no-such-thing").is_empty());
    }

    #[test]
    fn overview_stats_aggregate_snapshot() {
        let week = time::Duration::weeks(1).whole_milliseconds() as i64;
        let now = week * 100;
        let snap = MetaSnapshot {
            entries: vec![
                entry(
                    TargetType::Commit,
                    "aaa111",
                    "agent:model",
                    "\"claude\"",
                    ValueType::String,
                    now - week / 2,
                ),
                entry(
                    TargetType::Commit,
                    "bbb222",
                    "agent:prompt",
                    "\"fix it\"",
                    ValueType::String,
                    now - week - week / 2,
                ),
                entry(
                    TargetType::Project,
                    "",
                    "ci:runs",
                    "[]",
                    ValueType::List,
                    now - week * 50,
                ),
            ],
            promised_counts: BTreeMap::from([(TargetType::Branch, 3)]),
        };
        let stats = OverviewStats::compute(&snap, now);

        assert_eq!(stats.total_keys, 3);
        assert_eq!(stats.promised_keys, 3);
        assert_eq!(stats.strings, 2);
        assert_eq!(stats.lists, 1);
        assert_eq!(stats.sets, 0);
        assert_eq!(stats.distinct_targets, 3);
        assert_eq!(stats.target_types, 2);
        assert_eq!(stats.commits_with_meta, 2);
        assert_eq!(stats.top_namespace, Some(("agent".to_string(), 2)));
        assert_eq!(stats.last_update_ms, Some(now - week / 2));
        assert_eq!(stats.value_bytes, 8 + 8 + 2);

        // Newest entry lands in the current (last) week bucket, the older
        // one a week earlier; the 50-week-old entry is outside the window.
        assert_eq!(stats.weekly.len(), ACTIVITY_WEEKS);
        assert_eq!(stats.weekly[ACTIVITY_WEEKS - 1], 1);
        assert_eq!(stats.weekly[ACTIVITY_WEEKS - 2], 1);
        assert_eq!(stats.weekly.iter().sum::<u64>(), 2);
    }

    #[test]
    fn bytes_format_scales_units() {
        assert_eq!(format_bytes(812), "812 B");
        assert_eq!(format_bytes(4404), "4.3 KB");
        assert_eq!(format_bytes(1_300_000), "1.2 MB");
    }

    #[test]
    fn relative_timestamps_scale_with_age() {
        let minute = 60_000;
        let now = 100 * 24 * 60 * minute;
        assert_eq!(format_relative(now - 30_000, now), "just now");
        assert_eq!(format_relative(now - 5 * minute, now), "5m ago");
        assert_eq!(format_relative(now - 3 * 60 * minute, now), "3h ago");
        assert_eq!(format_relative(now - 2 * 24 * 60 * minute, now), "2d ago");
        // Beyond 30 days: absolute date.
        assert!(format_relative(0, now).starts_with("1970-"));
    }
}
