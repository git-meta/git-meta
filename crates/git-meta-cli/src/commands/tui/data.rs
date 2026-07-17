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

    /// Keys of one target, optionally fuzzy-filtered on key name or string
    /// value content (same match rule as `git meta inspect`).
    pub(super) fn key_rows(
        &self,
        target_type: &TargetType,
        target_value: &str,
        filter: &str,
    ) -> Vec<KeyRow> {
        let term = filter.to_lowercase();
        self.entries
            .iter()
            .filter(|e| &e.target_type == target_type && e.target_value == target_value)
            .filter(|e| {
                term.is_empty()
                    || fuzzy_matches(&term, &e.key)
                    || (e.value_type == ValueType::String
                        && fuzzy_matches(&term, &decode_string_value(&e.value)))
            })
            .map(|e| KeyRow {
                key: e.key.clone(),
                value: e.value.clone(),
                value_type: e.value_type.clone(),
                is_git_ref: e.is_git_ref,
                last_timestamp: e.last_timestamp,
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
    fn key_rows_filter_matches_key_or_string_value() {
        let snap = snapshot();
        let rows = snap.key_rows(&TargetType::Commit, "aaa111", "");
        assert_eq!(rows.len(), 2);

        // Matches the decoded string value "approved".
        let rows = snap.key_rows(&TargetType::Commit, "aaa111", "approved");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "review:status");
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
