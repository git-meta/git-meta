/// Typed error types for all gmeta-core operations.
pub mod error;

/// Local SQLite database for caching and querying metadata.
pub mod db;

// --- Primary API modules (documented, stable) ---

/// Materialize remote metadata into the local SQLite store.
pub mod materialize;
/// Pull remote metadata: fetch, materialize, and index history.
pub mod pull;
/// Push local metadata to a remote: serialize, push, and conflict resolution.
pub mod push;
/// Serialize local metadata to Git tree(s) and commit(s).
pub mod serialize;
/// The library entry point: a session combining a git repo with a metadata store.
pub mod session;
/// Session-scoped target handle with automatic email and timestamp.
pub mod session_handle;
/// Git tree serialization, parsing, merging, and filtering.
pub mod tree;
/// Core metadata types: targets, value types, and path-building helpers.
pub mod types;

// --- Internal modules (not part of the public API) ---

pub(crate) mod git_utils;
pub(crate) mod list_value;
pub(crate) mod prune;
pub(crate) mod sync;

// --- Public API re-exports ---

// Core types
pub use db::Store;
pub use error::{Error, Result};
pub use session::Session;
pub use session_handle::SessionTargetHandle;
pub use types::{MetaValue, Target, TargetType, ValueType};

// ListEntry is part of MetaValue::List, so it's genuinely public.
pub use list_value::ListEntry;

// Workflow output types
pub use materialize::{MaterializeOutput, MaterializeRefResult, MaterializeStrategy};
pub use pull::PullOutput;
pub use push::PushOutput;
pub use serialize::SerializeOutput;
pub use sync::CommitChange;

// --- CLI internals (not for library consumers) ---
// These are re-exported for the CLI crate's use but hidden from docs.
// Library consumers should use Session methods and MetaValue instead.

#[doc(hidden)]
pub mod __private {
    pub use crate::db::Store;
    pub use crate::list_value::{
        encode_entries, list_values_from_json, parse_entries, parse_timestamp_from_entry_name,
    };
    pub use crate::prune::{parse_since_to_cutoff_ms, parse_size, read_prune_rules, PruneRules};
    pub use crate::sync::parse_commit_changes;
}
