/// Typed error types for all gmeta-core operations.
pub mod error;

/// Local SQLite database for caching and querying metadata.
pub mod db;

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

// Internal modules — only visible when the `internal` feature is enabled.
// The CLI enables this feature; library consumers do not.

#[cfg(not(feature = "internal"))]
pub(crate) mod git_utils;
#[cfg(feature = "internal")]
pub mod git_utils;

#[cfg(not(feature = "internal"))]
pub(crate) mod list_value;
#[cfg(feature = "internal")]
pub mod list_value;

#[cfg(not(feature = "internal"))]
pub(crate) mod prune;
#[cfg(feature = "internal")]
pub mod prune;

#[cfg(not(feature = "internal"))]
pub(crate) mod sync;
#[cfg(feature = "internal")]
pub mod sync;

// --- Public API re-exports ---

pub use db::Store;
pub use error::{Error, Result};
pub use list_value::ListEntry;
pub use session::Session;
pub use session_handle::SessionTargetHandle;
pub use sync::CommitChange;
pub use types::{MetaValue, Target, TargetType, ValueType};

// Workflow output types
pub use materialize::{MaterializeOutput, MaterializeRefResult, MaterializeStrategy};
pub use pull::PullOutput;
pub use push::PushOutput;
pub use serialize::SerializeOutput;
