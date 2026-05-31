use std::path::{Path, PathBuf};

use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use time::OffsetDateTime;

/// A session combining a Git repository with its git-meta metadata store.
///
/// This is the primary entry point for git-meta consumers. It owns the
/// `gix::Repository`, the SQLite [`Store`](crate::db::Store), and resolved
/// configuration values (namespace, user email).
///
/// # Timestamps
///
/// By default, workflow operations use the wall clock for timestamps.
/// For deterministic tests, call [`with_timestamp()`](Self::with_timestamp)
/// to pin all operations to a fixed time:
///
/// ```ignore
/// let session = Session::discover()?.with_timestamp(1_700_000_000_000);
/// session.serialize()?; // uses the fixed timestamp
/// ```
///
/// # Example
///
/// ```no_run
/// use git_meta_lib::Session;
///
/// let session = Session::discover()?;
/// println!("email: {}", session.email());
/// println!("namespace: {}", session.namespace());
/// # Ok::<(), git_meta_lib::Error>(())
/// ```
#[derive(Debug)]
#[must_use]
pub struct Session {
    pub(crate) repo: gix::Repository,
    pub(crate) store: crate::db::Store,
    pub(crate) namespace: String,
    pub(crate) email: String,
    pub(crate) name: String,
    pub(crate) timestamp_override: Option<i64>,
}

/// Git object entry kind for simple tree walking APIs.
#[cfg(feature = "internal")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitTreeEntryKind {
    /// A Git blob object.
    Blob,
    /// A Git tree object.
    Tree,
    /// Any other object kind.
    Other,
}

/// A tree entry represented with plain strings instead of gitoxide types.
#[cfg(feature = "internal")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitTreeEntryInfo {
    /// Entry filename.
    pub name: String,
    /// Object id as a hex SHA string.
    pub oid: String,
    /// Entry kind.
    pub kind: GitTreeEntryKind,
}

/// Commit data represented with plain strings and integers for CLI consumers.
#[cfg(feature = "internal")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitInfo {
    /// Commit id as a hex SHA string.
    pub oid: String,
    /// Commit tree id as a hex SHA string.
    pub tree_oid: String,
    /// Author name.
    pub author_name: String,
    /// Author email.
    pub author_email: String,
    /// Author timestamp in seconds since the Unix epoch.
    pub author_time_seconds: i64,
    /// Commit message.
    pub message: String,
    /// Number of parent commits.
    pub parent_count: usize,
}

/// A reference and its peeled object id, represented as strings.
#[cfg(feature = "internal")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRefInfo {
    /// Full ref name.
    pub name: String,
    /// Peeled object id as a hex SHA string.
    pub oid: String,
}

impl Session {
    /// Discover a git repository from the current directory and open its
    /// metadata store.
    ///
    /// Walks upward from the current directory to find a `.git` directory,
    /// reads `user.email` and `meta.namespace` from git config, and opens
    /// (or creates) the SQLite database at `.git/git-meta.sqlite`.
    pub fn discover() -> crate::error::Result<Self> {
        let repo = crate::git_utils::discover_repo()?;
        Self::from_repo(repo)
    }

    /// Open a session for a known repository.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let repo = gix::open(".")?;
    /// let session = Session::open(repo.path())?;
    /// ```
    pub fn open(directory: impl Into<PathBuf>) -> crate::error::Result<Self> {
        Self::from_repo(gix::open(directory).map_err(|err| crate::error::Error::Git(err.into()))?)
    }

    /// Pin all workflow operations to a fixed timestamp.
    ///
    /// The value is milliseconds since the Unix epoch. When set,
    /// [`now()`](Self::now) returns this value instead of the wall clock.
    /// Useful for deterministic tests and replay scenarios.
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_override = Some(timestamp_ms);
        self
    }

    /// The current timestamp in milliseconds since the Unix epoch.
    ///
    /// Returns the fixed timestamp if [`with_timestamp()`](Self::with_timestamp)
    /// was called, otherwise the wall clock.
    pub(crate) fn now(&self) -> i64 {
        self.timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000)
    }

    fn from_repo(repo: gix::Repository) -> crate::error::Result<Self> {
        let db_path = crate::git_utils::db_path(&repo)?;
        let email = crate::git_utils::get_email(&repo)?;
        let name = crate::git_utils::get_name(&repo)?;
        let namespace = crate::git_utils::get_namespace(&repo)?;
        let store = crate::db::Store::open_with_repo(&db_path, repo.clone())?;

        Ok(Self {
            repo,
            store,
            namespace,
            email,
            name,
            timestamp_override: None,
        })
    }

    /// Access the metadata store directly.
    ///
    /// This is an advanced API for custom queries. Most consumers should use
    /// [`target()`](Self::target) for read/write operations.
    #[cfg(feature = "internal")]
    pub fn store(&self) -> &crate::db::Store {
        &self.store
    }

    /// Access the underlying gix repository.
    ///
    /// This is an advanced API. Most consumers should use Session's workflow
    /// methods (serialize, materialize, pull, push) instead.
    #[cfg(feature = "internal")]
    pub fn repo(&self) -> &gix::Repository {
        &self.repo
    }

    /// The metadata namespace (from git config `meta.namespace`, default `"meta"`).
    ///
    /// Used to construct ref paths like `refs/{namespace}/local/main`.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The user email from git config `user.email`.
    ///
    /// Used for authorship tracking on metadata mutations.
    pub fn email(&self) -> &str {
        &self.email
    }

    /// The user name from git config `user.name`.
    ///
    /// Used for commit signatures during serialization.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The local serialization ref path (e.g. `refs/meta/local/main`).
    pub(crate) fn local_ref(&self) -> String {
        format!("refs/{}/local/main", self.namespace)
    }

    /// A ref path for a named destination (e.g. `refs/meta/local/{destination}`).
    pub(crate) fn destination_ref(&self, destination: &str) -> String {
        format!("refs/{}/local/{}", self.namespace, destination)
    }

    /// Create a scoped handle for operations on a specific target.
    ///
    /// The handle carries the session's email and timestamp, so write
    /// operations don't need them as parameters:
    ///
    /// ```ignore
    /// let handle = session.target(&Target::parse("commit:abc123")?);
    /// handle.set_value("key", &MetaValue::String("value".into()))?;
    /// ```
    pub fn target(
        &self,
        target: &crate::types::Target,
    ) -> crate::session_handle::SessionTargetHandle<'_> {
        crate::session_handle::SessionTargetHandle::new(self, target.clone())
    }

    /// Remove all local metadata keys matching a key or key namespace.
    ///
    /// `key_prefix` matches either an exact key or keys below it in the
    /// colon-separated namespace. For example, `agent` clears both `agent` and
    /// `agent:model` across every target.
    ///
    /// Returns the number of metadata entries removed.
    pub fn clear_key_prefix(&self, key_prefix: &str) -> crate::error::Result<usize> {
        self.store
            .clear_key_prefix(key_prefix, self.email(), self.now())
    }

    /// Resolve a target's partial commit SHA using this session's repository.
    ///
    /// Returns a new target with the full SHA if the target was a partial commit,
    /// or a clone of the original target otherwise.
    pub fn resolve_target(
        &self,
        target: &crate::types::Target,
    ) -> crate::error::Result<crate::types::Target> {
        target.resolve(&self.repo)
    }

    /// Resolve which metadata remote to use.
    ///
    /// If `remote` is `Some`, validates that it is a configured meta remote.
    /// If `None`, returns the first configured meta remote.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to validate; if `None`, the first
    ///   configured metadata remote is returned
    ///
    /// # Returns
    ///
    /// The name of the resolved meta remote.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NoRemotes`](crate::error::Error::NoRemotes) if no
    /// meta remotes are configured, or
    /// [`Error::RemoteNotFound`](crate::error::Error::RemoteNotFound) if the
    /// specified name is not a meta remote.
    pub fn resolve_remote(&self, remote: Option<&str>) -> crate::error::Result<String> {
        crate::git_utils::resolve_meta_remote(&self.repo, remote)
    }

    /// Index metadata keys from commit history for blobless clone support.
    ///
    /// Walks commits from `tip_oid` backward (optionally stopping at `old_tip`)
    /// and inserts promisor entries for all keys found in commit messages or
    /// root-commit trees. Returns the number of new entries indexed.
    ///
    /// Call this after a blobless fetch to build an index of historical keys
    /// that can be hydrated on demand.
    pub(crate) fn index_history(
        &self,
        tip_oid: gix::ObjectId,
        old_tip: Option<gix::ObjectId>,
    ) -> crate::error::Result<usize> {
        crate::sync::insert_promisor_entries(&self.repo, &self.store, tip_oid, old_tip)
    }

    /// Index metadata keys from commit history using plain hex object ids.
    #[cfg(feature = "internal")]
    pub fn index_history_from_oid(
        &self,
        tip_oid: &str,
        old_tip: Option<&str>,
    ) -> crate::error::Result<usize> {
        let tip_oid = Self::parse_object_id(tip_oid)?;
        let old_tip = old_tip.map(Self::parse_object_id).transpose()?;
        crate::sync::insert_promisor_entries(&self.repo, &self.store, tip_oid, old_tip)
    }

    /// Serialize local metadata to Git tree(s) and commit(s).
    ///
    /// Determines incremental vs full mode automatically. Applies filter
    /// routing and pruning rules. Updates local refs and the materialization
    /// timestamp.
    pub fn serialize(&self) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run(self, self.now(), false)
    }

    /// Serialize local metadata and report progress through a callback.
    ///
    /// # Parameters
    ///
    /// - `progress`: callback invoked at major serialization steps.
    pub fn serialize_with_progress(
        &self,
        progress: impl FnMut(crate::serialize::SerializeProgress),
    ) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run_with_progress(self, self.now(), false, progress)
    }

    /// Serialize local metadata by rebuilding from the complete SQLite state.
    ///
    /// This bypasses incremental dirty-target detection while still avoiding a
    /// new commit when the rebuilt tree is identical to the current serialized
    /// ref. Applies filter routing and pruning rules. Updates local refs and
    /// the materialization timestamp when serialization succeeds.
    pub fn serialize_full(&self) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run(self, self.now(), true)
    }

    /// Serialize all local metadata and report progress through a callback.
    ///
    /// # Parameters
    ///
    /// - `progress`: callback invoked at major serialization steps.
    pub fn serialize_full_with_progress(
        &self,
        progress: impl FnMut(crate::serialize::SerializeProgress),
    ) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run_with_progress(self, self.now(), true, progress)
    }

    /// Materialize remote metadata into the local store.
    ///
    /// For each matching remote ref, determines the merge strategy and
    /// applies changes. Updates tracking refs and materialization timestamp.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name filter. If `None`, all remotes are
    ///   materialized.
    pub fn materialize(
        &self,
        remote: Option<&str>,
    ) -> crate::error::Result<crate::materialize::MaterializeOutput> {
        crate::materialize::run(self, remote, self.now())
    }

    /// Pull metadata from remote: fetch, materialize, and index history.
    ///
    /// Resolves the remote, fetches the metadata ref, hydrates tip blobs,
    /// serializes local state for merge, materializes remote changes, and
    /// indexes historical keys for lazy loading.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to pull from. If `None`, the first
    ///   configured metadata remote is used.
    pub fn pull(&self, remote: Option<&str>) -> crate::error::Result<crate::pull::PullOutput> {
        crate::pull::run(self, remote, self.now())
    }

    /// Serialize and attempt a single push to the remote.
    ///
    /// Returns the result of the push attempt. On non-fast-forward failure,
    /// the caller is responsible for calling [`resolve_push_conflict()`](Self::resolve_push_conflict)
    /// and retrying.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to push to. If `None`, the first
    ///   configured metadata remote is used.
    pub fn push_once(&self, remote: Option<&str>) -> crate::error::Result<crate::push::PushOutput> {
        crate::push::push_once(self, remote, self.now())
    }

    /// Serialize and attempt a single push to the remote, reporting progress.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name. If `None`, the first configured
    ///   metadata remote is used.
    /// - `progress`: callback invoked before long-running push phases.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, ref inspection, rebasing, or pushing
    /// fails.
    pub fn push_once_with_progress(
        &self,
        remote: Option<&str>,
        progress: impl FnMut(crate::push::PushProgress),
    ) -> crate::error::Result<crate::push::PushOutput> {
        crate::push::push_once_with_progress(self, remote, self.now(), progress)
    }

    /// After a failed push, fetch remote changes, materialize, re-serialize,
    /// and rebase local ref for clean fast-forward.
    ///
    /// Call this between push retries.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name. If `None`, the first configured
    ///   metadata remote is used.
    pub fn resolve_push_conflict(&self, remote: Option<&str>) -> crate::error::Result<()> {
        crate::push::resolve_push_conflict(self, remote, self.now())
    }

    /// Resolve a failed push and report progress.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name. If `None`, the first configured
    ///   metadata remote is used.
    /// - `progress`: callback invoked before long-running conflict resolution
    ///   phases.
    ///
    /// # Errors
    ///
    /// Returns an error if fetch, hydration, materialization, serialization, or
    /// rebase fails.
    pub fn resolve_push_conflict_with_progress(
        &self,
        remote: Option<&str>,
        progress: impl FnMut(crate::push::PushProgress),
    ) -> crate::error::Result<()> {
        crate::push::resolve_push_conflict_with_progress(self, remote, self.now(), progress)
    }

    #[cfg(feature = "internal")]
    fn parse_object_id(oid: &str) -> crate::error::Result<gix::ObjectId> {
        gix::ObjectId::from_hex(oid.as_bytes())
            .map_err(|err| crate::error::Error::Other(format!("invalid object id {oid}: {err}")))
    }

    #[cfg(feature = "internal")]
    fn other_error(err: impl std::fmt::Display) -> crate::error::Error {
        crate::error::Error::Other(err.to_string())
    }

    /// Run a git command in this session's repository and return stdout.
    #[cfg(feature = "internal")]
    pub fn run_git(&self, args: &[&str]) -> crate::error::Result<String> {
        crate::git_utils::run_git(&self.repo, args)
    }

    /// Repository git directory path.
    #[cfg(feature = "internal")]
    pub fn git_dir_path(&self) -> &Path {
        self.repo.path()
    }

    /// Repository worktree path, if this is a non-bare repository.
    #[cfg(feature = "internal")]
    pub fn workdir_path(&self) -> Option<&Path> {
        self.repo.workdir()
    }

    /// Read a string from git config.
    #[cfg(feature = "internal")]
    pub fn git_config_string(&self, key: &str) -> Option<String> {
        self.repo
            .config_snapshot()
            .string(key)
            .map(|value| value.to_string())
    }

    /// Read `core.pager` from git config.
    #[cfg(feature = "internal")]
    pub fn core_pager(&self) -> Option<String> {
        self.git_config_string("core.pager")
    }

    /// Write a blob and return its object id as a hex SHA string.
    #[cfg(feature = "internal")]
    pub fn write_blob_string(&self, value: &str) -> crate::error::Result<String> {
        let oid: gix::ObjectId = self
            .repo
            .write_blob(value.as_bytes())
            .map_err(Self::other_error)?
            .into();
        Ok(oid.to_string())
    }

    /// Read a blob object as UTF-8 text.
    #[cfg(feature = "internal")]
    pub fn read_blob_string(&self, oid: &str) -> crate::error::Result<String> {
        let oid = Self::parse_object_id(oid)?;
        let obj = oid.attach(&self.repo).object().map_err(Self::other_error)?;
        let blob = obj.into_blob();
        std::str::from_utf8(&blob.data)
            .map(str::to_string)
            .map_err(|err| {
                crate::error::Error::Other(format!("git blob {oid} is not UTF-8: {err}"))
            })
    }

    /// Resolve a ref or commit-ish to a peeled commit id.
    #[cfg(feature = "internal")]
    pub fn resolve_commitish(&self, spec: &str) -> crate::error::Result<String> {
        let obj = self.repo.rev_parse_single(spec).map_err(|err| {
            crate::error::Error::Other(format!("could not resolve ref '{spec}': {err}"))
        })?;
        let commit = obj
            .object()
            .map_err(Self::other_error)?
            .peel_tags_to_end()
            .map_err(Self::other_error)?
            .into_commit();
        Ok(commit.id().to_string())
    }

    /// Find metadata remote refs matching the namespace and optional remote filter.
    #[cfg(feature = "internal")]
    pub fn find_remote_refs(
        &self,
        remote: Option<&str>,
    ) -> crate::error::Result<Vec<(String, String)>> {
        Ok(
            crate::materialize::find_remote_refs(&self.repo, self.namespace(), remote)?
                .into_iter()
                .map(|(name, oid)| (name, oid.to_string()))
                .collect(),
        )
    }

    /// Find a reference and return its peeled object id if present.
    #[cfg(feature = "internal")]
    pub fn find_ref_oid(&self, ref_name: &str) -> crate::error::Result<Option<String>> {
        match self.repo.find_reference(ref_name) {
            Ok(reference) => Ok(Some(
                reference
                    .into_fully_peeled_id()
                    .map_err(Self::other_error)?
                    .detach()
                    .to_string(),
            )),
            Err(_) => Ok(None),
        }
    }

    /// List all refs with their peeled object ids.
    #[cfg(feature = "internal")]
    pub fn list_refs(&self) -> crate::error::Result<Vec<GitRefInfo>> {
        let mut refs = Vec::new();
        let platform = self.repo.references().map_err(Self::other_error)?;
        for reference in platform.all().map_err(Self::other_error)? {
            let reference = reference.map_err(|err| crate::error::Error::Other(err.to_string()))?;
            let name = reference.name().as_bstr().to_string();
            if let Ok(id) = reference.into_fully_peeled_id() {
                refs.push(GitRefInfo {
                    name,
                    oid: id.detach().to_string(),
                });
            }
        }
        Ok(refs)
    }

    /// Delete a reference if it exists.
    #[cfg(feature = "internal")]
    pub fn delete_ref(&self, ref_name: &str) -> crate::error::Result<bool> {
        match self.repo.find_reference(ref_name) {
            Ok(reference) => {
                reference
                    .delete()
                    .map_err(|err| crate::error::Error::Other(err.to_string()))?;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Create or update a reference from a hex object id.
    #[cfg(feature = "internal")]
    pub fn set_ref(&self, ref_name: &str, oid: &str, message: &str) -> crate::error::Result<()> {
        let oid = Self::parse_object_id(oid)?;
        self.repo
            .reference(ref_name, oid, PreviousValue::Any, message)
            .map_err(|err| crate::error::Error::Other(err.to_string()))?;
        Ok(())
    }

    /// Return commit information for an object id.
    #[cfg(feature = "internal")]
    pub fn commit_info(&self, oid: &str) -> crate::error::Result<GitCommitInfo> {
        let oid = Self::parse_object_id(oid)?;
        let commit = oid
            .attach(&self.repo)
            .object()
            .map_err(Self::other_error)?
            .into_commit();
        let tree_oid = commit
            .tree_id()
            .map_err(Self::other_error)?
            .detach()
            .to_string();
        let decoded = commit.decode().map_err(Self::other_error)?;
        let author = decoded
            .author()
            .map_err(|err| crate::error::Error::Other(err.to_string()))?;
        let author_time_seconds = author
            .time()
            .map_err(|err| crate::error::Error::Other(err.to_string()))?
            .seconds;
        let parent_count = decoded.parents().count();
        Ok(GitCommitInfo {
            oid: oid.to_string(),
            tree_oid,
            author_name: author.name.to_string(),
            author_email: author.email.to_string(),
            author_time_seconds,
            message: decoded.message.to_string(),
            parent_count,
        })
    }

    /// Return commit information if the object id names a readable commit.
    #[cfg(feature = "internal")]
    pub fn maybe_commit_info(&self, oid: &str) -> crate::error::Result<Option<GitCommitInfo>> {
        let Ok(oid) = Self::parse_object_id(oid) else {
            return Ok(None);
        };
        let Ok(object) = oid.attach(&self.repo).object() else {
            return Ok(None);
        };
        let commit = object.into_commit();
        let tree_oid = commit
            .tree_id()
            .map_err(Self::other_error)?
            .detach()
            .to_string();
        let decoded = commit.decode().map_err(Self::other_error)?;
        let author = decoded
            .author()
            .map_err(|err| crate::error::Error::Other(err.to_string()))?;
        let author_time_seconds = author
            .time()
            .map_err(|err| crate::error::Error::Other(err.to_string()))?
            .seconds;
        let parent_count = decoded.parents().count();
        Ok(Some(GitCommitInfo {
            oid: oid.to_string(),
            tree_oid,
            author_name: author.name.to_string(),
            author_email: author.email.to_string(),
            author_time_seconds,
            message: decoded.message.to_string(),
            parent_count,
        }))
    }

    /// Walk commits reachable from a starting object id.
    #[cfg(feature = "internal")]
    pub fn rev_walk_oids(&self, start_oid: &str) -> crate::error::Result<Vec<String>> {
        let start_oid = Self::parse_object_id(start_oid)?;
        let walk = self.repo.rev_walk(Some(start_oid));
        let iter = walk.all().map_err(Self::other_error)?;
        let mut oids = Vec::new();
        for info in iter {
            oids.push(
                info.map_err(|err| crate::error::Error::Other(err.to_string()))?
                    .id
                    .to_string(),
            );
        }
        Ok(oids)
    }

    /// Return merge-base object id for two commits, if one exists.
    #[cfg(feature = "internal")]
    pub fn merge_base_oid(&self, left: &str, right: &str) -> crate::error::Result<Option<String>> {
        let left = Self::parse_object_id(left)?;
        let right = Self::parse_object_id(right)?;
        Ok(self
            .repo
            .merge_base(left, right)
            .ok()
            .map(|oid| oid.to_string()))
    }

    /// Extract metadata keys from a tree object.
    #[cfg(feature = "internal")]
    pub fn extract_keys_from_tree(
        &self,
        tree_oid: &str,
    ) -> crate::error::Result<Vec<(String, String, String)>> {
        let tree_oid = Self::parse_object_id(tree_oid)?;
        crate::sync::extract_keys_from_tree(&self.repo, tree_oid)
    }

    /// Parse a metadata tree by object id.
    #[cfg(feature = "internal")]
    pub fn parse_metadata_tree(
        &self,
        tree_oid: &str,
    ) -> crate::error::Result<crate::tree::model::ParsedTree> {
        let tree_oid = Self::parse_object_id(tree_oid)?;
        crate::tree::format::parse_tree(&self.repo, tree_oid, "")
    }

    /// Return entries directly below a tree object.
    #[cfg(feature = "internal")]
    pub fn tree_entries(&self, tree_oid: &str) -> crate::error::Result<Vec<GitTreeEntryInfo>> {
        let tree_oid = Self::parse_object_id(tree_oid)?;
        let tree = tree_oid
            .attach(&self.repo)
            .object()
            .map_err(Self::other_error)?
            .into_tree();
        let mut entries = Vec::new();
        for entry in tree.iter() {
            let entry = entry.map_err(|err| crate::error::Error::Other(err.to_string()))?;
            let kind = if entry.mode().is_blob() {
                GitTreeEntryKind::Blob
            } else if entry.mode().is_tree() {
                GitTreeEntryKind::Tree
            } else {
                GitTreeEntryKind::Other
            };
            entries.push(GitTreeEntryInfo {
                name: entry.filename().to_string(),
                oid: entry.object_id().to_string(),
                kind,
            });
        }
        Ok(entries)
    }

    /// Find the object id for a path inside a tree.
    #[cfg(feature = "internal")]
    pub fn find_blob_oid_in_tree(
        &self,
        tree_oid: &str,
        path: &str,
    ) -> crate::error::Result<Option<String>> {
        let tree_oid = Self::parse_object_id(tree_oid)?;
        Ok(
            crate::git_utils::find_blob_oid_in_tree(&self.repo, tree_oid, path)?
                .map(|oid| oid.to_string()),
        )
    }

    /// Build a serialized tree from already-filtered records.
    #[cfg(feature = "internal")]
    pub fn build_filtered_tree_oid(
        &self,
        metadata: &[crate::db::types::SerializableEntry],
        tombstones: &[crate::db::types::TombstoneRecord],
        set_tombstones: &[crate::db::types::SetTombstoneRecord],
        list_tombstones: &[crate::db::types::ListTombstoneRecord],
    ) -> crate::error::Result<String> {
        Ok(crate::serialize::build_filtered_tree(
            &self.repo,
            metadata,
            tombstones,
            set_tombstones,
            list_tombstones,
        )?
        .to_string())
    }

    /// Count dropped and retained keys between two tree ids.
    #[cfg(feature = "internal")]
    pub fn count_prune_stats_for_trees(
        &self,
        original_oid: &str,
        pruned_oid: &str,
    ) -> crate::error::Result<(u64, u64)> {
        let original_oid = Self::parse_object_id(original_oid)?;
        let pruned_oid = Self::parse_object_id(pruned_oid)?;
        crate::serialize::count_prune_stats(&self.repo, original_oid, pruned_oid)
    }

    /// Create a one-file tree containing `README.md`, commit it, update a ref, and return the commit id.
    #[cfg(feature = "internal")]
    pub fn commit_readme_to_ref(
        &self,
        ref_name: &str,
        readme_content: &str,
        message: &str,
        parent_oid: Option<&str>,
        reflog_message: &str,
        must_not_exist: bool,
    ) -> crate::error::Result<String> {
        let blob_oid: gix::ObjectId = self
            .repo
            .write_blob(readme_content.as_bytes())
            .map_err(Self::other_error)?
            .into();
        let tree_oid = {
            let mut editor = self.repo.empty_tree().edit().map_err(Self::other_error)?;
            editor
                .upsert("README.md", gix::objs::tree::EntryKind::Blob, blob_oid)
                .map_err(Self::other_error)?;
            editor.write().map_err(Self::other_error)?
        };
        let parents = parent_oid
            .map(Self::parse_object_id)
            .transpose()?
            .into_iter()
            .collect::<Vec<_>>();
        let sig = gix::actor::Signature {
            name: self.name().into(),
            email: self.email().into(),
            time: gix::date::Time::now_local_or_utc(),
        };
        let commit = gix::objs::Commit {
            message: message.into(),
            tree: tree_oid.into(),
            author: sig.clone(),
            committer: sig,
            encoding: None,
            parents: parents.into(),
            extra_headers: Default::default(),
        };
        let commit_oid = self
            .repo
            .write_object(&commit)
            .map_err(Self::other_error)?
            .detach();
        let previous = if must_not_exist {
            PreviousValue::MustNotExist
        } else {
            PreviousValue::Any
        };
        self.repo
            .reference(ref_name, commit_oid, previous, reflog_message)
            .map_err(Self::other_error)?;
        Ok(commit_oid.to_string())
    }

    /// Create a commit for a tree, update a ref, and return the commit id.
    #[cfg(feature = "internal")]
    pub fn commit_tree_to_ref(
        &self,
        ref_name: &str,
        tree_oid: &str,
        parent_oid: Option<&str>,
        message: &str,
        reflog_message: &str,
    ) -> crate::error::Result<String> {
        let tree_oid = Self::parse_object_id(tree_oid)?;
        let parents = parent_oid
            .map(Self::parse_object_id)
            .transpose()?
            .into_iter()
            .collect::<Vec<_>>();
        let sig = gix::actor::Signature {
            name: self.name().into(),
            email: self.email().into(),
            time: gix::date::Time::now_local_or_utc(),
        };
        let commit = gix::objs::Commit {
            message: message.into(),
            tree: tree_oid,
            author: sig.clone(),
            committer: sig,
            encoding: None,
            parents: parents.into(),
            extra_headers: Default::default(),
        };
        let commit_oid = self
            .repo
            .write_object(&commit)
            .map_err(Self::other_error)?
            .detach();
        self.repo
            .reference(ref_name, commit_oid, PreviousValue::Any, reflog_message)
            .map_err(Self::other_error)?;
        Ok(commit_oid.to_string())
    }

    /// List configured metadata remotes.
    #[cfg(feature = "internal")]
    pub fn list_meta_remotes(&self) -> crate::error::Result<Vec<(String, String)>> {
        crate::git_utils::list_meta_remotes(&self.repo)
    }

    /// Hydrate blobs reachable from a remote metadata tip and return the count fetched.
    #[cfg(feature = "internal")]
    pub fn hydrate_tip_blobs_counted(
        &self,
        remote_name: &str,
        ref_name: &str,
    ) -> crate::error::Result<usize> {
        crate::git_utils::hydrate_tip_blobs_counted(&self.repo, remote_name, ref_name)
    }

    /// Fetch specific blob ids from a metadata remote.
    #[cfg(feature = "internal")]
    pub fn fetch_blob_oids(&self, remote_name: &str, oids: &[String]) -> crate::error::Result<()> {
        let parsed = oids
            .iter()
            .map(|oid| Self::parse_object_id(oid))
            .collect::<crate::error::Result<Vec<_>>>()?;
        crate::git_utils::fetch_blob_oids(&self.repo, remote_name, &parsed)
    }

    /// Read a named blob entry from a tree.
    #[cfg(feature = "internal")]
    pub fn tree_blob_string(
        &self,
        tree_oid: &str,
        name: &str,
    ) -> crate::error::Result<Option<String>> {
        for entry in self.tree_entries(tree_oid)? {
            if entry.name == name && entry.kind == GitTreeEntryKind::Blob {
                return self.read_blob_string(&entry.oid).map(Some);
            }
        }
        Ok(None)
    }

    /// Return a named subtree entry object id from a tree.
    #[cfg(feature = "internal")]
    pub fn tree_subtree_oid(
        &self,
        tree_oid: &str,
        name: &str,
    ) -> crate::error::Result<Option<String>> {
        Ok(self
            .tree_entries(tree_oid)?
            .into_iter()
            .find(|entry| entry.name == name && entry.kind == GitTreeEntryKind::Tree)
            .map(|entry| entry.oid))
    }
}
