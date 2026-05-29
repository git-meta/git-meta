//! Resolution of the object-size threshold that decides when a string value
//! is offloaded to a Git blob instead of being stored inline in SQLite.

use crate::error::{Error, Result};
use crate::prune::parse_size;
use crate::types::{Target, GIT_REF_THRESHOLD};

use super::Store;

/// Shared project config key (serialized to the metadata remote) that overrides
/// the maximum inline object size before string values are stored as Git blobs.
pub(crate) const OBJECT_MAX_SIZE_KEY: &str = "meta:sqlite:object-max-size";

/// Local-only override (never serialized) for the maximum inline object size.
/// Takes precedence over [`OBJECT_MAX_SIZE_KEY`].
pub(crate) const LOCAL_OBJECT_MAX_SIZE_KEY: &str = "meta:local:sqlite:object-max-size";

impl Store {
    /// Resolve the maximum size, in bytes, a string value may occupy inline in
    /// SQLite before it is offloaded to a Git blob and referenced by its OID.
    ///
    /// Resolution order (first match wins):
    /// 1. `meta:local:sqlite:object-max-size` — local-only override
    /// 2. `meta:sqlite:object-max-size` — shared project config
    /// 3. [`GIT_REF_THRESHOLD`] — built-in default
    ///
    /// Values accept human-friendly sizes (e.g. `4k`, `1m`) or a plain byte
    /// count. A configured value of `0` offloads every non-empty string value.
    ///
    /// # Errors
    ///
    /// Returns an error if a configured value cannot be parsed as a size.
    pub fn object_max_size(&self) -> Result<usize> {
        for key in [LOCAL_OBJECT_MAX_SIZE_KEY, OBJECT_MAX_SIZE_KEY] {
            let Some(raw) = self.read_config_string(key)? else {
                continue;
            };
            let bytes = parse_size(&raw)
                .map_err(|_| Error::InvalidValue(format!("invalid {key} value: {raw}")))?;
            return Ok(usize::try_from(bytes).unwrap_or(usize::MAX));
        }
        Ok(GIT_REF_THRESHOLD)
    }

    /// Read a project-scoped string config value, returning the decoded string.
    fn read_config_string(&self, key: &str) -> Result<Option<String>> {
        match self.get(&Target::project(), key)? {
            Some(entry) => Ok(Some(serde_json::from_str(&entry.value)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::ValueType;

    fn set_config(db: &Store, key: &str, value: &str) {
        let json = serde_json::to_string(value).unwrap();
        db.set(
            &Target::project(),
            key,
            &json,
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
    }

    #[test]
    fn defaults_to_git_ref_threshold() {
        let db = Store::open_in_memory().unwrap();
        assert_eq!(db.object_max_size().unwrap(), GIT_REF_THRESHOLD);
    }

    #[test]
    fn shared_key_overrides_default() {
        let db = Store::open_in_memory().unwrap();
        set_config(&db, OBJECT_MAX_SIZE_KEY, "4k");
        assert_eq!(db.object_max_size().unwrap(), 4096);
    }

    #[test]
    fn local_key_takes_precedence_over_shared() {
        let db = Store::open_in_memory().unwrap();
        set_config(&db, OBJECT_MAX_SIZE_KEY, "4k");
        set_config(&db, LOCAL_OBJECT_MAX_SIZE_KEY, "16");
        assert_eq!(db.object_max_size().unwrap(), 16);
    }

    #[test]
    fn plain_byte_count_is_accepted() {
        let db = Store::open_in_memory().unwrap();
        set_config(&db, OBJECT_MAX_SIZE_KEY, "2048");
        assert_eq!(db.object_max_size().unwrap(), 2048);
    }

    #[test]
    fn invalid_value_errors() {
        let db = Store::open_in_memory().unwrap();
        set_config(&db, OBJECT_MAX_SIZE_KEY, "not-a-size");
        assert!(db.object_max_size().is_err());
    }
}
