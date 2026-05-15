use crate::error::{Error, Result};
use crate::session::Session;
use crate::types::{MetaValue, Target, ValueType};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

/// A scoped handle for operations on a specific target within a session.
///
/// Created via [`Session::target()`]. Carries the target, email, and
/// timestamp from the session so callers never have to pass them.
///
/// # Example
///
/// ```ignore
/// let session = Session::discover()?;
/// let handle = session.target(&Target::parse("commit:abc123")?);
/// handle.set("agent:model", "claude")?;
/// let val = handle.get_value("agent:model")?;
/// ```
#[derive(Debug)]
pub struct SessionTargetHandle<'a> {
    session: &'a Session,
    target: &'a Target,
}

impl<'a> SessionTargetHandle<'a> {
    pub(crate) fn new(session: &'a Session, target: &'a Target) -> Self {
        Self { session, target }
    }

    /// Get a metadata value by key.
    pub fn get_value(&self, key: &str) -> Result<Option<MetaValue>> {
        self.session.store.get_value(self.target, key)
    }

    /// Set a metadata value with convenience conversion.
    ///
    /// Accepts anything that converts to [`MetaValue`]: `&str`, `String`,
    /// `Vec<ListEntry>`, `BTreeSet<String>`, or `MetaValue` directly.
    ///
    /// ```ignore
    /// handle.set("key", "hello")?;                    // string
    /// handle.set("key", MetaValue::String("hello".into()))?; // explicit
    /// ```
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set(&self, key: &str, value: impl Into<MetaValue>) -> Result<()> {
        let meta_value = value.into();
        self.session.store.set_value(
            self.target,
            key,
            &meta_value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Merge string metadata fields under a common key prefix.
    ///
    /// The record must serialize to a JSON object. Object field names, including
    /// `serde` renames, become key suffixes. String values are written as
    /// `prefix:field`.
    ///
    /// This is a partial update, not a replacement operation. Null fields are
    /// skipped and existing keys are left untouched. This makes `Option<T>`
    /// fields useful for patch-style records, but callers that need to clear a
    /// field must remove that key explicitly.
    ///
    /// ```ignore
    /// #[derive(serde::Serialize)]
    /// #[serde(rename_all = "kebab-case")]
    /// struct Source<'a> {
    ///     agent: &'a str,
    ///     tool_version: Option<&'a str>,
    /// }
    ///
    /// handle.set_record("agent-session:abc:source:def", &Source {
    ///     agent: "codex",
    ///     tool_version: Some("1.2.3"),
    /// })?;
    ///
    /// // Later updates that serialize `tool_version` as null do not remove the
    /// // existing `agent-session:abc:source:def:tool-version` key.
    /// handle.set_record("agent-session:abc:source:def", &Source {
    ///     agent: "codex",
    ///     tool_version: None,
    /// })?;
    /// ```
    pub fn set_record(&self, prefix: &str, record: impl Serialize) -> Result<()> {
        let Value::Object(fields) = serde_json::to_value(record)? else {
            return Err(Error::InvalidValue(
                "record metadata must serialize to a JSON object".to_string(),
            ));
        };

        for (field, value) in fields {
            match value {
                Value::Null => {}
                Value::String(value) => self.set(&format!("{prefix}:{field}"), value)?,
                _ => {
                    return Err(Error::InvalidValue(format!(
                        "record metadata field '{field}' must serialize to a string or null"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Read string metadata fields under a common key prefix into a record.
    ///
    /// This is the read-side pair to [`set_record`](Self::set_record). Immediate
    /// child keys like `prefix:field` become JSON object fields before being
    /// deserialized into `T`. Missing records return `Ok(None)`. Nested keys such
    /// as `prefix:child:field` are ignored because they belong to a deeper
    /// metadata subtree, not this record.
    ///
    /// Because [`set_record`](Self::set_record) leaves null or omitted fields
    /// untouched, `get_record` reads the current merged field set under the
    /// prefix, not the exact last value passed to `set_record`.
    ///
    /// # Errors
    ///
    /// Returns an error if an immediate child field exists but is not a string,
    /// or if the collected fields do not deserialize into `T`.
    pub fn get_record<T>(&self, prefix: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let field_prefix = format!("{prefix}:");
        let mut fields = Map::new();

        for (key, value) in self.get_all_values(Some(prefix))? {
            let Some(field) = key.strip_prefix(&field_prefix) else {
                continue;
            };
            if field.contains(':') {
                continue;
            }

            match value {
                MetaValue::String(value) => {
                    fields.insert(field.to_string(), Value::String(value));
                }
                _ => {
                    return Err(Error::InvalidValue(format!(
                        "record metadata field '{field}' must be a string"
                    )));
                }
            }
        }

        if fields.is_empty() {
            return Ok(None);
        }

        serde_json::from_value(Value::Object(fields))
            .map(Some)
            .map_err(Into::into)
    }

    /// Remove a metadata key.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn remove(&self, key: &str) -> Result<bool> {
        self.session
            .store
            .remove(self.target, key, self.session.email(), self.session.now())
    }

    /// Push a value onto a list.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_push(&self, key: &str, value: &str) -> Result<()> {
        self.session.store.list_push(
            self.target,
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Apply list/set edits in one transaction.
    ///
    /// Empty edit batches are no-ops. If any edit fails, none of the batch is
    /// committed. The session email and timestamp are used for every edit.
    pub fn apply_edits<'b>(
        &self,
        edits: impl IntoIterator<Item = crate::MetaEdit<'b>>,
    ) -> Result<()> {
        self.session
            .store
            .apply_edits(self.target, edits, self.session.email(), self.session.now())
    }

    /// Pop a value from a list.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_pop(&self, key: &str, value: &str) -> Result<()> {
        self.session.store.list_pop(
            self.target,
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Remove a list entry by index.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_remove(&self, key: &str, index: usize) -> Result<()> {
        self.session.store.list_remove(
            self.target,
            key,
            index,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Add a member to a set.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set_add(&self, key: &str, value: &str) -> Result<()> {
        self.session.store.set_add(
            self.target,
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Remove a member from a set.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set_remove(&self, key: &str, value: &str) -> Result<()> {
        self.session.store.set_remove(
            self.target,
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// The target this handle is scoped to.
    pub fn target(&self) -> &Target {
        self.target
    }

    /// Get all metadata for this target as typed (key, value) pairs.
    ///
    /// Optionally filters by key prefix (e.g., `Some("agent")` returns
    /// all keys starting with `agent` or `agent:`).
    ///
    /// # Parameters
    ///
    /// - `prefix`: optional key prefix to filter by
    ///
    /// # Returns
    ///
    /// A vector of `(key, MetaValue)` pairs for matching metadata entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or deserialization fails.
    pub fn get_all_values(&self, prefix: Option<&str>) -> Result<Vec<(String, MetaValue)>> {
        let entries = self.session.store.get_all(self.target, prefix)?;
        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            let meta_value = match entry.value_type {
                ValueType::String => {
                    let s: String =
                        serde_json::from_str(&entry.value).unwrap_or_else(|_| entry.value.clone());
                    MetaValue::String(s)
                }
                ValueType::List => {
                    let entries = crate::list_value::parse_entries(&entry.value)?;
                    MetaValue::List(entries)
                }
                ValueType::Set => {
                    let members: Vec<String> = serde_json::from_str(&entry.value)?;
                    MetaValue::Set(members.into_iter().collect())
                }
            };
            result.push((entry.key, meta_value));
        }
        Ok(result)
    }

    /// Get list entries for a key on this target.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key name
    ///
    /// # Returns
    ///
    /// A vector of [`ListEntry`](crate::list_value::ListEntry) values with
    /// resolved content and timestamps.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is missing, the value is not a list, or
    /// the database read fails.
    pub fn list_entries(&self, key: &str) -> Result<Vec<crate::list_value::ListEntry>> {
        self.session.store.list_entries(self.target, key)
    }

    /// Get authorship info (last author email and timestamp) for a key on this target.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key name
    ///
    /// # Returns
    ///
    /// `Some(Authorship)` if the key has been modified at least once,
    /// `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn get_authorship(&self, key: &str) -> Result<Option<crate::db::types::Authorship>> {
        self.session.store.get_authorship(self.target, key)
    }
}
