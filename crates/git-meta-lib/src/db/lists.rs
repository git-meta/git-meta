use rusqlite::{params, OptionalExtension};

use crate::error::{Error, Result};

use super::{
    load_list_entries_by_metadata_id, load_list_rows_by_metadata_id, resolve_blob,
    types::Operation, Store, COLLECTION_LOG_VALUE,
};
use crate::list_value::ListEntry;
use crate::types::{validate_key, Target};

impl Store {
    /// Push a value onto a list. If the key is currently a string, convert to list first.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the value to push
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_push(
        &self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let sp = self.savepoint()?;
        let entry = ListEntry {
            value: value.to_string(),
            timestamp,
        };
        self.append_list_entries(target, key, std::slice::from_ref(&entry), email, timestamp)?;
        sp.commit()?;

        Ok(())
    }

    /// Apply list/set edits in one transaction.
    ///
    /// Empty edit batches are no-ops. If any edit fails, none of the batch is
    /// committed. List entry timestamps are adjusted only when needed to keep
    /// appended entries ordered after existing entries.
    pub fn apply_edits<'a>(
        &self,
        target: &Target,
        edits: impl IntoIterator<Item = crate::MetaEdit<'a>>,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let edits = edits.into_iter().collect::<Vec<_>>();
        for edit in &edits {
            let key = match edit {
                crate::MetaEdit::ListAppend { key, .. } | crate::MetaEdit::SetAdd { key, .. } => {
                    key
                }
            };
            validate_key(key)?;
        }

        let sp = self.savepoint()?;

        for edit in edits {
            match edit {
                crate::MetaEdit::ListAppend { key, entries } => {
                    self.append_list_entries(target, key, entries, email, timestamp)?;
                }
                crate::MetaEdit::SetAdd { key, members } => {
                    self.add_set_members(target, key, members, email, timestamp)?;
                }
            }
        }

        sp.commit()?;
        Ok(())
    }

    fn append_list_entries(
        &self,
        target: &Target,
        key: &str,
        entries: &[ListEntry],
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        if entries.is_empty() {
            return Ok(());
        }

        let target_type = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let (metadata_id, mut last_timestamp) =
            self.ensure_list_metadata_for_append(target_type, target_value, key, timestamp)?;

        for entry in entries {
            let entry_timestamp = if entry.timestamp <= last_timestamp {
                last_timestamp + 1
            } else {
                entry.timestamp
            };
            last_timestamp = entry_timestamp;

            self.conn.execute(
                "INSERT INTO list_values (metadata_id, value, timestamp, is_git_ref)
                 VALUES (?1, ?2, ?3, ?4)",
                params![metadata_id, &entry.value, entry_timestamp, 0],
            )?;
        }

        self.conn.execute(
            "UPDATE metadata
             SET value = '[]', value_type = 'list', last_timestamp = ?1
             WHERE rowid = ?2",
            params![timestamp, metadata_id],
        )?;
        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', ?5, ?6, ?7)",
            params![
                target_type,
                target_value,
                key,
                COLLECTION_LOG_VALUE,
                Operation::Push.as_str(),
                email,
                timestamp
            ],
        )?;
        self.delete_metadata_tombstone(target_type, target_value, key)?;

        Ok(())
    }

    fn ensure_list_metadata_for_append(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        timestamp: i64,
    ) -> Result<(i64, i64)> {
        let existing = {
            let mut stmt = self.conn.prepare(
                "SELECT rowid, value, value_type, is_git_ref FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;
            stmt.query_row(params![target_type, target_value, key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, _, current_type, _)) if current_type == "list" => {
                let last_timestamp: Option<i64> = self.conn.query_row(
                    "SELECT MAX(timestamp) FROM list_values WHERE metadata_id = ?1",
                    [metadata_id],
                    |row| row.get(0),
                )?;
                Ok((metadata_id, last_timestamp.unwrap_or(i64::MIN)))
            }
            Some((metadata_id, current_val, current_type, is_git_ref))
                if current_type == "string" =>
            {
                let current_str = if is_git_ref {
                    resolve_blob(self.repo.as_ref(), &current_val, true)?
                } else {
                    serde_json::from_str(&current_val)?
                };
                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1, is_git_ref = 0
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;
                self.conn.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                self.conn.execute(
                    "INSERT INTO list_values (metadata_id, value, timestamp)
                     VALUES (?1, ?2, 0)",
                    params![metadata_id, current_str],
                )?;
                Ok((metadata_id, 0))
            }
            Some(_) => Err(Error::TypeMismatch {
                key: key.to_string(),
                expected: "list".into(),
            }),
            None => {
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4)",
                    params![target_type, target_value, key, timestamp],
                )?;
                Ok((self.conn.last_insert_rowid(), i64::MIN))
            }
        }
    }

    /// Pop a value from a list.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the value to pop (removed by matching)
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_pop(
        &self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let sp = self.savepoint()?;
        let existing = {
            let mut stmt = self.conn.prepare(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type_str, target_value, key], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                let mut list_rows = load_list_rows_by_metadata_id(&self.conn, metadata_id)?;
                if let Some(pos) = list_rows.iter().rposition(|row| row.value == value) {
                    let removed = list_rows.remove(pos);
                    self.conn.execute(
                        "DELETE FROM list_values WHERE rowid = ?1",
                        params![removed.rowid],
                    )?;
                } else {
                    return Err(Error::ValueNotFound(format!("'{value}' not found in list")));
                }

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'pop', ?5, ?6)",
                    params![
                        target_type_str,
                        target_value,
                        key,
                        COLLECTION_LOG_VALUE,
                        email,
                        timestamp
                    ],
                )?;

                self.conn.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                sp.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }

    /// Get list entries for display (resolved values with timestamps).
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    pub fn list_entries(&self, target: &Target, key: &str) -> Result<Vec<ListEntry>> {
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let metadata_id = self
            .conn
            .query_row(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type_str, target_value, key],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;

        match metadata_id {
            Some((id, vtype)) => {
                if vtype != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                load_list_entries_by_metadata_id(&self.conn, self.repo.as_ref(), id)
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }

    /// Remove a list entry by index, creating a list tombstone for serialization.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `index`: the zero-based index of the entry to remove
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_remove(
        &self,
        target: &Target,
        key: &str,
        index: usize,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let sp = self.savepoint()?;
        let existing = {
            let mut stmt = self.conn.prepare(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type_str, target_value, key], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                let mut list_rows = load_list_rows_by_metadata_id(&self.conn, metadata_id)?;
                if index >= list_rows.len() {
                    return Err(Error::IndexOutOfRange {
                        index,
                        size: list_rows.len(),
                    });
                }

                let removed = list_rows.remove(index);

                // Build the entry name used in git tree serialization
                let entry_name = crate::list_value::make_entry_name_from_parts(
                    removed.timestamp,
                    &removed.value,
                );

                self.conn.execute(
                    "DELETE FROM list_values WHERE rowid = ?1",
                    params![removed.rowid],
                )?;

                // Record a list tombstone so serialize propagates the deletion
                self.conn.execute(
                    "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                     VALUES ('list_entry', ?1, ?2, ?3, ?4, '', ?5, ?6)
                     ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                     SET timestamp = excluded.timestamp, email = excluded.email",
                    params![target_type_str, target_value, key, entry_name, timestamp, email],
                )?;

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', ?5, ?6, ?7)",
                    params![
                        target_type_str,
                        target_value,
                        key,
                        COLLECTION_LOG_VALUE,
                        Operation::ListRemove.as_str(),
                        email,
                        timestamp
                    ],
                )?;

                self.conn.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                sp.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }
}
