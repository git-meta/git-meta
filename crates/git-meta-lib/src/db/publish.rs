use std::collections::BTreeSet;

use rusqlite::{params, OptionalExtension};

use crate::{
    error::{Error, Result},
    tree::filter::LOCAL_PREFIX,
    types::{set_member_id, validate_key, LocalPublish, Target, ValueType},
};

use super::{types::Operation, Store, COLLECTION_LOG_VALUE};

#[derive(Debug)]
struct LocalPublishRow {
    rowid: i64,
    local_key: String,
    published_key: String,
    value: String,
    value_type: ValueType,
}

impl Store {
    /// Publish local-only metadata in one transaction.
    ///
    /// Prefix publishes convert hydrated `local:` metadata rows into
    /// non-local keys in-place, preserving list/set backing rows. Set-member
    /// publishes add requested members to the published set and remove them
    /// from the local set in the same savepoint.
    pub(crate) fn publish_local<'a>(
        &self,
        target: &Target,
        entries: impl IntoIterator<Item = LocalPublish<'a>>,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        for entry in &entries {
            match entry {
                LocalPublish::KeyPrefix {
                    local_prefix,
                    published_prefix,
                } => {
                    validate_local_publish_pair(local_prefix, published_prefix)?;
                }
                LocalPublish::SetMembers {
                    local_key,
                    published_key,
                    members: _,
                } => {
                    validate_local_publish_pair(local_key, published_key)?;
                }
            }
        }

        let sp = self.savepoint()?;
        for entry in entries {
            match entry {
                LocalPublish::KeyPrefix {
                    local_prefix,
                    published_prefix,
                } => {
                    self.publish_key_prefix_tx(
                        target,
                        local_prefix,
                        published_prefix,
                        email,
                        timestamp,
                    )?;
                }
                LocalPublish::SetMembers {
                    local_key,
                    published_key,
                    members,
                } => {
                    self.publish_set_members_tx(
                        target,
                        local_key,
                        published_key,
                        members,
                        email,
                        timestamp,
                    )?;
                }
            }
        }
        sp.commit()?;
        Ok(())
    }

    fn publish_key_prefix_tx(
        &self,
        target: &Target,
        local_prefix: &str,
        published_prefix: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let rows =
            self.local_publish_rows(target_type, target_value, local_prefix, published_prefix)?;
        if rows.is_empty() {
            return Ok(());
        }

        for row in &rows {
            let existing = self
                .conn
                .query_row(
                    "SELECT rowid, is_promised FROM metadata
                     WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type, target_value, row.published_key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?)),
                )
                .optional()?;
            match existing {
                Some((_rowid, true)) => {
                    return Err(Error::InvalidValue(format!(
                        "published key '{}' is promised and must be materialized before publishing",
                        row.published_key
                    )));
                }
                Some((rowid, false)) if rowid != row.rowid => {
                    return Err(Error::InvalidValue(format!(
                        "published key '{}' already exists",
                        row.published_key
                    )));
                }
                _ => {}
            }
        }

        for row in &rows {
            self.conn.execute(
                "UPDATE metadata
                 SET key = ?1, last_timestamp = ?2
                 WHERE rowid = ?3",
                params![row.published_key, timestamp, row.rowid],
            )?;
            self.delete_tombstones_for_key(target_type, target_value, &row.published_key)?;
            self.conn.execute(
                "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                 VALUES ('metadata', ?1, ?2, ?3, '', '', ?4, ?5)
                 ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                 SET timestamp = excluded.timestamp, email = excluded.email",
                params![target_type, target_value, row.local_key, timestamp, email],
            )?;
            self.conn.execute(
                "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                 VALUES (?1, ?2, ?3, '', '', ?4, ?5, ?6)",
                params![
                    target_type,
                    target_value,
                    row.local_key,
                    Operation::Remove.as_str(),
                    email,
                    timestamp
                ],
            )?;
            let log_value = match row.value_type {
                ValueType::String => row.value.as_str(),
                ValueType::List | ValueType::Set => COLLECTION_LOG_VALUE,
            };
            self.conn.execute(
                "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    target_type,
                    target_value,
                    row.published_key,
                    log_value,
                    row.value_type.as_str(),
                    Operation::Set.as_str(),
                    email,
                    timestamp
                ],
            )?;
        }

        Ok(())
    }

    fn local_publish_rows(
        &self,
        target_type: &str,
        target_value: &str,
        local_prefix: &str,
        published_prefix: &str,
    ) -> Result<Vec<LocalPublishRow>> {
        let child_lower = format!("{local_prefix}:");
        let child_upper = format!("{local_prefix};");
        let mut stmt = self.conn.prepare(
            "SELECT rowid, key, value, value_type, is_promised
             FROM metadata
             WHERE target_type = ?1 AND target_value = ?2
             AND (key = ?3 OR (key >= ?4 AND key < ?5))
             ORDER BY key",
        )?;
        let rows = stmt.query_map(
            params![
                target_type,
                target_value,
                local_prefix,
                child_lower,
                child_upper
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            },
        )?;

        let mut publish_rows = Vec::new();
        for row in rows {
            let (rowid, local_key, value, value_type, is_promised) = row?;
            if is_promised {
                return Err(Error::InvalidValue(format!(
                    "local key '{local_key}' is promised and must be materialized before publishing"
                )));
            }
            let suffix = local_key.strip_prefix(local_prefix).ok_or_else(|| {
                Error::InvalidKey(format!("key '{local_key}' is outside local publish prefix"))
            })?;
            let published_key = format!("{published_prefix}{suffix}");
            publish_rows.push(LocalPublishRow {
                rowid,
                local_key,
                published_key,
                value,
                value_type: value_type.parse()?,
            });
        }
        Ok(publish_rows)
    }

    fn publish_set_members_tx(
        &self,
        target: &Target,
        local_key: &str,
        published_key: &str,
        members: &[String],
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let unique_members = unique_members(members);
        let local_metadata_id = self.local_set_metadata_id(target, local_key)?;
        self.ensure_published_set_can_receive(target, published_key)?;
        for member in &unique_members {
            let member_id = set_member_id(member);
            let exists = self.conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM set_values
                    WHERE metadata_id = ?1 AND member_id = ?2
                )",
                params![local_metadata_id, member_id],
                |row| row.get::<_, bool>(0),
            )?;
            if !exists {
                return Err(Error::ValueNotFound(format!("'{member}' not found in set")));
            }
        }

        self.add_set_members(target, published_key, &unique_members, email, timestamp)?;
        for member in &unique_members {
            self.set_remove(target, local_key, member, email, timestamp)?;
        }
        Ok(())
    }

    fn local_set_metadata_id(&self, target: &Target, local_key: &str) -> Result<i64> {
        let target_type = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let existing = self
            .conn
            .query_row(
                "SELECT rowid, value_type, is_promised FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type, target_value, local_key],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                },
            )
            .optional()?;
        match existing {
            Some((_metadata_id, _value_type, true)) => Err(Error::InvalidValue(format!(
                "local set '{local_key}' is promised and must be materialized before publishing"
            ))),
            Some((metadata_id, value_type, false)) => match value_type.parse()? {
                ValueType::Set => Ok(metadata_id),
                ValueType::String | ValueType::List => Err(Error::TypeMismatch {
                    key: local_key.to_string(),
                    expected: "set".into(),
                }),
            },
            None => Err(Error::KeyNotFound {
                key: local_key.to_string(),
            }),
        }
    }

    fn ensure_published_set_can_receive(&self, target: &Target, published_key: &str) -> Result<()> {
        let target_type = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let existing = self
            .conn
            .query_row(
                "SELECT value_type, is_promised FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type, target_value, published_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
            )
            .optional()?;
        match existing {
            Some((_value_type, true)) => Err(Error::InvalidValue(format!(
                "published set '{published_key}' is promised and must be materialized before publishing"
            ))),
            Some((value_type, false)) => match value_type.parse()? {
                ValueType::Set => Ok(()),
                ValueType::String | ValueType::List => Err(Error::TypeMismatch {
                    key: published_key.to_string(),
                    expected: "set".into(),
                }),
            },
            None => Ok(()),
        }
    }

    fn delete_tombstones_for_key(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM tombstones
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type, target_value, key],
        )?;
        Ok(())
    }
}

fn validate_local_publish_pair(local_key: &str, published_key: &str) -> Result<()> {
    validate_key(local_key)?;
    validate_key(published_key)?;
    if local_key == published_key {
        return Err(Error::InvalidKey(
            "local and published keys must differ".into(),
        ));
    }
    if !local_key.starts_with(LOCAL_PREFIX) {
        return Err(Error::InvalidKey(
            "local publish source must start with 'local:'".into(),
        ));
    }
    if published_key.starts_with(LOCAL_PREFIX) {
        return Err(Error::InvalidKey(
            "local publish destination must not start with 'local:'".into(),
        ));
    }
    Ok(())
}

fn unique_members(members: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();
    for member in members {
        if seen.insert(member.as_str()) {
            unique.push(member.clone());
        }
    }
    unique
}
