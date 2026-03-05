use anyhow::{bail, Result};
use rusqlite::{params, Connection};
use std::path::Path;

use crate::list_value::{encode_entries, ensure_unique_timestamp, parse_entries, ListEntry};

pub struct Db {
    pub conn: Connection,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Db { conn };
        db.init_schema()?;
        Ok(db)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Db { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                value_type TEXT NOT NULL,
                last_timestamp INTEGER NOT NULL DEFAULT 0,
                UNIQUE(target_type, target_value, key)
            );

            CREATE TABLE IF NOT EXISTS list_values (
                metadata_id INTEGER NOT NULL,
                value TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_list_values_metadata_ts
                ON list_values(metadata_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_list_values_metadata_value_ts
                ON list_values(metadata_id, value, timestamp);

            CREATE TABLE IF NOT EXISTS metadata_log (
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                value_type TEXT NOT NULL,
                operation TEXT NOT NULL,
                email TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metadata_tombstones (
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                key TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                email TEXT NOT NULL,
                UNIQUE(target_type, target_value, key)
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_materialized INTEGER
            );

            INSERT OR IGNORE INTO sync_state (id, last_materialized) VALUES (1, NULL);",
        )?;
        Ok(())
    }

    /// Set a value (upsert). JSON-encodes the value for storage.
    pub fn set(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        value_type: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        match value_type {
            "string" => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'string', ?5)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = excluded.value, value_type = 'string', last_timestamp = excluded.last_timestamp",
                    params![target_type, target_value, key, value, timestamp],
                )?;

                let metadata_id: i64 = tx.query_row(
                    "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type, target_value, key],
                    |row| row.get(0),
                )?;
                tx.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
            }
            "list" => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = '[]', value_type = 'list', last_timestamp = excluded.last_timestamp",
                    params![target_type, target_value, key, timestamp],
                )?;

                let metadata_id: i64 = tx.query_row(
                    "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type, target_value, key],
                    |row| row.get(0),
                )?;

                tx.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;

                for entry in parse_entries(value)? {
                    tx.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp)
                         VALUES (?1, ?2, ?3)",
                        params![metadata_id, entry.value, entry.timestamp],
                    )?;
                }
            }
            _ => bail!("unknown value type: {}", value_type),
        }

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, 'set', ?6, ?7)",
            params![target_type, target_value, key, value, value_type, email, timestamp],
        )?;

        tx.execute(
            "DELETE FROM metadata_tombstones
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type, target_value, key],
        )?;

        tx.commit()?;

        Ok(())
    }

    /// Get a single value by exact key.
    pub fn get(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, value, value_type FROM metadata
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
        )?;

        let result = stmt
            .query_row(params![target_type, target_value, key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .optional()?;

        match result {
            Some((metadata_id, value, value_type)) if value_type == "list" => Ok(Some((
                encode_list_entries_by_metadata_id(&self.conn, metadata_id)?,
                value_type,
            ))),
            Some((_, value, value_type)) => Ok(Some((value, value_type))),
            None => Ok(None),
        }
    }

    /// Get all key/value pairs for a target, optionally filtered by key prefix.
    pub fn get_all(
        &self,
        target_type: &str,
        target_value: &str,
        key_prefix: Option<&str>,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match key_prefix {
            Some(prefix) => (
                "SELECT rowid, key, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 AND (key = ?3 OR key LIKE ?4 ESCAPE '\\')
                 ORDER BY key",
                vec![
                    Box::new(target_type.to_string()),
                    Box::new(target_value.to_string()),
                    Box::new(prefix.to_string()),
                    Box::new(format!("{}:%", escape_like_pattern(prefix))),
                ],
            ),
            None => (
                "SELECT rowid, key, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 ORDER BY key",
                vec![
                    Box::new(target_type.to_string()),
                    Box::new(target_value.to_string()),
                ],
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            let (metadata_id, key, value, value_type) = row?;
            if value_type == "list" {
                let encoded = encode_list_entries_by_metadata_id(&self.conn, metadata_id)?;
                results.push((key, encoded, value_type));
            } else {
                results.push((key, value, value_type));
            }
        }
        Ok(results)
    }

    /// Get authorship info for a key from the log (most recent entry).
    pub fn get_authorship(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT email, timestamp FROM metadata_log
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3
             ORDER BY timestamp DESC LIMIT 1",
        )?;

        let result = stmt
            .query_row(params![target_type, target_value, key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;

        Ok(result)
    }

    /// Remove a key.
    pub fn rm(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<bool> {
        let tx = self.conn.unchecked_transaction()?;

        let metadata_id = tx
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type, target_value, key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;

        let deleted = if let Some(metadata_id) = metadata_id {
            tx.execute(
                "DELETE FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            tx.execute("DELETE FROM metadata WHERE rowid = ?1", params![metadata_id])?
        } else {
            0
        };

        if deleted > 0 {
            tx.execute(
                "INSERT INTO metadata_tombstones (target_type, target_value, key, timestamp, email)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(target_type, target_value, key) DO UPDATE
                 SET timestamp = excluded.timestamp, email = excluded.email",
                params![target_type, target_value, key, timestamp, email],
            )?;

            tx.execute(
                "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                 VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
                params![target_type, target_value, key, email, timestamp],
            )?;
        }

        tx.commit()?;

        Ok(deleted > 0)
    }

    /// Push a value onto a list. If the key is currently a string, convert to list first.
    pub fn list_push(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let existing = {
            let mut stmt = tx.prepare(
                "SELECT rowid, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type, target_value, key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .optional()?
        };

        let (metadata_id, mut entries) = match existing {
            Some((metadata_id, current_val, current_type)) => {
                if current_type == "list" {
                    let entries = load_list_entries_by_metadata_id_tx(&tx, metadata_id)?;
                    (metadata_id, entries)
                } else {
                    // Convert string to list
                    let current_str: String = serde_json::from_str(&current_val)?;
                    tx.execute(
                        "UPDATE metadata
                         SET value = '[]', value_type = 'list', last_timestamp = ?1
                         WHERE rowid = ?2",
                        params![timestamp, metadata_id],
                    )?;
                    tx.execute(
                        "DELETE FROM list_values WHERE metadata_id = ?1",
                        params![metadata_id],
                    )?;
                    tx.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp)
                         VALUES (?1, ?2, 0)",
                        params![metadata_id, current_str],
                    )?;
                    let entries = load_list_entries_by_metadata_id_tx(&tx, metadata_id)?;
                    (metadata_id, entries)
                }
            }
            None => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4)",
                    params![target_type, target_value, key, timestamp],
                )?;
                let metadata_id = tx.last_insert_rowid();
                (metadata_id, Vec::new())
            }
        };

        let unique_timestamp = ensure_unique_timestamp(timestamp, &entries);
        let new_entry = ListEntry {
            value: value.to_string(),
            timestamp: unique_timestamp,
        };
        tx.execute(
            "INSERT INTO list_values (metadata_id, value, timestamp)
             VALUES (?1, ?2, ?3)",
            params![metadata_id, &new_entry.value, new_entry.timestamp],
        )?;
        entries.push(new_entry);

        let new_value = encode_entries(&entries)?;

        tx.execute(
            "UPDATE metadata
             SET value = '[]', value_type = 'list', last_timestamp = ?1
             WHERE rowid = ?2",
            params![timestamp, metadata_id],
        )?;

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', 'push', ?5, ?6)",
            params![target_type, target_value, key, &new_value, email, timestamp],
        )?;

        tx.execute(
            "DELETE FROM metadata_tombstones
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type, target_value, key],
        )?;

        tx.commit()?;

        Ok(())
    }

    /// Pop a value from a list.
    pub fn list_pop(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let existing = {
            let mut stmt = tx.prepare(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type, target_value, key], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "list" {
                    bail!("key '{}' is not a list", key);
                }
                let mut list_rows = load_list_rows_by_metadata_id_tx(&tx, metadata_id)?;
                if let Some(pos) = list_rows.iter().rposition(|row| row.value == value) {
                    let removed = list_rows.remove(pos);
                    tx.execute(
                        "DELETE FROM list_values WHERE rowid = ?1",
                        params![removed.rowid],
                    )?;
                } else {
                    bail!("value '{}' not found in list", value);
                }

                let list_entries: Vec<ListEntry> = list_rows
                    .iter()
                    .map(|row| ListEntry {
                        value: row.value.clone(),
                        timestamp: row.timestamp,
                    })
                    .collect();
                let new_value = encode_entries(&list_entries)?;

                tx.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                tx.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'pop', ?5, ?6)",
                    params![target_type, target_value, key, &new_value, email, timestamp],
                )?;

                tx.execute(
                    "DELETE FROM metadata_tombstones
                     WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type, target_value, key],
                )?;

                tx.commit()?;

                Ok(())
            }
            None => bail!("key '{}' not found", key),
        }
    }

    /// Apply a tombstone from exchange data:
    /// remove current value (if any), record tombstone, and log the operation.
    pub fn apply_tombstone(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        let metadata_id = tx
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type, target_value, key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if let Some(metadata_id) = metadata_id {
            tx.execute(
                "DELETE FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            tx.execute("DELETE FROM metadata WHERE rowid = ?1", params![metadata_id])?;
        }

        tx.execute(
            "INSERT INTO metadata_tombstones (target_type, target_value, key, timestamp, email)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(target_type, target_value, key) DO UPDATE
             SET timestamp = excluded.timestamp, email = excluded.email",
            params![target_type, target_value, key, timestamp, email],
        )?;

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
            params![target_type, target_value, key, email, timestamp],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get all metadata entries (for serialization).
    /// Returns (target_type, target_value, key, value, value_type, last_timestamp).
    pub fn get_all_metadata(&self) -> Result<Vec<(String, String, String, String, String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, target_type, target_value, key, value, value_type, last_timestamp
             FROM metadata
             ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            let (metadata_id, target_type, target_value, key, value, value_type, last_timestamp) =
                row?;
            if value_type == "list" {
                let encoded = encode_list_entries_by_metadata_id(&self.conn, metadata_id)?;
                results.push((
                    target_type,
                    target_value,
                    key,
                    encoded,
                    value_type,
                    last_timestamp,
                ));
            } else {
                results.push((target_type, target_value, key, value, value_type, last_timestamp));
            }
        }
        Ok(results)
    }

    /// Get all tombstones for serialization.
    /// Returns (target_type, target_value, key, timestamp, email).
    pub fn get_all_tombstones(&self) -> Result<Vec<(String, String, String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, timestamp, email
             FROM metadata_tombstones
             ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get entries modified since a given timestamp (for incremental serialization).
    pub fn get_modified_since(
        &self,
        since: i64,
    ) -> Result<Vec<(String, String, String, String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT ml.target_type, ml.target_value, ml.key, ml.operation,
                    COALESCE(m.value, ''), COALESCE(m.value_type, '')
             FROM metadata_log ml
             LEFT JOIN metadata m ON ml.target_type = m.target_type
                 AND ml.target_value = m.target_value AND ml.key = m.key
             WHERE ml.timestamp > ?1
             ORDER BY ml.target_type, ml.target_value, ml.key",
        )?;

        let rows = stmt.query_map(params![since], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get the set of (target_type, target_value, key) that have been locally
    /// modified since a given timestamp.
    #[allow(dead_code)]
    pub fn get_locally_modified_keys(
        &self,
        since: Option<i64>,
    ) -> Result<std::collections::HashSet<(String, String, String)>> {
        use std::collections::HashSet;

        let since_ts = since.unwrap_or(0);
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT target_type, target_value, key
             FROM metadata_log
             WHERE timestamp > ?1",
        )?;

        let rows = stmt.query_map(params![since_ts], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut result = HashSet::new();
        for row in rows {
            result.insert(row?);
        }
        Ok(result)
    }

    /// Get the last materialized timestamp.
    pub fn get_last_materialized(&self) -> Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT last_materialized FROM sync_state WHERE id = 1")?;
        let result = stmt
            .query_row([], |row| row.get::<_, Option<i64>>(0))
            .optional()?;
        Ok(result.flatten())
    }

    /// Set the last materialized timestamp.
    pub fn set_last_materialized(&self, timestamp: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE sync_state SET last_materialized = ?1 WHERE id = 1",
            params![timestamp],
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ListRow {
    rowid: i64,
    value: String,
    timestamp: i64,
}

fn load_list_entries_by_metadata_id(
    conn: &Connection,
    metadata_id: i64,
) -> Result<Vec<ListEntry>> {
    let mut stmt = conn.prepare(
        "SELECT value, timestamp
         FROM list_values
         WHERE metadata_id = ?1
         ORDER BY timestamp",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok(ListEntry {
            value: row.get::<_, String>(0)?,
            timestamp: row.get::<_, i64>(1)?,
        })
    })?;

    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn load_list_entries_by_metadata_id_tx(
    tx: &rusqlite::Transaction<'_>,
    metadata_id: i64,
) -> Result<Vec<ListEntry>> {
    let mut stmt = tx.prepare(
        "SELECT value, timestamp
         FROM list_values
         WHERE metadata_id = ?1
         ORDER BY timestamp",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok(ListEntry {
            value: row.get::<_, String>(0)?,
            timestamp: row.get::<_, i64>(1)?,
        })
    })?;

    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn load_list_rows_by_metadata_id_tx(
    tx: &rusqlite::Transaction<'_>,
    metadata_id: i64,
) -> Result<Vec<ListRow>> {
    let mut stmt = tx.prepare(
        "SELECT rowid, value, timestamp
         FROM list_values
         WHERE metadata_id = ?1
         ORDER BY timestamp",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok(ListRow {
            rowid: row.get::<_, i64>(0)?,
            value: row.get::<_, String>(1)?,
            timestamp: row.get::<_, i64>(2)?,
        })
    })?;

    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn encode_list_entries_by_metadata_id(conn: &Connection, metadata_id: i64) -> Result<String> {
    let entries = load_list_entries_by_metadata_id(conn, metadata_id)?;
    Ok(encode_entries(&entries)?)
}

fn escape_like_pattern(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '%' => out.push_str("\\%"),
            '_' => out.push_str("\\_"),
            _ => out.push(ch),
        }
    }
    out
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "agent:model",
            "\"claude-4.6\"",
            "string",
            "test@test.com",
            1000,
        )
        .unwrap();
        let result = db.get("commit", "abc123", "agent:model").unwrap();
        assert_eq!(
            result,
            Some(("\"claude-4.6\"".to_string(), "string".to_string()))
        );
    }

    #[test]
    fn test_set_upsert() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit", "abc123", "key", "\"v1\"", "string", "a@b.com", 1000,
        )
        .unwrap();
        db.set(
            "commit", "abc123", "key", "\"v2\"", "string", "a@b.com", 2000,
        )
        .unwrap();
        let result = db.get("commit", "abc123", "key").unwrap();
        assert_eq!(result, Some(("\"v2\"".to_string(), "string".to_string())));
    }

    #[test]
    fn test_get_all_with_prefix() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "agent:model",
            "\"claude\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "agent:provider",
            "\"anthropic\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit", "abc123", "other", "\"val\"", "string", "a@b.com", 1000,
        )
        .unwrap();

        let results = db.get_all("commit", "abc123", Some("agent")).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_like_wildcards() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "a%:literal",
            "\"match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "abc:anything",
            "\"should-not-match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "a_:literal",
            "\"underscore-match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "ab:anything",
            "\"underscore-should-not-match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();

        let percent_results = db.get_all("commit", "abc123", Some("a%")).unwrap();
        let percent_keys: Vec<String> = percent_results.into_iter().map(|r| r.0).collect();
        assert_eq!(percent_keys, vec!["a%:literal".to_string()]);

        let underscore_results = db.get_all("commit", "abc123", Some("a_")).unwrap();
        let underscore_keys: Vec<String> = underscore_results.into_iter().map(|r| r.0).collect();
        assert_eq!(underscore_keys, vec!["a_:literal".to_string()]);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_backslash() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            r"agent\name:model",
            "\"match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "agentxname:model",
            "\"should-not-match\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db.get_all("commit", "abc123", Some(r"agent\name")).unwrap();
        let keys: Vec<String> = results.into_iter().map(|r| r.0).collect();
        assert_eq!(keys, vec![r"agent\name:model".to_string()]);
    }

    #[test]
    fn test_rm() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit", "abc123", "key", "\"val\"", "string", "a@b.com", 1000,
        )
        .unwrap();
        assert!(db.rm("commit", "abc123", "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get("commit", "abc123", "key").unwrap(), None);
    }

    #[test]
    fn test_rm_creates_tombstone() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit", "abc123", "key", "\"val\"", "string", "a@b.com", 1000,
        )
        .unwrap();
        assert!(db.rm("commit", "abc123", "key", "a@b.com", 2000).unwrap());

        let tombstones = db.get_all_tombstones().unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(
            tombstones[0],
            (
                "commit".to_string(),
                "abc123".to_string(),
                "key".to_string(),
                2000,
                "a@b.com".to_string()
            )
        );
    }

    #[test]
    fn test_set_clears_tombstone() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit", "abc123", "key", "\"v1\"", "string", "a@b.com", 1000,
        )
        .unwrap();
        assert!(db.rm("commit", "abc123", "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get_all_tombstones().unwrap().len(), 1);

        db.set(
            "commit", "abc123", "key", "\"v2\"", "string", "a@b.com", 3000,
        )
        .unwrap();

        assert_eq!(db.get_all_tombstones().unwrap().len(), 0);
        let result = db.get("commit", "abc123", "key").unwrap();
        assert_eq!(result, Some(("\"v2\"".to_string(), "string".to_string())));
    }

    #[test]
    fn test_list_push() {
        let db = Db::open_in_memory().unwrap();
        db.list_push("commit", "abc123", "tags", "first", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "tags", "second", "a@b.com", 2000)
            .unwrap();
        let (val, vtype) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        assert_eq!(vtype, "list");
        let list = crate::list_value::list_values_from_json(&val).unwrap();
        assert_eq!(list, vec!["first", "second"]);
    }

    #[test]
    fn test_set_list_stores_rows_in_list_values_table() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "tags",
            r#"[{"value":"a","timestamp":1000},{"value":"b","timestamp":1001}]"#,
            "list",
            "a@b.com",
            2000,
        )
        .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let list_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(list_rows, 2);

        let (val, vtype) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        assert_eq!(vtype, "list");
        let list = crate::list_value::list_values_from_json(&val).unwrap();
        assert_eq!(list, vec!["a", "b"]);
    }

    #[test]
    fn test_set_list_replaces_existing_list_rows() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "tags",
            r#"[{"value":"a","timestamp":1000},{"value":"b","timestamp":1001}]"#,
            "list",
            "a@b.com",
            2000,
        )
        .unwrap();
        db.set(
            "commit",
            "abc123",
            "tags",
            r#"[{"value":"c","timestamp":3000}]"#,
            "list",
            "a@b.com",
            4000,
        )
        .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let list_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(list_rows, 1);

        let (val, _) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        let list = crate::list_value::list_values_from_json(&val).unwrap();
        assert_eq!(list, vec!["c"]);
    }

    #[test]
    fn test_list_push_converts_string() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "key",
            "\"original\"",
            "string",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.list_push("commit", "abc123", "key", "appended", "a@b.com", 2000)
            .unwrap();
        let (val, vtype) = db.get("commit", "abc123", "key").unwrap().unwrap();
        assert_eq!(vtype, "list");
        let list = crate::list_value::list_values_from_json(&val).unwrap();
        assert_eq!(list, vec!["original", "appended"]);
    }

    #[test]
    fn test_list_pop() {
        let db = Db::open_in_memory().unwrap();
        db.list_push("commit", "abc123", "tags", "a", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "tags", "b", "a@b.com", 2000)
            .unwrap();
        db.list_pop("commit", "abc123", "tags", "b", "a@b.com", 3000)
            .unwrap();
        let (val, _) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        let list = crate::list_value::list_values_from_json(&val).unwrap();
        assert_eq!(list, vec!["a"]);
    }

    #[test]
    fn test_apply_tombstone_removes_list_values_rows() {
        let db = Db::open_in_memory().unwrap();
        db.list_push("commit", "abc123", "tags", "a", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "tags", "b", "a@b.com", 2000)
            .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let before_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(before_count, 2);

        db.apply_tombstone("commit", "abc123", "tags", "user@example.com", 3000)
            .unwrap();

        let after_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after_count, 0);
        assert_eq!(db.get("commit", "abc123", "tags").unwrap(), None);
    }

    #[test]
    fn test_authorship() {
        let db = Db::open_in_memory().unwrap();
        db.set(
            "commit",
            "abc123",
            "key",
            "\"val\"",
            "string",
            "user@example.com",
            42000,
        )
        .unwrap();
        let (email, ts) = db
            .get_authorship("commit", "abc123", "key")
            .unwrap()
            .unwrap();
        assert_eq!(email, "user@example.com");
        assert_eq!(ts, 42000);
    }

    #[test]
    fn test_sync_state() {
        let db = Db::open_in_memory().unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), None);
        db.set_last_materialized(5000).unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), Some(5000));
    }

    #[test]
    fn test_last_timestamp_stored_and_returned() {
        let db = Db::open_in_memory().unwrap();

        // set stores the timestamp
        db.set(
            "commit", "abc123", "key", "\"val\"", "string", "a@b.com", 5000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].5, 5000);

        // upsert updates the timestamp
        db.set(
            "commit", "abc123", "key", "\"val2\"", "string", "a@b.com", 9000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries[0].5, 9000);

        // list_push stores the timestamp
        db.list_push("commit", "abc123", "tags", "first", "a@b.com", 11000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.2 == "tags").unwrap();
        assert_eq!(tags.5, 11000);

        // list_pop updates the timestamp
        db.list_push("commit", "abc123", "tags", "second", "a@b.com", 12000)
            .unwrap();
        db.list_pop("commit", "abc123", "tags", "second", "a@b.com", 13000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.2 == "tags").unwrap();
        assert_eq!(tags.5, 13000);
    }
}
