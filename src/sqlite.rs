use super::CollectionReport;
use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;

pub(super) fn save_report(path: &Path, report: &CollectionReport) -> Result<()> {
    let connection = Connection::open(path)
        .with_context(|| format!("open SQLite database {}", path.display()))?;
    initialize_schema(&connection)?;

    let payload =
        serde_json::to_string(report).context("serialize collection report for SQLite storage")?;

    connection
        .execute(
            "INSERT INTO collection_snapshots (
                created_at,
                collected_at_unix_seconds,
                namespace,
                all_namespaces,
                top,
                row_limit,
                energy_source,
                payload
            ) VALUES (CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                report.collected_at_unix_seconds as i64,
                report.scope.namespace.as_deref(),
                report.scope.all_namespaces,
                report.scope.top,
                report.scope.limit.map(|limit| limit as i64),
                report.telemetry.energy_source,
                payload,
            ],
        )
        .with_context(|| format!("insert collection snapshot into {}", path.display()))?;

    Ok(())
}

fn initialize_schema(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS collection_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                collected_at_unix_seconds INTEGER NOT NULL,
                namespace TEXT,
                all_namespaces INTEGER NOT NULL,
                top TEXT NOT NULL,
                row_limit INTEGER,
                energy_source TEXT NOT NULL,
                payload TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_collection_snapshots_collected_at
            ON collection_snapshots (collected_at_unix_seconds);

            CREATE INDEX IF NOT EXISTS idx_collection_snapshots_created_at
            ON collection_snapshots (created_at);

            CREATE INDEX IF NOT EXISTS idx_collection_snapshots_namespace
            ON collection_snapshots (namespace);",
        )
        .context("initialize SQLite collection snapshot schema")?;

    Ok(())
}
