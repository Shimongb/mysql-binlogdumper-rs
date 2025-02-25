use tokio::sync::mpsc;

use duckdb::{Connection, Result, params};
use mysql_async::prelude::*;
use mysql_async::*;
use mysql_binlogdumper_rs::event::table_map_event::TableMapEvent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TableInfo {
    pub column_name: String,
    pub column_type: String,
    pub is_nullable: String,
    pub column_key: String,
    pub column_default: Option<String>,
    pub column_extra: String,
    pub ordinal_position: u32,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub column_types: Vec<u8>,
    pub column_metas: Vec<u16>,
    pub null_bits: Vec<bool>,
    pub schema_version: String,
}

// Create a channel type for async communication
#[derive(Clone)]
pub struct SchemaUpdateRequest {
    schema_name: String,
    table_name: String,
    schema_version: String,
    binlog_timestamp: u32,
    column_types: Vec<u8>,
    column_meta: Vec<u16>,
    null_bits: Vec<bool>,
}

pub fn create_schema_update_request(
    schema_name: &str,
    table_name: &str,
    schema_version: &str,
    header_timestamp: u32,
    event: &TableMapEvent,
) -> SchemaUpdateRequest {
    SchemaUpdateRequest {
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        schema_version: schema_version.to_string(),
        binlog_timestamp: header_timestamp,
        column_types: event.column_types.clone(),
        column_meta: event.column_metas.clone(),
        null_bits: event.null_bits.clone(),
    }
}

pub async fn get_table_columns(
    conn_pool: &Pool,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<TableInfo>, Error> {
    let mut conn = conn_pool.get_conn().await?;

    let result: Vec<Row> = conn
        .query_iter(format!("DESCRIBE `{schema_name}`.`{table_name}`"))
        .await?
        .collect()
        .await?;

    Ok(result
        .iter()
        .enumerate()
        .map(|(i, row)| TableInfo {
            column_name: row.get(0).unwrap(),
            column_type: row.get(1).unwrap(),
            is_nullable: row.get(2).unwrap(),
            column_key: row.get(3).unwrap(),
            column_default: row.get(4).unwrap(),
            column_extra: row.get(5).unwrap(),
            ordinal_position: (i as u32) + 1,
        })
        .collect())
}

// Background task to handle schema updates
pub async fn schema_update_worker(
    mut rx: mpsc::Receiver<SchemaUpdateRequest>,
    pool: Pool,
    conn: Connection,
) {
    while let Some(req) = rx.recv().await {
        match get_table_columns(&pool, &req.schema_name, &req.table_name).await {
            Ok(table_info) => {
                // Persist both the binlog schema change and the current table info
                if let Err(e) = conn.execute(
                    r"
                    INSERT INTO schema_changes (
                        event_schema_name,
                        event_table_name,
                        schema_version,
                        binlog_timestamp,
                        column_types,
                        column_meta,
                        null_bits,
                        table_info
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ",
                    params![
                        req.schema_name,
                        req.table_name,
                        req.schema_version,
                        req.binlog_timestamp,
                        serde_json::to_string(&req.column_types).unwrap(),
                        serde_json::to_string(&req.column_meta).unwrap(),
                        serde_json::to_string(&req.null_bits).unwrap(),
                        serde_json::to_string(&table_info).unwrap(),
                    ],
                ) {
                    eprintln!("Error persisting schema change: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Error fetching table info: {}", e);
            }
        }
    }
}
