use sql_queries::BOOTSTRAP_SCHEMA;
use mysql_binlogdumper_rs::{
    binlog_client::BinlogClient, event::event_data::EventData,
    event::table_map_event::TableMapEvent,
};
use duckdb::{params, Connection, Result};
use mysql_async::prelude::*;
use mysql_async::*;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::join;
use tokio::sync::mpsc;

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
struct TableSchema {
    column_types: Vec<u8>,
    column_metas: Vec<u16>,
    null_bits: Vec<bool>,
    schema_version: String,
}

// Create a channel type for async communication
#[derive(Clone)]
struct SchemaUpdateRequest {
    schema_name: String,
    table_name: String,
    schema_version: String,
    binlog_timestamp: u32,
    column_types: Vec<u8>,
    column_meta: Vec<u16>,
    null_bits: Vec<bool>,
}

#[derive(Clone)]
struct EventPersistenceRequest {
    schema_name: String,
    table_name: String,
    event_type: String,
    event_data: EventData,
    binlog_file: String,
    event_position: u32,
    event_timestamp: u32,
    schema_version: String,
}

async fn get_table_columns(
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
async fn schema_update_worker(
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

async fn event_persistence_worker(
    mut rx: mpsc::Receiver<EventPersistenceRequest>,
    conn: Connection,
) {
    while let Some(req) = rx.recv().await {
        // Persist the binlog event
        let event_rows = match req.event_data {
            EventData::WriteRows(write_event) => write_event
                .rows
                .clone()
                .into_iter()
                .map(|mut row| row.to_json())
                .collect::<Vec<Value>>(),

            EventData::DeleteRows(delete_event) => delete_event
                .rows
                .clone()
                .into_iter()
                .map(|mut row| row.to_json())
                .collect::<Vec<Value>>(),

            EventData::UpdateRows(update_event) => update_event
                .rows
                .clone()
                .into_iter()
                .map(|(mut before, mut after)| {
                    Value::Object(
                        [
                            ("before".to_string(), before.to_json()),
                            ("after".to_string(), after.to_json()),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect::<Vec<Value>>(),

            _ => vec![],
        };
        if let Err(e) = conn.execute(
            r"
            INSERT INTO binlog_events (
                binlog_filename, binlog_position, binlog_timestamp,
                event_schema_name, event_table_name, event_type,
                schema_version, event_row_count, event_rows
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
            params![
                req.binlog_file,
                req.event_position,
                req.event_timestamp,
                req.schema_name,
                req.table_name,
                req.event_type,
                req.schema_version,
                event_rows.len(),
                serde_json::to_string(&event_rows).unwrap(),
            ],
        ) {
            eprintln!("Error persisting binlog event: {}", e);
        }
    }
}

async fn handle_close(
    conn: &Connection,
    current_binlog_file: &str,
    current_binlog_pos: u64,
    output_path: String,
) -> Result<String> {
    let export_path = create_manifest(
        &conn,
        output_path,
        format!("{current_binlog_file}:{current_binlog_pos}"),
    )?;

    Ok(export_path)
}

fn should_stop(
    current_binlog_file: &str,
    current_binlog_pos: u64,
    stop_file: &Option<String>,
    stop_pos: &Option<u64>,
    start_time: &Instant,
    max_duration: u64,
) -> bool {
    // Check time-based stop condition first
    if max_duration > 0 && start_time.elapsed() >= Duration::from_secs(max_duration) {
        println!(
            "Stopping due to time limit of {} seconds reached",
            max_duration
        );
        return true;
    }
    if let Some(stop_file) = stop_file {
        if current_binlog_file == stop_file {
            if let Some(stop_pos) = stop_pos {
                return current_binlog_pos >= *stop_pos;
            }
            return true; // Stop if only the file matches
        }
    }
    false
}

fn set_excluded_items(excluded_objects: String) -> Option<Vec<String>> {
    let excluded_objects = if excluded_objects.is_empty() {
        None
    } else {
        Some(
            excluded_objects
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>(),
        )
    };
    excluded_objects
}

fn object_excluded(
    excluded_dbs: &Option<Vec<String>>,
    excluded_objects: &Option<Vec<String>>,
    schema_name: &String,
    table_name: &String,
) -> bool {
    // Check if the current database is in the excluded list
    if excluded_dbs
        .as_ref()
        .map_or(false, |excluded| excluded.contains(schema_name))
    {
        return true;
    }

    // Check if the current table (or wildcard) is in the excluded objects list
    if excluded_objects.as_ref().map_or(false, |excluded| {
        let full_name = format!("{}.{}", schema_name, table_name);
        let wildcard_name = format!("*.{}", table_name);
        excluded.contains(&full_name) || excluded.contains(&wildcard_name)
    }) {
        return true;
    }

    false
}

fn create_schema_update_request(
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

fn create_event_persistence_request(
    schema_name: &str,
    table_name: &str,
    event_type: String,
    event_data: EventData,
    binlog_file: &str,
    event_position: &u32,
    event_timestamp: &u32,
    schema_cache: &HashMap<String, TableSchema>,
) -> EventPersistenceRequest {
    EventPersistenceRequest {
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        event_type,
        event_data,
        binlog_file: binlog_file.to_string(),
        event_position: event_position.clone(),
        event_timestamp: event_timestamp.clone(),
        schema_version: schema_cache
            .get(format!("{schema_name}.{table_name}").as_str())
            .unwrap()
            .schema_version
            .to_string(),
    }
}

#[tokio::main]
pub async fn dump_and_parse(
    conn: &Connection,
    db_url: String,
    pool: Pool,
    binlog_file: String,
    binlog_position: u32,
    stop_at_binlog_file: String,
    excluded_dbs: String,
    excluded_objects: String,
    include_event_types: String,
    gtid_enabled: bool,
    gtid_set: String,
    server_id: u64,
    max_duration: u64,
    output_path: String,
) -> Result<String, Box<dyn std::error::Error>> {
    // Create a channel for schema updates
    let (tx, rx) = mpsc::channel(100); // Buffer size of 100
                                       // Clone connection and spawn the bg worker
    let worker_conn = conn.try_clone()?;
    tokio::spawn(schema_update_worker(rx, pool.clone(), worker_conn));

    // Create a channel for event persistence
    let (bl_tx, bl_rx) = mpsc::channel(500); // Buffer size of 100
                                             // Clone connection and spawn the bg worker
    tokio::spawn(event_persistence_worker(bl_rx, conn.try_clone().unwrap()));

    // Tracking variables
    let mut current_binlog_file = binlog_file.clone();
    let mut current_binlog_pos: u64;
    // let mut current_schema_version = String::new();
    let mut schema_cache: HashMap<String, TableSchema> = HashMap::new();

    // Excluded databases and objects
    let excluded_dbs = set_excluded_items(excluded_dbs);
    let excluded_objects = set_excluded_items(excluded_objects);

    // Stop at binlog file and position
    let (stop_file, stop_pos) = if !stop_at_binlog_file.is_empty() {
        if let Some((file, pos)) = stop_at_binlog_file.split_once(':') {
            (Some(file.to_string()), pos.parse::<u64>().ok())
        } else {
            (Some(stop_at_binlog_file), None)
        }
    } else {
        (None, None)
    };

    // Create start time tracker
    let start_time = Instant::now();

    let mut client = BinlogClient {
        url: db_url,
        binlog_filename: binlog_file,
        binlog_position,
        server_id,
        gtid_enabled,
        gtid_set,
        include_event_types: include_event_types.clone(),
        heartbeat_interval_secs: 5,
    };

    let mut stream = client.connect().await.unwrap();

    loop {
        let (header, data) = stream.read().await.unwrap();

        // Update the current position after every event
        current_binlog_pos = u64::from(header.next_event_position);

        match &data {
            EventData::Rotate(event) => {
                current_binlog_file = event.binlog_filename.to_string();
                current_binlog_pos = event.binlog_position;

                println!("Rotating to: {current_binlog_file}:{current_binlog_pos}");
            }
            EventData::TableMap(event) => {
                // Skip if database or object is excluded
                if object_excluded(
                    &excluded_dbs,
                    &excluded_objects,
                    &event.database_name,
                    &event.table_name,
                ) {
                    continue;
                }

                // Check if schema has changed or is new
                let cache_key = format!("{}.{}", event.database_name, event.table_name);
                let should_update = schema_cache.get(&cache_key).map_or(true, |cached_schema| {
                    cached_schema.column_types != event.column_types
                        || cached_schema.column_metas != event.column_metas
                        || cached_schema.null_bits != event.null_bits
                });

                // Check if schema has changed
                if should_update {
                    // Send schema update request to background worker
                    let schema_version = format!("{current_binlog_file}.{current_binlog_pos}");
                    let update_req = create_schema_update_request(
                        &event.database_name,
                        &event.table_name,
                        &schema_version,
                        header.timestamp,
                        &event,
                    );

                    if let Err(e) = tx.send(update_req).await {
                        eprintln!("Failed to send schema update request: {}", e);
                    }

                    // Update cache with current schema
                    schema_cache.insert(
                        cache_key.clone(),
                        TableSchema {
                            column_types: event.column_types.clone(),
                            column_metas: event.column_metas.clone(),
                            null_bits: event.null_bits.clone(),
                            schema_version,
                        },
                    );
                }
            }
            EventData::WriteRows(write_event) => {
                // Skip if database or object is excluded
                if object_excluded(
                    &excluded_dbs,
                    &excluded_objects,
                    &write_event.schema_name,
                    &write_event.table_name,
                ) {
                    continue;
                }

                if let Err(e) = bl_tx
                    .send(create_event_persistence_request(
                        &write_event.schema_name,
                        &write_event.table_name,
                        "INSERT".to_string(),
                        data.clone(),
                        &current_binlog_file,
                        &header.next_event_position,
                        &header.timestamp,
                        &schema_cache,
                    ))
                    .await
                {
                    eprintln!("Failed to send event persistence request: {}", e);
                }
            }
            EventData::DeleteRows(delete_event) => {
                // Skip if database or object is excluded
                if object_excluded(
                    &excluded_dbs,
                    &excluded_objects,
                    &delete_event.schema_name,
                    &delete_event.table_name,
                ) {
                    continue;
                }

                if let Err(e) = bl_tx
                    .send(create_event_persistence_request(
                        &delete_event.schema_name,
                        &delete_event.table_name,
                        "DELETE".to_string(),
                        data.clone(),
                        &current_binlog_file,
                        &header.next_event_position,
                        &header.timestamp,
                        &schema_cache,
                    ))
                    .await
                {
                    eprintln!("Failed to send event persistence request: {}", e);
                }
            }
            EventData::UpdateRows(update_event) => {
                // Skip if database or object is excluded
                if object_excluded(
                    &excluded_dbs,
                    &excluded_objects,
                    &update_event.schema_name,
                    &update_event.table_name,
                ) {
                    continue;
                }

                if let Err(e) = bl_tx
                    .send(create_event_persistence_request(
                        &update_event.schema_name,
                        &update_event.table_name,
                        "UPDATE".to_string(),
                        data.clone(),
                        &current_binlog_file,
                        &header.next_event_position,
                        &header.timestamp,
                        &schema_cache,
                    ))
                    .await
                {
                    eprintln!("Failed to send event persistence request: {}", e);
                }
            }
            _ => {}
        }

        // Check if we should stop processing
        if should_stop(
            &current_binlog_file,
            current_binlog_pos,
            &stop_file,
            &stop_pos,
            &start_time,
            max_duration,
        ) {
            println!(
                "Stopping at {current_binlog_file}:{current_binlog_pos} (elapsed time: {:.2?})",
                start_time.elapsed()
            );
            break;
        }
    }
    let (_, _, export_path) = join!(
        stream.close(),
        pool.disconnect(),
        handle_close(conn, &current_binlog_file, current_binlog_pos, output_path,)
    );

    Ok(export_path.unwrap())
}

fn create_manifest(
    conn: &Connection,
    output_path: String,
    last_processed_binlog: String,
) -> Result<String> {
    let start_time = Instant::now();
    let export_path = format!("{output_path}/{last_processed_binlog}");

    match std::fs::create_dir_all(&export_path) {
        Err(e) => {
            eprintln!("Error creating export directory: {}", e);
            return Ok("".to_string());
        }
        _ => {}
    }

    if let Err(e) = conn.execute_batch(format!(r"
        SELECT * FROM force_checkpoint();

        COPY (SELECT * FROM events_manifest_view ORDER BY min_binlog_timestamp) TO '{export_path}/manifest.csv' (FORMAT CSV);
        COPY schema_changes TO '{export_path}/schema_changes.csv' (FORMAT CSV);
        COPY (SELECT * FROM binlog_events_view) TO '{export_path}/binlog_events_full.parquet' (FORMAT PARQUET);
        COPY (SELECT * FROM binlog_lag_view) TO '{export_path}/export_stats.json' (FORMAT JSON);
        ").as_str()
    )
    {
        eprintln!("Error creating manifest: {}", e);
        return Ok("".to_string());
    }

    println!("Manifest created in {:.2?}", start_time.elapsed());

    Ok(export_path)
}

pub fn bootstrap_runtime_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(BOOTSTRAP_SCHEMA)?;
    Ok(())
}
