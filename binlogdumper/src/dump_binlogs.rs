use mysql_binlogdumper_rs::{binlog_client::BinlogClient, event::event_data::EventData};

use crate::event_persistence::{create_event_persistence_request, event_persistence_worker};
use crate::runtime_db_utils::handle_close;
use crate::schema_update::{TableSchema, create_schema_update_request, schema_update_worker};

use duckdb::Connection;
use mysql_async::Pool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::join;
use tokio::sync::mpsc;

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
