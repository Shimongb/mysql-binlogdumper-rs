mod event_persistence;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;


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

pub async fn event_persistence_worker(
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

pub fn create_event_persistence_request(
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
