use crate::sql_queries::BOOTSTRAP_SCHEMA;
use duckdb::{Connection, Result};
use std::time::Instant;

pub async fn handle_close(
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
