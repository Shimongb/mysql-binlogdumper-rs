mod dump_binlogs;
mod event_persistence;
mod runtime_db_utils;
mod schema_update;
mod sql_queries;

use duckdb::Connection;
use dump_binlogs::dump_and_parse;
use mysql_async::{Opts, Pool};
use runtime_db_utils::bootstrap_runtime_schema;

fn main() -> duckdb::Result<(), Box<dyn std::error::Error>> {
    let db_url = "mysql://root:963963xyz@127.0.0.1:3306".to_string();

    // Set up MySQL async connection pool
    let opts = Opts::from_url(db_url.as_str())?;

    // Set up DuckDB connection for event storage
    let conn = Connection::open_in_memory()?;

    let _ = bootstrap_runtime_schema(&conn);

    let export_path = dump_and_parse(
        &conn,
        db_url,
        Pool::new(opts),
        // "".to_string(),
        "binlog.000002".to_string(),
        // 0,
        4,
        "".to_string(),
        "dba,sre,tmp,temp,mysql,sys,performance_schema,information_schema".to_string(),
        "*.DATABASECHANGELOG,*.DATABASECHANGELOGLOCK,*.QRTZ_SCHEDULER_STATE,*.QRTZ_LOCKS,*.QRTZ_TRIGGERS,*.QRTZ_JOB_DETAILS,*.QRTZ_CALENDARS,*.QRTZ_SIMPLE_TRIGGERS,*.QRTZ_CRON_TRIGGERS,*.QRTZ_BLOB_TRIGGERS,*.QRTZ_FIRED_TRIGGERS".to_string(),
        "WRITEROWS,EXTWRITEROWS,UPDATEROWS,EXTUPDATEROWS,DELETEROWS,EXTDELETEROWS".to_string(),
        false,
        "".to_string(),
        1001,
        120,
        "output_binlog_dir".to_string(),
    )?;

    println!("{:?}", export_path);
    Ok(())
}
