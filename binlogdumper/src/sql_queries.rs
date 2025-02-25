pub const BOOTSTRAP_SCHEMA: &str = r#"
CREATE SEQUENCE IF NOT EXISTS seq_schema_changes;

CREATE TABLE IF NOT EXISTS schema_changes (
    id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_schema_changes'),
    event_schema_name VARCHAR,
    event_table_name VARCHAR,
    schema_version VARCHAR,
    binlog_timestamp BIGINT,
    column_types JSON,
    column_meta JSON,
    null_bits JSON,
    table_info JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX ix_schema_changes ON schema_changes (
    event_schema_name,
    event_table_name,
    schema_version
);

CREATE SEQUENCE IF NOT EXISTS seq_binlog_events;

CREATE TABLE IF NOT EXISTS binlog_events (
    id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_binlog_events'),
    binlog_filename VARCHAR,
    binlog_position INTEGER,
    binlog_timestamp BIGINT,
    binlog_timestamp_fmt TIMESTAMP GENERATED ALWAYS AS (make_timestamp(binlog_timestamp * 1000000)) VIRTUAL,
    event_schema_name VARCHAR,
    event_table_name VARCHAR,
    event_type VARCHAR,
    schema_version VARCHAR,
    event_row_count INTEGER,
    event_rows JSON,
    inserted_at timestamp default current_timestamp
);

CREATE INDEX ix_event_schema_name_table_name ON binlog_events (
    event_type,
    event_schema_name,
    event_table_name,
    schema_version
);

CREATE VIEW IF NOT EXISTS events_manifest_view AS
SELECT  binlog_filename,
        event_schema_name,
        event_table_name,
        min(id) as first_seq_id,
        max(id) as last_seq_id,
        min(binlog_position) as min_binlog_position,
        max(binlog_position) as max_binlog_position,
        min(binlog_timestamp) as min_binlog_timestamp,
        max(binlog_timestamp) as max_binlog_timestamp,
        min(binlog_timestamp_fmt) as min_binlog_timestamp_fmt,
        max(binlog_timestamp_fmt) as max_binlog_timestamp_fmt,
        min(inserted_at) as min_inserted_at,
        max(inserted_at) as max_inserted_at,
        min(event_row_count) as min_event_row_count,
        max(event_row_count) as max_event_row_count,
        sum(event_row_count) as total_event_row_count
FROM binlog_events
GROUP BY ALL;

CREATE VIEW IF NOT EXISTS binlog_events_view AS
WITH all_events_cte as (
  SELECT  * exclude(event_rows),
          if(event_type = 'UPDATE', 1, 0) as has_before_values,
          event_rows->'$[*]' event_rows_arr
  FROM binlog_events
),
unnest_rows_cte as (
  SELECT  *,
          generate_subscripts(event_rows_arr, 1) AS event_row_ix,
          unnest(event_rows_arr) as event_row
  FROM all_events_cte
),
table_columns as (
  SELECT  t.event_schema_name,
          t.event_table_name,
          t.schema_version,
          map(
            array_agg('COLUMN_' || (j.column_info->>'$.ordinal_position') order by (j.column_info->>'$.ordinal_position')::int),
            array_agg(j.column_info->>'$.column_name' order by (j.column_info->>'$.ordinal_position')::int)
          ) as column_name_map,
          count() as column_count
  FROM schema_changes t, unnest(t.table_info->'$[*]') j (column_info)
  GROUP by t.event_schema_name, t.event_table_name, t.schema_version
),
abstract_event_type_cte as (
  SELECT  t.*,
          if(t.has_before_values = 1, t.event_row.before, null) as event_row_before,
          if(t.has_before_values = 1, t.event_row.after, t.event_row) as event_row
  FROM unnest_rows_cte t
),
rename_cte as (
  SELECT  t.id,
          t.event_schema_name,
          t.event_table_name,
          t.event_type,
          t.event_row_ix,
          t.has_before_values,
          case when t.has_before_values = 1 then
            to_json(map(
              map_values(column_name_map),
              apply(map_keys(column_name_map), x -> json_extract(t.event_row_before, x))
            ))
          else t.event_row_before end as renamed_event_row_before,
          to_json(map(
            map_values(column_name_map),
            apply(map_keys(column_name_map), x -> json_extract(t.event_row, x))
          )) as renamed_event_row
  FROM abstract_event_type_cte t
  JOIN table_columns c on
        c.event_schema_name = t.event_schema_name
    and c.event_table_name = t.event_table_name
    and t.schema_version = c.schema_version
  ORDER by t.id, t.event_row_ix
),
agg_event_rows_cte as (
  SELECT  id, event_schema_name, event_table_name, event_type,
          json_group_array(if(
              has_before_values = 1,
              json_object(
                'before', renamed_event_row_before,
                'after', renamed_event_row
              ),
              renamed_event_row
          )) as renamed_event_rows
  FROM rename_cte
  GROUP BY id, event_schema_name, event_table_name, event_type
)
SELECT  q.* exclude(event_rows_arr, has_before_values),
        ifnull(t.renamed_event_rows, q.event_rows_arr) as event_rows
FROM all_events_cte q
LEFT JOIN agg_event_rows_cte t USING (id)
ORDER BY id
;

CREATE VIEW IF NOT EXISTS binlog_lag_view AS
SELECT  min(binlog_timestamp_fmt) as min_binlog_timestamp,
        max(binlog_timestamp_fmt) as max_binlog_timestamp,
        max(binlog_timestamp_fmt) - min(binlog_timestamp_fmt) as total_binlog_lag,
        max(inserted_at) - min(inserted_at) as total_insert_lag,
        max(inserted_at) - max(binlog_timestamp_fmt) as final_lag,
        min(inserted_at) - min(binlog_timestamp_fmt) as initial_lag,
        count(distinct binlog_filename) as binlog_file_count,
        count(distinct event_schema_name || '.' || event_table_name) as table_count,
        sum(event_row_count) as total_row_count
FROM binlog_events;
"#;
