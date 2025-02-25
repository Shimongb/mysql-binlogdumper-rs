use std::io::Cursor;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    binlog_error::BinlogError,
    column::{
        column_type::ColumnType, column_value::ColumnValue, json::json_binary::JsonBinary,
        json::json_string_formatter::HEX_CODES,
    },
    ext::cursor_ext::CursorExt,
};

use super::table_map_event::TableMapEvent;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RowEvent {
    pub column_values: Vec<ColumnValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_values_json: Option<Value>,
}

pub fn vec_u8to_hex_string(array: &[u8]) -> String {
    let mut hex_string = String::new();
    for byte in array {
        hex_string.push(HEX_CODES[(byte >> 4) as usize]);
        hex_string.push(HEX_CODES[(byte & 0xF) as usize]);
    }
    hex_string
}

impl RowEvent {
    #[allow(clippy::needless_range_loop)]
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        table_map_event: &TableMapEvent,
        included_columns: &[bool],
    ) -> Result<Self, BinlogError> {
        let null_columns = cursor.read_bits(included_columns.len(), false)?;
        let mut column_values = Vec::with_capacity(table_map_event.column_types.len());
        let mut skipped_column_count = 0;
        for i in 0..table_map_event.column_types.len() {
            if !included_columns[i] {
                skipped_column_count += 1;
                column_values.push(ColumnValue::None);
                continue;
            }

            let index = i - skipped_column_count;
            if null_columns[index] {
                column_values.push(ColumnValue::None);
                continue;
            }

            let column_meta = table_map_event.column_metas[i];
            let mut column_type = table_map_event.column_types[i];
            let mut column_length = column_meta;

            if column_type == ColumnType::String as u8 && column_meta >= 256 {
                (column_type, column_length) =
                    ColumnType::parse_string_column_meta(column_meta, column_type)?;
            }

            let col_value = ColumnValue::parse(
                cursor,
                ColumnType::from_code(column_type),
                column_meta,
                column_length,
            )?;
            column_values.push(col_value);
        }

        Ok(Self {
            column_values,
            column_values_json: None,
        })
    }

    pub fn to_json(&mut self) -> Value {
        let mut json_object = serde_json::Map::new();
        for (i, column_value) in self.column_values.iter().enumerate() {
            let column_name = format!("COLUMN_{}", i + 1); // Adjust column naming as needed
            let json_value = match column_value {
                ColumnValue::Json(bytes) => {
                    // Retain JSON as-is using JsonBinary::parse_as_string
                    let json_str =
                        JsonBinary::parse_as_string(bytes).unwrap_or_else(|_| "null".to_string());
                    serde_json::from_str(&json_str).unwrap_or(Value::Null)
                }
                ColumnValue::None => Value::Null,
                ColumnValue::Bit(bit_value) => Value::Bool(*bit_value != 0),
                ColumnValue::String(bytes) | ColumnValue::Blob(bytes) => {
                    // Convert BLOB to hex and VARCHAR to string
                    match std::str::from_utf8(bytes) {
                        Ok(v) => Value::String(v.to_string()),
                        Err(_) => Value::String(vec_u8to_hex_string(bytes)),
                    }
                }
                ColumnValue::Decimal(decimal_value) => Value::String(decimal_value.to_string()),
                ColumnValue::Tiny(int_value) => Value::Number((*int_value).into()),
                ColumnValue::Short(int_value) => Value::Number((*int_value).into()),
                ColumnValue::Long(int_value) => Value::Number((*int_value).into()),
                ColumnValue::LongLong(bigint_value) => Value::Number((*bigint_value).into()),
                ColumnValue::DateTime(date_time) | ColumnValue::Date(date_time) => {
                    Value::String(date_time.to_string())
                }
                ColumnValue::Timestamp(timestamp) => {
                    if let Some(dt) = DateTime::from_timestamp_micros(*timestamp) {
                        if *timestamp % 1_000_000 == 0 {
                            // If no microseconds, format as standard TIMESTAMP
                            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        } else {
                            // If microseconds exist, format as TIMESTAMP(6)
                            Value::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::String(format!("{:?}", column_value)), // Fallback for unsupported types
            };
            json_object.insert(column_name, json_value);
        }
        Value::Object(json_object)
    }

    // Convenience method to store JSON representation
    pub fn with_json(mut self) -> Self {
        self.column_values_json = Some(self.to_json());
        self
    }
}
