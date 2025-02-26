use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DmlType {
    Insert,
    Update,
    Delete,
}

impl fmt::Display for DmlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DmlType::Insert => write!(f, "INSERT"),
            DmlType::Update => write!(f, "UPDATE"),
            DmlType::Delete => write!(f, "DELETE"),
        }
    }
}

pub trait DmlEvent {
    fn schema_name(&self) -> &str;
    
    fn table_name(&self) -> &str;
    
    fn event_type(&self) -> DmlType;
    
    fn rows_as_json(&self) -> Vec<Value>;
    
    fn row_count(&self) -> usize;
}

impl DmlEvent for super::write_rows_event::WriteRowsEvent {
    fn schema_name(&self) -> &str {
        &self.schema_name
    }
    
    fn table_name(&self) -> &str {
        &self.table_name
    }
    
    fn event_type(&self) -> DmlType {
        DmlType::Insert
    }
    
    fn rows_as_json(&self) -> Vec<Value> {
        self.rows
            .iter()
            .map(|row| row.clone().to_json())
            .collect()
    }
    
    fn row_count(&self) -> usize {
        self.rows.len()
    }
}

impl DmlEvent for super::delete_rows_event::DeleteRowsEvent {
    fn schema_name(&self) -> &str {
        &self.schema_name
    }
    
    fn table_name(&self) -> &str {
        &self.table_name
    }
    
    fn event_type(&self) -> DmlType {
        DmlType::Delete
    }
    
    fn rows_as_json(&self) -> Vec<Value> {
        self.rows
            .iter()
            .map(|row| row.clone().to_json())
            .collect()
    }
    
    fn row_count(&self) -> usize {
        self.rows.len()
    }
}

impl DmlEvent for super::update_rows_event::UpdateRowsEvent {
    fn schema_name(&self) -> &str {
        &self.schema_name
    }
    
    fn table_name(&self) -> &str {
        &self.table_name
    }
    
    fn event_type(&self) -> DmlType {
        DmlType::Update
    }
    
    fn rows_as_json(&self) -> Vec<Value> {
        self.rows
            .iter()
            .map(|(before, after)| {
                Value::Object(
                    [
                        ("before".to_string(), before.clone().to_json()),
                        ("after".to_string(), after.clone().to_json()),
                    ]
                    .into_iter()
                    .collect()
                )
            })
            .collect()
    }
    
    fn row_count(&self) -> usize {
        self.rows.len()
    }
}
