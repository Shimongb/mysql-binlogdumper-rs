use serde::{Deserialize, Serialize};

use super::{
    format_description_event::FormatDescriptionEvent,
    rotate_event::RotateEvent, table_map_event::TableMapEvent,
    transaction_payload_event::TransactionPayloadEvent,
    gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent,
    query_event::QueryEvent, rows_query_event::RowsQueryEvent,
    dml_event::DmlEvent, delete_rows_event::DeleteRowsEvent,
    update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
    xa_prepare_event::XaPrepareEvent, xid_event::XidEvent,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum EventData {
    NotSupported,
    FormatDescription(FormatDescriptionEvent),
    PreviousGtids(PreviousGtidsEvent),
    Gtid(GtidEvent),
    Query(QueryEvent),
    TableMap(TableMapEvent),
    WriteRows(WriteRowsEvent),
    UpdateRows(UpdateRowsEvent),
    DeleteRows(DeleteRowsEvent),
    Xid(XidEvent),
    XaPrepare(XaPrepareEvent),
    Rotate(RotateEvent),
    TransactionPayload(TransactionPayloadEvent),
    RowsQuery(RowsQueryEvent),
    HeartBeat,
}

impl EventData {
    pub fn as_dml_event(&self) -> Option<(&dyn DmlEvent, String)> {
        match self {
            Self::WriteRows(event) => Some((event as &dyn DmlEvent, event.event_type().to_string())),
            Self::DeleteRows(event) => Some((event as &dyn DmlEvent, event.event_type().to_string())),
            Self::UpdateRows(event) => Some((event as &dyn DmlEvent, event.event_type().to_string())),
            _ => None,
        }
    }
}
