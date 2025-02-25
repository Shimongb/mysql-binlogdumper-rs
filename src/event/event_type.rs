use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::str::FromStr;
use std::vec;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, PartialEq)]
#[repr(u8)]
pub enum EventType {
    // refer: https://github.com/mysql/mysql-server/blob/trunk/libs/mysql/binlog/event/binlog_event.h
    #[num_enum(default)]
    Unknown = 0,
    StartV3 = 1,
    Query = 2,
    Stop = 3,
    Rotate = 4,
    Intvar = 5,
    Load = 6,
    Slave = 7,
    CreateFile = 8,
    AppendBlock = 9,
    ExecLoad = 10,
    DeleteFile = 11,
    NewLoad = 12,
    Rand = 13,
    UserVar = 14,
    FormatDescription = 15,
    Xid = 16,
    BeginLoadQuery = 17,
    ExecuteLoadQuery = 18,
    TableMap = 19,
    PreGaWriteRows = 20,
    PreGaUpdateRows = 21,
    PreGaDeleteRows = 22,
    WriteRows = 23,
    UpdateRows = 24,
    DeleteRows = 25,
    Incident = 26,
    HeartBeat = 27,
    Ignorable = 28,
    RowsQuery = 29,
    ExtWriteRows = 30,
    ExtUpdateRows = 31,
    ExtDeleteRows = 32,
    Gtid = 33,
    AnonymousGtid = 34,
    PreviousGtids = 35,
    TransactionContext = 36,
    ViewChange = 37,
    XaPrepare = 38,
    PartialUpdateRowsEvent = 39,
    TransactionPayload = 40,
    AnnotateRows = 160,
    BinlogCheckpoint = 161,
    MariadbGtid = 162,
    MariadbGtidList = 163,
}

impl EventType {
    pub fn from_code(code: u8) -> EventType {
        if let Ok(res) = EventType::try_from(code) {
            return res;
        }
        EventType::Unknown
    }

    pub fn to_code(event_type: EventType) -> u8 {
        event_type.into()
    }
    
    pub fn get_event_types_from_str(event_types_str: &String) -> Option<Vec<EventType>> {
        if event_types_str.is_empty() {
            None
        } else {
            let mut event_type_list = vec![EventType::Rotate];
            event_type_list.extend(event_types_str
                .split(',')
                .map(|s| s.trim().to_string().to_ascii_uppercase())
                .map(|s| EventType::from_str(&s).unwrap())
                .collect::<Vec<EventType>>());
            
            Some(event_type_list)
        }
    }
}

impl FromStr for EventType {
    type Err = ();

    fn from_str(input: &str) -> Result<EventType, Self::Err> {
        match input {
            "UNKNOWN" => Ok(EventType::Unknown),
            "STARTV3" => Ok(EventType::StartV3),
            "QUERY" => Ok(EventType::Query),
            "STOP" => Ok(EventType::Stop),
            "ROTATE" => Ok(EventType::Rotate),
            "INTVAR" => Ok(EventType::Intvar),
            "LOAD" => Ok(EventType::Load),
            "SLAVE" => Ok(EventType::Slave),
            "CREATEFILE" => Ok(EventType::CreateFile),
            "APPENDBLOCK" => Ok(EventType::AppendBlock),
            "EXECLOAD" => Ok(EventType::ExecLoad),
            "DELETEFILE" => Ok(EventType::DeleteFile),
            "NEWLOAD" => Ok(EventType::NewLoad),
            "RAND" => Ok(EventType::Rand),
            "USERVAR" => Ok(EventType::UserVar),
            "FORMATDESCRIPTION" => Ok(EventType::FormatDescription),
            "XID" => Ok(EventType::Xid),
            "BEGINLOADQUERY" => Ok(EventType::BeginLoadQuery),
            "EXECUTELOADQUERY" => Ok(EventType::ExecuteLoadQuery),
            "TABLEMAP" => Ok(EventType::TableMap),
            "PREGAWRITEROWS" => Ok(EventType::PreGaWriteRows),
            "PREGAUPDATEROWS" => Ok(EventType::PreGaUpdateRows),
            "PREGADELETEROWS" => Ok(EventType::PreGaDeleteRows),
            "WRITEROWS" => Ok(EventType::WriteRows),
            "UPDATEROWS" => Ok(EventType::UpdateRows),
            "DELETEROWS" => Ok(EventType::DeleteRows),
            "INCIDENT" => Ok(EventType::Incident),
            "HEARTBEAT" => Ok(EventType::HeartBeat),
            "IGNORABLE" => Ok(EventType::Ignorable),
            "ROWSQUERY" => Ok(EventType::RowsQuery),
            "EXTWRITEROWS" => Ok(EventType::ExtWriteRows),
            "EXTUPDATEROWS" => Ok(EventType::ExtUpdateRows),
            "EXTDELETEROWS" => Ok(EventType::ExtDeleteRows),
            "GTID" => Ok(EventType::Gtid),
            "ANONYMOUSGTID" => Ok(EventType::AnonymousGtid),
            "PREVIOUSGTIDS" => Ok(EventType::PreviousGtids),
            "TRANSACTIONCONTEXT" => Ok(EventType::TransactionContext),
            "VIEWCHANGE" => Ok(EventType::ViewChange),
            "XAPREPARE" => Ok(EventType::XaPrepare),
            "PARTIALUPDATEROWSEVENT" => Ok(EventType::PartialUpdateRowsEvent),
            "TRANSACTIONPAYLOAD" => Ok(EventType::TransactionPayload),
            "ANNOTATEROWS" => Ok(EventType::AnnotateRows),
            "BINLOGCHECKPOINT" => Ok(EventType::BinlogCheckpoint),
            "MARIADBGTID" => Ok(EventType::MariadbGtid),
            "MARIADBGTIDLIST" => Ok(EventType::MariadbGtidList),
            _ => Ok(EventType::Unknown),
        }
    }
}
