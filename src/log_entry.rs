use bincode::{Decode, Encode};

/*
* LogEntry is the basic unit of the log.
* Log Entry Format :
* ksz | key | vsz | value
* u64 | vec<u8> | u64 | vec<u8>
*/
#[derive(Debug, Encode, Decode, Clone)]
pub struct LogEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

