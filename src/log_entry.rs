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

impl LogEntry {
    pub fn size(&self) -> u64 {
        return 16 + self.key_size() + self.value_size();
    }

    pub fn key_size(&self) -> u64 {
        return self.key.len() as u64
    }

    pub fn value_size(&self) -> u64 {
        return self.value.len() as u64
    }
}

