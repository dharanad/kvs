use bincode::{Decode, Encode};

/*
* LogEntry is the basic unit of the log.
* Max Value Size : 256KB -> 262144 Bytes
* Max Key Size : 64 Bytes
* Max Log Entry Size : 262208 Bytes
* Log Entry Format :
* ksz | key | vsz | value
* u64 | vec<u8> | u64 | vec<u8>
* u64 -> 8 bytes
*/
#[derive(Debug, Encode, Decode)]
pub struct LogEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

