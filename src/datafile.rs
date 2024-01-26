use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::anyhow;

use crate::LogEntry;
use crate::Result;

#[derive(Debug)]
struct DataFile {
    f: File,
    // FIX: Search for a better type
    id: String,
    offset: u64,
}

impl DataFile {
    pub fn new(path: PathBuf) -> Result<DataFile> {
        // Validate if path is a directory
        if !path.is_dir() {
            return Err(anyhow::anyhow!("Path is not a directory"));
        }
        // current timestamp
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis()
            .to_string();
        let file_name = format!("{}.dat", now);
        let path = path.join(file_name);
        // Create file
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(DataFile {
            f,
            id: now,
            offset: 0,
        })
    }
}

impl DataFile {
    // Write key value to datafile and return the offset of value
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let ksz = key.len() as u64;
        let entry = LogEntry {
            key,
            value,
        };
        let value_offset = self.offset + 16 + ksz;
        let bin = bincode::encode_to_vec(&entry,
                                         bincode::config::standard()
                                             .with_fixed_int_encoding())?;
        let bytes_written = self.f.write(&bin)?;
        if bytes_written != bin.len() {
            return Err(anyhow!("incomplete write"));
        }
        self.offset += bin.len() as u64;
        Ok(value_offset)
    }

    pub fn read(&mut self, value_offset: u64, value_size: u64) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; value_size as usize];
        self.f.seek(SeekFrom::Start(value_offset))?;
        let bytes_read = self.f.read(&mut buf)?;
        if bytes_read != value_size as usize {
            return Err(anyhow!("incomplete read"));
        }
        Ok(Some(buf))
    }
}

mod tests {
    use std::path::PathBuf;

    use crate::datafile::DataFile;

    fn create_tmp_dir() -> PathBuf {
        // Create directory if not exists
        match std::fs::create_dir("./tmp") {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    panic!("Error creating directory {:?}", e);
                }
            }
        }
        let path = PathBuf::from("./tmp");
        path
    }

    fn delete_dir(path: PathBuf) {
        // delete directory
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_data_file() {
        use super::*;
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let df = DataFile::new(temp_dir_path.clone()).unwrap();
        println!("{:?}", df);
        // delete directory
        delete_dir(temp_dir_path);
    }

    #[test]
    fn test_data_write() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        let value_offset = df.write(
            "dharan".as_bytes().to_vec(),
            "aditya".as_bytes().to_vec(),
        ).unwrap();
        println!("{}", value_offset);
        delete_dir(temp_dir_path)
    }

    #[test]
    fn test_data_read() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        let value_offset = df.write(
            "key".as_bytes().to_vec(),
            "value".as_bytes().to_vec(),
        ).unwrap();
        println!("{}", value_offset);
        let value = df.read(value_offset, 5).unwrap();
        match value {
            Some(v) => {
                print!("{:?}", v);
            }
            None => {
                panic!("unimplemented")
            }
        }
        delete_dir(temp_dir_path)
    }
}