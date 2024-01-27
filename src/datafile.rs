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
    name: String,
    offset: u64,
    is_mutable: bool,
}

impl DataFile {
    /// Create a new Datafile, fail is already exist
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
        let path = path.join(&file_name);
        // Create file
        let f = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(DataFile {
            f,
            name: file_name,
            offset: 0,
            is_mutable: true,
        })
    }

    pub fn open() -> Result<DataFile> {
        unimplemented!()
    }
}

impl DataFile {
    // Write key value to datafile and return the offset of value
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        assert_eq!(self.is_mutable, true);
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

    pub fn read(&mut self, value_offset: u64, value_size: u64) -> Result<(u64,Vec<u8>)> {
        assert_eq!(self.is_mutable, false);
        let mut buf = vec![0u8; value_size as usize];
        self.f.seek(SeekFrom::Start(value_offset))?;
        let bytes_read = self.f.read(&mut buf)?;
        if bytes_read != value_size as usize {
            return Err(anyhow!("incomplete read"));
        }
        Ok((bytes_read as u64, buf))
    }

    pub fn close(&mut self) -> Result<()> {
        self.f.sync_data()?;
        self.is_mutable = false;
        Ok(())
    }
}

mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use rand::distributions::Alphanumeric;
    use rand::Rng;

    use super::*;

    // Helper function to create a temp directory
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

    // Helper function to delete directory
    fn delete_dir(path: PathBuf) {
        // delete directory
        std::fs::remove_dir_all(path).unwrap();
    }

    fn rand_string(size: usize) -> String {
        let s = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect();
        return s;
    }

    fn rand_key() -> String {
        let rand_ksz = rand::thread_rng().gen_range(10..20) as usize;
        return rand_string(rand_ksz);
    }
    fn rand_value() -> String {
        let rand_ksz = rand::thread_rng().gen_range(10..200) as usize;
        return rand_string(rand_ksz);
    }

    #[test]
    // Test for creating a new data file
    fn test_data_file_new() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let df = DataFile::new(temp_dir_path.clone());
        assert_eq!(df.is_err(), false);
        // delete directory
        delete_dir(temp_dir_path);
    }

    #[test]
    fn test_data_write() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        let value_offset = df.write(
            "key".as_bytes().to_vec(),
            "value".as_bytes().to_vec(),
        );
        assert_eq!(value_offset.is_err(), false);
        assert_eq!(value_offset.unwrap(), 19);
        delete_dir(temp_dir_path)
    }

    #[test]
    fn test_bulk_write() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
        }
        delete_dir(temp_dir_path);
    }

    #[test]
    fn test_data_read() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        let key = rand_key();
        let value = rand_value();
        let value_sz =  value.len() as u64;
        let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        assert_eq!(res.is_ok(), true);
        // Capture value
        let value_offset = res.unwrap();
        assert_eq!(df.close().is_ok(), true);
        // Test for read
        let res = df.read(value_offset, value_sz);
        assert_eq!(res.is_ok(), true);
        let (bytes_read, buf) = res.unwrap();
        assert_eq!(value_sz, bytes_read);
        assert_eq!(value.as_bytes().to_vec(), buf);
        delete_dir(temp_dir_path)
    }

    #[test]
    fn test_bulk_read() {
        let mut key_value_map: HashMap<String, (u64, String)>  = HashMap::new();
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::new(temp_dir_path.clone()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
            key_value_map.insert(key, (res.unwrap(), value));
        }
        assert_eq!(df.close().is_ok(), true);
        for (key, (offset, value)) in key_value_map.iter() {
            let value_bytes = value.as_bytes().to_vec();
            let res = df.read(offset.clone(), value_bytes.len() as u64);
            assert_eq!(res.is_ok(), true);
            let (bytes_read, buf) = res.unwrap();
            assert_eq!(bytes_read, value_bytes.len() as u64);
            assert_eq!(buf, value_bytes);
        }
        delete_dir(temp_dir_path);
    }
}