use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use anyhow::__private::kind::TraitKind;

use anyhow::anyhow;
use crate::key_dir::KeyDir;

use crate::LogEntry;
use crate::Result;

#[derive(Debug)]
pub struct DataFile {
    inner: File,
    pub id: String,
    offset: u64,
    is_mutable: bool,
}

impl DataFile {
    /// Create a new Datafile, fail is already exist
    pub fn open(path: PathBuf) -> Result<DataFile> {
        // Validate if path is a directory
        if !path.is_dir() {
            return Err(anyhow::anyhow!("Path is not a directory"));
        }
        // FIXME: Change file name timestamp-rand_str.dat
        let file_name = "main.dat".to_string();
        let path = path.join(&file_name);
        let mut f = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let offset = f.seek(SeekFrom::End(0))?;
        Ok(DataFile {
            inner: f,
            id: file_name,
            offset,
            is_mutable: true,
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
        // Move offset to last written pos
        self.inner.seek(SeekFrom::Start(self.offset))?;
        let value_offset = self.offset + 16 + ksz;
        let bin = bincode::encode_to_vec(&entry,
                                         bincode::config::standard()
                                             .with_fixed_int_encoding())?;
        let bytes_written = self.inner.write(&bin)?;
        if bytes_written != bin.len() {
            return Err(anyhow!("incomplete write"));
        }
        self.offset += bin.len() as u64;
        Ok(value_offset)
    }

    pub fn read(&self, value_offset: u64, value_size: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; value_size as usize];
        let bytes_read = self.inner.read_at(&mut buf, value_offset)?;
        if bytes_read != value_size as usize {
            return Err(anyhow!("incomplete read"));
        }
        Ok(buf)
    }

    pub fn update_key_dir(&self, key_dir: &mut KeyDir) {
        let mut reader = BufReader::new(&self.inner);
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut offset = 0u64;
        let file_id = self.id.to_owned();
        loop {
            let res: std::result::Result<LogEntry, bincode::error::DecodeError> = bincode::decode_from_reader(&mut reader,
                                                    bincode::config::standard()
                                                        .with_fixed_int_encoding());
            match res {
                Ok(entry) => {
                    let key = std::str::from_utf8(&entry.key).unwrap().to_string();
                    let key_sz = entry.key.len() as u64;
                    let value_sz = entry.value.len() as u64;
                    let value_offset = offset + 16 + key_sz;
                    // value_sz == 0 represent a deleted key
                    if value_sz > 0 {
                        key_dir.put(
                            file_id.to_owned(),
                            key.clone(),
                            value_offset,
                            value_sz
                        );
                    }
                    if value_sz == 0 && key_dir.contains_key(&key) {
                        key_dir.remove_key(&key);
                    }
                    offset += 16 + key_sz + value_sz;
                }
                Err(e) => {
                    //FIXME
                    break;
                }
            }
        }
    }

    pub fn close(&mut self) -> Result<()> {
        self.inner.sync_data()?;
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
    #[ignore]
    // Test for creating a new data file
    fn test_data_file_new() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let df = DataFile::open(temp_dir_path.clone());
        assert_eq!(df.is_err(), false);
        // delete directory
        delete_dir(temp_dir_path);
    }

    #[test]
    #[ignore]
    fn test_data_write() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        let value_offset = df.write(
            "key".as_bytes().to_vec(),
            "value".as_bytes().to_vec(),
        );
        assert_eq!(value_offset.is_err(), false);
        assert_eq!(value_offset.unwrap(), 19);
        delete_dir(temp_dir_path)
    }

    #[test]
    #[ignore]
    fn test_bulk_write() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
        }
        delete_dir(temp_dir_path);
    }

    #[test]
    #[ignore]
    fn test_data_read() {
        // Create temp directory
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        let key = rand_key();
        let value = rand_value();
        let value_sz = value.len() as u64;
        let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        assert_eq!(res.is_ok(), true);
        // Capture value
        let value_offset = res.unwrap();
        assert_eq!(df.close().is_ok(), true);
        // Test for read
        let res = df.read(value_offset, value_sz);
        assert_eq!(res.is_ok(), true);
        let buf = res.unwrap();
        assert_eq!(value.as_bytes().to_vec(), buf);
        delete_dir(temp_dir_path)
    }

    #[test]
    #[ignore]
    fn test_bulk_read() {
        let mut key_value_map: HashMap<String, (u64, String)> = HashMap::new();
        let temp_dir_path = create_tmp_dir();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
            key_value_map.insert(key, (res.unwrap(), value));
        }
        assert_eq!(df.close().is_ok(), true);
        for (_key, (offset, value)) in key_value_map.iter() {
            let value_bytes = value.as_bytes().to_vec();
            let res = df.read(offset.clone(), value_bytes.len() as u64);
            assert_eq!(res.is_ok(), true);
            let buf = res.unwrap();
            assert_eq!(buf, value_bytes);
        }
        delete_dir(temp_dir_path);
    }
}