use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;

use anyhow::__private::kind::TraitKind;
use anyhow::anyhow;

use crate::LogEntry;
use crate::Result;

#[derive(Debug)]
pub struct DataFile {
    pub id: String,
    path: PathBuf,
    reader: DataFileReader,
    writer: DataFileWriter,
    inner: File,
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
        let inner = File::create(&path)?;
        let reader = DataFileReader::new(&path)?;
        let writer = DataFileWriter::new(&path)?;
        Ok(DataFile {
            id: file_name,
            path,
            reader,
            writer,
            inner,
        })
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        self.inner.sync_all().unwrap();
        self.writer.sync().unwrap();
    }
}

impl DataFile {
    // Write key value to datafile and return the offset of value
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        self.writer.append(key, value)
    }

    pub fn read(&self, value_offset: u64, value_size: u64) -> Result<Vec<u8>> {
        self.reader.read(value_offset, value_size)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn read_all(&self) -> Result<Vec<LogEntry>> {
        self.reader.read_all()
    }

    pub fn compact(&mut self) -> Result<()> {
        let mut map: HashMap<Vec<u8>, LogEntry> = HashMap::new();
        let itr = DataFileIterator::new(&self.path)?;
        for le in itr {
            if le.value.len() > 0 {
                map.insert(le.key.to_owned(), le.into());
            } else {
                map.remove(&le.key);
            }
        }
        std::fs::remove_file(&self.path)?;
        let f = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(self.path.clone())?;
        let mut writer = BufWriter::new(&f);
        for (_, val) in map {
            let v = bincode::encode_to_vec(
                val, bincode::config::standard().with_fixed_int_encoding()).unwrap();
            writer.write(&v).unwrap();
        }
        drop(writer);
        self.inner = f;
        Ok(())
    }
}

fn calculate_key_offset(offset: u64, _le: &LogEntry) -> u64 {
    return offset + 8;
}

fn calculate_value_offset(offset: u64, le: &LogEntry) -> u64 {
    return offset + 16 + le.key_size();
}

#[derive(Debug)]
pub struct LogReadResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub key_offset: u64,
    pub value_offset: u64,
}

impl Into<LogEntry> for LogReadResult {
    fn into(self) -> LogEntry {
        LogEntry {
            key: self.key,
            value: self.value,
        }
    }
}

pub struct DataFileIterator {
    inner: BufReader<File>,
    offset: u64,
}

impl DataFileIterator {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let f = File::options()
            .read(true)
            .open(path)?;
        let reader = BufReader::new(f);
        Ok(DataFileIterator {
            inner: reader,
            offset: 0,
        })
    }
}

impl Iterator for DataFileIterator {
    type Item = LogReadResult;

    fn next(&mut self) -> Option<Self::Item> {
        let res: std::result::Result<LogEntry, bincode::error::DecodeError>
            = bincode::decode_from_reader(&mut self.inner,
                                          bincode::config::standard()
                                              .with_fixed_int_encoding());
        match res {
            Ok(le) => {
                let key_offset = calculate_key_offset(self.offset, &le);
                let value_offset = calculate_value_offset(self.offset, &le);
                // Update offset
                self.offset += le.size();
                Some(LogReadResult {
                    key: le.key,
                    value: le.value,
                    key_offset,
                    value_offset,
                })
            }
            Err(_e) => {
                // FIXME
                None
            }
        }
    }
}

#[derive(Debug)]
struct DataFileReader {
    inner: File,
}

impl DataFileReader {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let mut f = File::options()
            .read(true)
            .open(path)?;
        Ok(DataFileReader {
            inner: f
        })
    }
    pub fn read(&self, value_offset: u64, value_size: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; value_size as usize];
        let bytes_read = self.inner.read_at(&mut buf, value_offset)?;
        if bytes_read != value_size as usize {
            return Err(anyhow!("incomplete read"));
        }
        Ok(buf)
    }

    // FIXME: Fix the offset or add docs around offset
    pub fn read_all(&self) -> Result<Vec<LogEntry>> {
        let mut r: Vec<LogEntry> = Vec::new();
        let mut reader = BufReader::new(&self.inner);
        loop {
            let res: std::result::Result<LogEntry, bincode::error::DecodeError>
                = bincode::decode_from_reader(&mut reader,
                                              bincode::config::standard()
                                                  .with_fixed_int_encoding());
            match res {
                Ok(entry) => {
                    r.push(entry)
                }
                Err(_e) => {
                    //FIXME
                    break;
                }
            }
        }
        Ok(r)
    }
}

#[derive(Debug)]
struct DataFileWriter {
    inner: File,
    offset: u64,
    byte_written: u64,
}

impl DataFileWriter {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let mut f = File::options()
            .append(true)
            .open(path)?;
        Ok(DataFileWriter {
            inner: f,
            offset: 0,
            byte_written: 0,
        })
    }

    pub fn append(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let entry = LogEntry {
            key,
            value,
        };
        let value_offset = calculate_value_offset(self.offset, &entry);
        let bin = bincode::encode_to_vec(&entry,
                                         bincode::config::standard()
                                             .with_fixed_int_encoding())?;
        let bytes_written = self.inner.write(&bin)?;
        if bytes_written != bin.len() {
            return Err(anyhow!("incomplete write"));
        }
        self.byte_written += bytes_written as u64;
        self.offset += bytes_written as u64;
        Ok(value_offset)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
}


mod tests {
    use std::collections::HashMap;

    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use tempfile::TempDir;

    use super::*;

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
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path().to_owned();
        let df = DataFile::open(temp_dir_path.clone());
        assert_eq!(df.is_err(), false);
    }

    #[test]
    fn test_data_write() {
        // Create temp directory
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path().to_owned();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        let value_offset = df.write(
            "key".as_bytes().to_vec(),
            "value".as_bytes().to_vec(),
        );
        assert_eq!(value_offset.is_err(), false);
        assert_eq!(value_offset.unwrap(), 19);
    }

    #[test]
    fn test_bulk_write() {
        // Create temp directory
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path().to_owned();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
        }
    }

    #[test]
    fn test_data_read() {
        // Create temp directory
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path().to_owned();
        let mut df = DataFile::open(temp_dir_path.clone()).unwrap();
        let key = rand_key();
        let value = rand_value();
        let value_sz = value.len() as u64;
        let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        assert_eq!(res.is_ok(), true);
        // Capture value
        let value_offset = res.unwrap();
        // Test for read
        let res = df.read(value_offset, value_sz);
        assert_eq!(res.is_ok(), true);
        let buf = res.unwrap();
        assert_eq!(value.as_bytes().to_vec(), buf);
    }

    #[test]
    fn test_bulk_read() {
        let mut key_value_map: HashMap<String, (u64, String)> = HashMap::new();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut df = DataFile::open(temp_dir.path().to_owned()).unwrap();
        for _ in 1..=1000 {
            let key = rand_key();
            let value = rand_value();
            let res = df.write(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            assert_eq!(res.is_ok(), true);
            key_value_map.insert(key, (res.unwrap(), value));
        }
        for (_key, (offset, value)) in key_value_map.iter() {
            let value_bytes = value.as_bytes().to_vec();
            let res = df.read(offset.clone(), value_bytes.len() as u64);
            assert_eq!(res.is_ok(), true);
            let buf = res.unwrap();
            assert_eq!(buf, value_bytes);
        }
    }

    #[test]
    fn test_datafile_iterator() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut datafile = DataFile::open(temp_dir.path().to_owned()).unwrap();
        let datafile_path = datafile.path().to_owned();
        let keys = vec!["k1", "k2", "k3"];
        let values = vec!["v1", "v2", "v3"];
        for i in 0..3 {
            let key = keys[i].as_bytes().to_vec();
            let value = values[i].as_bytes().to_vec();
            assert_eq!(datafile.write(key, value).is_ok(), true);
        }
        drop(datafile);
        let datafile_itr = DataFileIterator::new(&datafile_path).unwrap();
        let mut count = 0;
        for r in datafile_itr {
            assert_eq!(r.key, keys[count].as_bytes().to_vec());
            assert_eq!(r.value, values[count].as_bytes().to_vec());
            count += 1
        }
        assert_eq!(count, 3);
    }
}