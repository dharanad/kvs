use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, Ok};

use crate::datafile::{DataFile, DataFileIterator};
use crate::index::KeyDir;
use crate::log_entry::LogEntry;
use crate::Result;

/// Key-value store implementation.
pub struct KvStore {
    path: PathBuf,
    active_datafile: DataFile,
    // FIXME: Scan for data file in path
    old_datafiles: Vec<DataFile>,
    key_dir: KeyDir
}

impl KvStore {

    /// Opens a KvStore at the given path.
    pub fn open(path: &Path) -> Result<KvStore> {
        let df = DataFile::open(path.to_owned()).unwrap();
        let mut key_dir = KeyDir::new();
        Self::init_index(&df, &mut key_dir);
        Ok(Self {
            active_datafile: df,
            path: path.to_owned(),
            old_datafiles: Vec::new(),
            key_dir
        })
    }

    /// Sets a key-value pair in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if value.is_empty() {
            return Err(anyhow!("value cannot be empty"));
        }
        // FIXME: Move compaction to background thread
        if self.active_datafile.size()? > 10 * 1024 /* 10KB */ {
            self.active_datafile.compact()?;
            Self::init_index(&self.active_datafile, &mut self.key_dir);
        }
        // FIXME Keep track of file size. Then Check and run compaction here
        self._key(key, value)
    }

    fn _key(&mut self, key: String, value: String) -> Result<()> {
        let key_bytes = key.as_bytes().to_vec();
        let value_bytes = value.as_bytes().to_vec();
        let value_sz = value_bytes.len() as u64;
        // Write the key value entry to datafile
        let value_offset = self.active_datafile.write(key_bytes, value_bytes)?;
        let file_id = self.active_datafile.id.to_owned();
        // FIXME: Below line add side effects to this method
        // We should move that away
        // Update key dir
        if value_sz > 0 {
            let _ = &self.key_dir.put(file_id, key, value_offset, value_sz);
        }
        Ok(())
    }

    /// Retrieves the value associated with the given key from the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    ///
    /// # Returns
    ///
    /// The value associated with the key, if it exists.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if !self.key_dir.contains_key(&key) {
            return Ok(None)
        }
        // get key metadata from key dir
        let e = self.key_dir.get(&key).unwrap();
        let read_op = self.active_datafile.read(e.value_offset, e.value_sz)?;
        Ok(Some(std::str::from_utf8(&read_op).unwrap().to_string()))
    }

    /// Removes the key-value pair associated with the given key from the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_dir.contains_key(&key) {
            let _ = self.key_dir.remove_key(&key).is_some();
            return self._key(key.clone(), "".to_string())
        }
        return Err(anyhow!("Key not found"))
    }


    /// Initializes the index
    fn init_index(datafile: &DataFile, key_dir: &mut KeyDir) {
        let reader = DataFileIterator::new(datafile.path()).unwrap();
        let file_id = datafile.id.to_owned();
        for res in reader {
            let key = std::str::from_utf8(&res.key).unwrap().to_string();
            let value_offset = res.value_offset;
            let le: LogEntry = res.into();
            let value_sz = le.value_size();
            if value_sz > 0 {
                key_dir.put(
                    file_id.to_owned(),
                    key.clone(),
                    value_offset,
                    value_sz
                );
            } else { // value_sz == 0 represent a deleted key
                if key_dir.contains_key(&key) {
                    key_dir.remove_key(&key);
                }
            }
        }
    }
}
