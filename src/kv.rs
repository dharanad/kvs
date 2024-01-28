use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, Ok};

use crate::datafile::DataFile;
use crate::index::KeyDir;
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
    /// Creates a new instance of KvStore.
    pub fn new() -> Self {
        // FIXME: Should we keep this method
        match std::fs::create_dir_all("./db/data") {
            std::prelude::rust_2015::Ok(_) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    panic!("Error creating directory {:?}", e);
                }
            }
        };
        let mut df = DataFile::open(PathBuf::from("./db/data")).unwrap();
        let mut key_dir = KeyDir::new();
        // FIXME
        df.update_key_dir(&mut key_dir);
        Self {
            active_datafile: df,
            path: PathBuf::from("./db"),
            old_datafiles: Vec::new(),
            key_dir,
        }
    }

    /// Opens a KvStore at the given path.
    pub fn open(path: &Path) -> Result<KvStore> {
        let df = DataFile::open(path.to_owned()).unwrap();
        let mut key_dir = KeyDir::new();
        // FIXME
        df.update_key_dir(&mut key_dir);
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
        // FIXME Check and run compaction here
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
        &self.key_dir.put(file_id, key, value_offset, value_sz);
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
        // FIXME: Find file with file_id
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
            let res = self._key(key.clone(), "".to_string());
            let _ = self.key_dir.remove_key(&key).is_some();
            return res;
        }
        return Err(anyhow!("Key not found"))
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
