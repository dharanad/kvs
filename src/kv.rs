use std::{collections::HashMap, path::Path};

use anyhow::Ok;

/// KvStore custom error
pub type Result<T> = anyhow::Result<T>;

/// Key-value store implementation.
pub struct KvStore {
    /// The store.
    store: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new instance of KvStore.
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Opens a KvStore at the given path.
    pub fn open(path: &Path) -> Result<KvStore> {
        panic!("unimplemented")
    }

    /// Sets a key-value pair in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
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
        Ok(self.store.get(&key).map(|val| val.to_owned()))
    }

    /// Removes the key-value pair associated with the given key from the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let _ = &self.store.remove(&key);
        Ok(())
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
