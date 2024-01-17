use std::collections::HashMap;

/// Key-value store implementation.
pub struct KvStore {
    /// The store.
    store: HashMap<String, String>
}

impl KvStore {
    /// Creates a new instance of KvStore.
    pub fn new() -> Self {
        Self {
            store: HashMap::new()
        }
    }

    /// Sets a key-value pair in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
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
    pub fn get(&self, key: String) -> Option<String> {
        match self.store.get(&key) {
            Some(val) => {
                Option::Some(val.to_owned())
            }
            None => {
                Option::None
            }
        }
    }

    /// Removes the key-value pair associated with the given key from the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    pub fn remove(&mut self, key: String) {
        let _ = &self.store.remove(&key);
    }
}