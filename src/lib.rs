use std::collections::HashMap;

pub mod cli;

pub struct KvStore {
    store: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            store: HashMap::new()
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

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

    pub fn remove(&mut self, key: String) {
        let _ = &self.store.remove(&key);
    }
}