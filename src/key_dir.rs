use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Entry {
    pub file_id: String,
    pub value_offset: u64,
    pub value_sz: u64,
}

#[derive(Debug)]
pub struct KeyDir {
    inner: HashMap<String, Entry>
}

impl KeyDir {

    pub fn new() -> Self {
        Self {
            inner: HashMap::new()
        }
    }
    pub fn put(&mut self, file_id: String, key: String, value_offset: u64, value_sz: u64) {
        let e = Entry {
            file_id,
            value_sz,
            value_offset
        };
        let hm = &mut self.inner;
        hm.insert(key, e);
    }

    pub fn get(&self, key: &str) -> Option<Entry> {
        self.inner.get(key).map(|e|  e.clone())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }
}