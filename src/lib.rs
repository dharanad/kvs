pub mod cli;

pub struct KvStore {

}

impl KvStore {
    pub fn new() -> Self {
        Self {}
    }

    pub fn set(&mut self, key: String, value: String) {
        panic!("unimplemented")
    }

    pub fn get(&self, key: String) -> Option<String> {
        panic!("unimplemented")
    }

    pub fn remove(&mut self, key: String) {
        panic!("unimplemented")
    }
}