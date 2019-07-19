use std::collections::HashMap;

/// KvStore struct implemented with a hashmap
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// Create a KvStore
    pub fn new() -> Self {
        KvStore(HashMap::new())
    }

    /// Get value by key
    pub fn get<S: Into<String>>(&self, key: S) -> Option<String> {
        self.0.get(&key.into()).cloned()
    }

    /// Set value of key
    pub fn set<S: Into<String>, T: Into<String>>(&mut self, key: S, value: T) {
        self.0.insert(key.into(), value.into());
    }

    /// Remove key from store
    pub fn remove<S: Into<String>>(&mut self, key: S) {
        self.0.remove(&key.into());
    }
}