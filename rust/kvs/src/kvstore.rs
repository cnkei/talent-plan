use crate::result;

use std::collections::HashMap;
use std::path::Path;

/// KvStore struct implemented with a hashmap
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// Create a KvStore
    pub fn new() -> Self {
        KvStore(HashMap::new())
    }

    /// Open an index file
    pub fn open<P: AsRef<Path>>(path: P) -> result::Result<KvStore> {
        unimplemented!()
    }

    /// Get value by key
    pub fn get<S: Into<String>>(&self, key: S) -> result::Result<Option<String>> {
        Ok(self.0.get(&key.into()).cloned())
    }

    /// Set value of key
    pub fn set<S: Into<String>, T: Into<String>>(
        &mut self,
        key: S,
        value: T,
    ) -> result::Result<()> {
        self.0.insert(key.into(), value.into());
        Ok(())
    }

    /// Remove key from store
    pub fn remove<S: Into<String>>(&mut self, key: S) -> result::Result<()> {
        self.0.remove(&key.into());
        Ok(())
    }
}
