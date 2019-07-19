#![deny(missing_docs)]

//! Simple in-memory KvStore implementation
//!
//! # Create a KvStore
//!
//! The [`KvStore`] struct is used to create a KvStore
//!
//! ```
//! use kvs::KvStore;
//!
//! let store = KvStore::new();
//! ```
//!
//! # Store operations
//!
//! Supports [`get`], [`set`] and [`remove`]
//!
//! ```
//! use kvs::KvStore;
//!
//! let mut store = KvStore::new();
//!
//! store.set("key", "value");
//! assert_eq!(Some(String::from("value")), store.get("key"));
//!
//! // empty key
//! assert_eq!(None, store.get("no_such_key"));
//!
//! // removed key
//! store.remove(String::from("key"));
//! assert_eq!(None, store.get("key"));
//! ```
//!
//! [`KvStore`]: struct.KvStore.html
//! [`get`]: struct.KvStore.html#method.get
//! [`set`]: struct.KvStore.html#method.set
//! [`remove`]: struct.KvStore.html#method.remove

pub use kvstore::KvStore;

mod kvstore;