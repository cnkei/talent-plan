/// KvStore error type
#[derive(Debug)]
pub enum KvsError {}

/// KvStore result type
pub type Result<T> = std::result::Result<T, KvsError>;
