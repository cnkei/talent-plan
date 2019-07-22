use std::process;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Opt {
    /// Get value by key from KvStore
    #[structopt(name = "get")]
    Get {
        /// Key to retrieve
        #[structopt(name = "KEY")]
        key: String,
    },
    /// Set key-value to KvStore
    #[structopt(name = "set")]
    Set {
        /// Key to set
        #[structopt(name = "KEY")]
        key: String,
        /// Value to set
        #[structopt(name = "VALUE")]
        value: String,
    },
    /// Remove key from KvStore
    #[structopt(name = "rm")]
    Rm {
        /// Key to remove
        #[structopt(name = "KEY")]
        key: String,
    },
}

fn main() -> kvs::Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::Get { .. } | Opt::Set { .. } | Opt::Rm { .. } => eprintln!("unimplemented"),
    }
    process::exit(-1);
}
