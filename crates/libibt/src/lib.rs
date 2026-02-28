pub mod error;
pub mod header;
pub mod reader;
pub mod session_info;
pub mod var_header;

#[cfg(feature = "arrow")]
pub mod channel;

pub use error::{IbtError, Result};
pub use header::{DiskSubHeader, IbtHeader};
pub use reader::IbtFile;
pub use var_header::{VarHeader, VarType};

/// Open and parse an IBT file from disk.
pub fn read_ibt_file<P: AsRef<std::path::Path>>(path: P) -> Result<IbtFile> {
    IbtFile::open(path)
}

/// Parse an IBT file from in-memory bytes.
pub fn read_ibt(data: Vec<u8>) -> Result<IbtFile> {
    IbtFile::from_bytes(data)
}
