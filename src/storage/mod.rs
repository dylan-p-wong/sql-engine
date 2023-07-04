use std::fmt::Error;

use crate::types::Chunk;

pub mod parquet;

pub trait StorageReader {
    fn next_chunk(&mut self) -> Result<Chunk, Error>;
}

pub fn get_table_path(s: &str) -> String {
    if s.starts_with('\'') && s.ends_with('\'') {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}
