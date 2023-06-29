use std::fmt::Error;
use crate::types::{Chunk};
use crate::executor::Executor;

pub struct Empty {}

impl Empty {
    pub fn new() -> Result<Box<Empty>, Error> {
        Ok(Box::new(Empty{}))
    }
}

impl Executor for Empty {
    fn execute(self: Box<Self>) -> Result<Chunk, Error> {
        let mut data_chunks = Vec::new();
        data_chunks.push(Vec::new());
        return Ok(Chunk{headers: Vec::new(), data_chunks: data_chunks});
    }
}
