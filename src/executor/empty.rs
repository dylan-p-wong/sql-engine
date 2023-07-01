use std::fmt::Error;
use std::mem::swap;
use crate::types::{Chunk};
use crate::executor::Executor;

pub struct Empty {
    buffer : Chunk,
}

impl Empty {
    pub fn new() -> Result<Box<Empty>, Error> {
        Ok(Box::new(Empty{
            buffer: Chunk{ data_chunks: vec![vec![]] }},
        ))
    }
}

impl Executor for Empty {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        let mut res_chunks = Vec::new();
        swap(&mut res_chunks, &mut self.buffer.data_chunks);
        return Ok(Chunk { data_chunks: res_chunks });
    }

    fn get_output_schema(&self) -> Vec<crate::types::Column> {
        vec![]
    }
}
