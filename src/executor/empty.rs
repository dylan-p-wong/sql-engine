use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::Chunk;
use std::mem::swap;

pub struct Empty {
    buffer: Chunk,
}

impl Empty {
    pub fn new() -> Result<Box<Empty>, Error> {
        let mut buffer = Chunk::new();
        buffer.add_row(vec![]);
        Ok(Box::new(Empty { buffer }))
    }
}

impl Executor for Empty {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        let mut res = Chunk::new();
        swap(&mut res, &mut self.buffer);
        Ok(res)
    }

    fn get_output_schema(&self) -> OutputSchema {
        OutputSchema::new()
    }
}
