use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::Chunk;

use super::{Buffer, VECTOR_SIZE_THRESHOLD};

pub struct Empty {
    buffer: Buffer,
}

impl Empty {
    pub fn new() -> Result<Box<Empty>, Error> {
        let mut buffer = Buffer::new();
        buffer.add_row(vec![]);
        Ok(Box::new(Empty { buffer }))
    }
}

impl Executor for Empty {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        Ok(self.buffer.get_sized_chunk(VECTOR_SIZE_THRESHOLD))
    }

    fn get_output_schema(&self) -> OutputSchema {
        OutputSchema::new()
    }
}
