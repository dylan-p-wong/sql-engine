use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::Chunk;

use super::{Buffer, VECTOR_SIZE_THRESHOLD};

pub struct Limit {
    output_schema: OutputSchema,
    limit: u64,
    child: Box<dyn Executor>,

    buffer: Buffer,
}

impl Limit {
    pub fn new(
        child: Box<dyn Executor>,
        limit: u64,
        output_schema: OutputSchema,
    ) -> Result<Box<Limit>, Error> {
        Ok(Box::new(Limit {
            limit,
            child,
            output_schema,
            buffer: Buffer::new(),
        }))
    }
}

impl Executor for Limit {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        if self.limit == 0 {
            return Ok(Chunk::default());
        }

        while self.buffer.size() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child.next_chunk()?;

            if next_chunk.is_empty() {
                break;
            }

            for row in next_chunk.get_rows().iter() {
                self.buffer.add_row(row.clone());
                self.limit -= 1;
                if self.limit == 0 {
                    break;
                }
            }
            if self.limit == 0 {
                break;
            }
        }

        Ok(self.buffer.get_sized_chunk(VECTOR_SIZE_THRESHOLD))
    }

    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
