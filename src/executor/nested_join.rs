use std::{mem::swap};

use crate::types::{error::Error, Chunk, Column, Row};

use super::{Executor, VECTOR_SIZE_THRESHOLD};

pub struct NestedLoopJoin {
    output_schema: Vec<Column>,
    child_left: Box<dyn Executor>,
    child_right: Box<dyn Executor>,

    buffer: Chunk,
    right_rows: Option<Vec<Row>>,
}

impl NestedLoopJoin {
    pub fn new(
        child_left: Box<dyn Executor>,
        child_right: Box<dyn Executor>,
        output_schema: Vec<Column>,
    ) -> Result<Box<NestedLoopJoin>, Error> {
        Ok(Box::new(NestedLoopJoin {
            buffer: Chunk::default(),
            right_rows: None,
            child_left,
            child_right,
            output_schema,
        }))
    }

    fn init_right_rows(&mut self) -> Result<(), Error> {
        let mut res = Vec::new();
        if self.right_rows.is_none() {
            loop {
                let chunk = self.child_right.next_chunk()?;
                if chunk.data_chunks.is_empty() {
                    break;
                }
                for row in chunk.data_chunks {
                    res.push(row)
                }
            }
        }
        self.right_rows = Some(res);
        Ok(())
    }
}

impl Executor for NestedLoopJoin {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        self.init_right_rows()?;

        while self.buffer.data_chunks.len() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child_left.next_chunk()?;

            if next_chunk.data_chunks.is_empty() {
                break;
            }

            for left_row in next_chunk.data_chunks {
                for right_row in self.right_rows.as_ref().unwrap().iter() {
                    // TODO: add join condition here
                    let mut new_row = left_row.clone();
                    new_row.append(&mut right_row.clone());
                    self.buffer.data_chunks.push(new_row);
                }
            }
        }

        if self.buffer.data_chunks.is_empty() {
            return Ok(Chunk::default());
        }

        let mut res_chunks = Vec::new();
        swap(&mut res_chunks, &mut self.buffer.data_chunks);
        Ok(Chunk {
            data_chunks: res_chunks,
        })
    }
    fn get_output_schema(&self) -> Vec<Column> {
        self.output_schema.clone()
    }
}
