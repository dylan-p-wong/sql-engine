use std::{cell::OnceCell, mem::swap};

use crate::types::{error::Error, Chunk, Column, Row};

use super::{Executor, VECTOR_SIZE_THRESHOLD};

pub struct NestedLoopJoin {
    output_schema: Vec<Column>,
    child_left: Box<dyn Executor>,
    child_right: Box<dyn Executor>,

    buffer: Chunk,
    right_rows: OnceCell<Result<Vec<Row>, Error>>,
}

impl NestedLoopJoin {
    pub fn new(
        child_left: Box<dyn Executor>,
        child_right: Box<dyn Executor>,
        output_schema: Vec<Column>,
    ) -> Result<Box<NestedLoopJoin>, Error> {
        Ok(Box::new(NestedLoopJoin {
            buffer: Chunk::default(),
            right_rows: OnceCell::new(),
            child_left,
            child_right,
            output_schema,
        }))
    }

    fn get_or_init_right(&mut self) -> Result<Vec<Row>, Error> {
        self.right_rows
            .get_or_init(|| {
                let mut res = Vec::new();
                loop {
                    let chunk = self.child_right.next_chunk()?;
                    if chunk.data_chunks.is_empty() {
                        break;
                    }
                    for row in chunk.data_chunks {
                        res.push(row)
                    }
                }
                Ok(res)
            })
            .clone()
    }
}

impl Executor for NestedLoopJoin {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        let right_rows_result = self.get_or_init_right();
        if right_rows_result.is_err() {
            return Err(right_rows_result.err().unwrap());
        }

        while self.buffer.data_chunks.len() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child_left.next_chunk()?;

            if next_chunk.data_chunks.is_empty() {
                break;
            }

            for left_row in next_chunk.data_chunks {
                for right_row in right_rows_result.as_ref().unwrap().iter() {
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
