use crate::executor::expression::ExprEvaluator;
use crate::executor::Executor;
use crate::types::{Chunk, Column, TupleValue};
use sqlparser::ast::Expr;
use std::fmt::Error;
use std::mem::swap;

use super::VECTOR_SIZE_THRESHOLD;

pub struct Filter {
    output_schema: Vec<Column>,
    filter: Expr,
    child: Box<dyn Executor>,

    buffer: Chunk,
}

impl Filter {
    pub fn new(
        child: Box<dyn Executor>,
        filter: Expr,
        output_schema: Vec<Column>,
    ) -> Result<Box<Filter>, Error> {
        Ok(Box::new(Filter {
            filter,
            child,
            output_schema,
            buffer: Chunk::default(),
        }))
    }
}

impl Executor for Filter {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        while self.buffer.data_chunks.len() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child.next_chunk()?;

            if next_chunk.data_chunks.is_empty() {
                break;
            }

            let helper_chunks: Result<Vec<_>, _> = next_chunk
                .data_chunks
                .into_iter()
                .map(|row| {
                    let e = ExprEvaluator::evaluate(&self.filter, &row, &self.output_schema)?;
                    Ok((row, e))
                })
                .collect();

            let filtered_chunks: Vec<Vec<TupleValue>> = helper_chunks?
                .into_iter()
                .filter(|(_, field)| {
                    ExprEvaluator::is_truthy(field)
                })
                .map(|(row, _)| row)
                .collect();

            self.buffer.data_chunks.extend(filtered_chunks);
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
