use crate::executor::expression::ExprEvaluator;
use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::{Chunk, TupleValue};
use sqlparser::ast::Expr;
use std::mem::swap;

use super::VECTOR_SIZE_THRESHOLD;

pub struct Filter {
    output_schema: OutputSchema,
    filter: Expr,
    child: Box<dyn Executor>,

    buffer: Chunk,
}

impl Filter {
    pub fn new(
        child: Box<dyn Executor>,
        filter: Expr,
        output_schema: OutputSchema,
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
        while self.buffer.size() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child.next_chunk()?;

            if next_chunk.is_empty() {
                break;
            }

            let helper_chunks: Result<Vec<_>, _> = next_chunk
                .get_rows()
                .iter()
                .map(|row| {
                    let e = ExprEvaluator::evaluate(&self.filter, row, &self.output_schema)?;
                    Ok((row, e))
                })
                .collect();

            let filtered_chunks: Vec<Vec<TupleValue>> = helper_chunks?
                .into_iter()
                .filter(|(_, field)| ExprEvaluator::to_boolean(field))
                .map(|(row, _)| row.clone())
                .collect();

            for row in filtered_chunks {
                self.buffer.add_row(row)
            }
        }

        if self.buffer.is_empty() {
            return Ok(Chunk::default());
        }

        let mut res = Chunk::new();
        swap(&mut res, &mut self.buffer);
        Ok(res)
    }

    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
