use crate::executor::expression::ExprEvaluator;
use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::Chunk;
use sqlparser::ast::Expr;

use super::{Buffer, VECTOR_SIZE_THRESHOLD};

pub struct Filter {
    output_schema: OutputSchema,
    filter: Expr,
    child: Box<dyn Executor>,

    buffer: Buffer,
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
            buffer: Buffer::new(),
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

            for row in next_chunk.get_rows().iter() {
                let e = ExprEvaluator::evaluate(&self.filter, row, &self.output_schema)?;
                if ExprEvaluator::to_boolean(&e) {
                    self.buffer.add_row(row.clone());
                }
            }
        }

        Ok(self.buffer.get_sized_chunk(VECTOR_SIZE_THRESHOLD))
    }

    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
