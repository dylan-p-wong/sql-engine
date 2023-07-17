use crate::{
    executor::expression::ExprEvaluator,
    planner::OutputSchema,
    types::{error::Error, Chunk, Row, TupleValue},
};
use sqlparser::ast::SelectItem;

use super::{Buffer, Executor, VECTOR_SIZE_THRESHOLD};

pub struct Projection {
    output_schema: OutputSchema,
    select: Vec<SelectItem>,
    child: Box<dyn Executor>,

    buffer: Buffer,
}

impl Projection {
    pub fn new(
        child: Box<dyn Executor>,
        select: Vec<SelectItem>,
        output_schema: OutputSchema,
    ) -> Result<Box<Projection>, Error> {
        Ok(Box::new(Projection {
            buffer: Buffer::new(),
            select,
            child,
            output_schema,
        }))
    }
}

impl Executor for Projection {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        while self.buffer.size() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child.next_chunk()?;

            if next_chunk.is_empty() {
                break;
            }

            for row in next_chunk.get_rows() {
                let mut new_row = Row::new();

                for item in &self.select {
                    match &item {
                        SelectItem::UnnamedExpr(expr) => {
                            let e = ExprEvaluator::evaluate(
                                expr,
                                row,
                                &self.child.get_output_schema(),
                            )?;
                            new_row.push(TupleValue { value: e });
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            let e = ExprEvaluator::evaluate(
                                expr,
                                row,
                                &self.child.get_output_schema(),
                            )?;
                            new_row.push(TupleValue { value: e });
                        }
                        SelectItem::Wildcard(_) => {
                            for col in row {
                                new_row.push(col.clone());
                            }
                        }
                        _ => {
                            return Err(Error::Execution(format!("{} not supported", item)));
                        }
                    }
                }
                self.buffer.add_row(new_row);
            }
        }

        Ok(self.buffer.get_sized_chunk(VECTOR_SIZE_THRESHOLD))
    }

    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
