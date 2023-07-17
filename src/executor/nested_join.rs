use sqlparser::ast::Expr;

use crate::{
    planner::OutputSchema,
    types::{error::Error, Chunk, Row},
};

use super::{expression::ExprEvaluator, Buffer, Executor, VECTOR_SIZE_THRESHOLD};

pub struct NestedLoopJoin {
    output_schema: OutputSchema,
    predicate: Option<Expr>,
    child_left: Box<dyn Executor>,
    child_right: Box<dyn Executor>,

    buffer: Buffer,
    right_rows: Option<Vec<Row>>,
}

impl NestedLoopJoin {
    pub fn new(
        child_left: Box<dyn Executor>,
        child_right: Box<dyn Executor>,
        predicate: Option<Expr>,
        output_schema: OutputSchema,
    ) -> Result<Box<NestedLoopJoin>, Error> {
        Ok(Box::new(NestedLoopJoin {
            buffer: Buffer::new(),
            right_rows: None,
            predicate,
            child_left,
            child_right,
            output_schema,
        }))
    }

    fn init_right_rows(&mut self) -> Result<(), Error> {
        // TODO: consider when right rows is too large to fit in memory
        let mut res = Vec::new();
        if self.right_rows.is_none() {
            loop {
                let chunk = self.child_right.next_chunk()?;
                if chunk.is_empty() {
                    break;
                }
                for row in chunk.get_rows() {
                    res.push(row.clone())
                }
            }
            self.right_rows = Some(res);
        }
        Ok(())
    }
}

impl Executor for NestedLoopJoin {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        self.init_right_rows()?;

        while self.buffer.size() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child_left.next_chunk()?;

            if next_chunk.is_empty() {
                break;
            }

            for left_row in next_chunk.get_rows() {
                for right_row in self.right_rows.as_ref().unwrap().iter() {
                    let mut new_row = left_row.clone();
                    new_row.append(&mut right_row.clone());

                    if self.predicate.is_some() {
                        let e = ExprEvaluator::evaluate(
                            self.predicate.as_ref().unwrap(),
                            &new_row,
                            &self.output_schema,
                        )?;
                        if !ExprEvaluator::to_boolean(&e) {
                            continue;
                        }
                    }

                    self.buffer.add_row(new_row);
                }
            }
        }

        Ok(self.buffer.get_sized_chunk(VECTOR_SIZE_THRESHOLD))
    }
    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
