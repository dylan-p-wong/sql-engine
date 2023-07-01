use std::{fmt::Error, mem::swap};

use sqlparser::ast::{SelectItem};
use crate::{types::{Chunk, Column, Row, TupleValue}, executor::expression::ExprEvaluator};

use super::{Executor, VECTOR_SIZE_THRESHOLD};

pub struct Projection {
    output_schema: Vec<Column>,
    select : Vec<SelectItem>,
    child : Box<dyn Executor>,

    buffer : Chunk,
}

impl Projection {
    pub fn new(child : Box<dyn Executor>, select : Vec<SelectItem>, output_schema : Vec<Column>) -> Result<Box<Projection>, Error> {        
        Ok(Box::new(Projection {
            buffer: Chunk::default(),
            select: select,
            child: child,
            output_schema: output_schema,
        }))
    }
}

impl Executor for Projection {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        println!("Projection");
        while self.buffer.data_chunks.len() < VECTOR_SIZE_THRESHOLD {
            let next_chunk = self.child.next_chunk()?;

            if next_chunk.data_chunks.len() == 0 {
                break;
            }

            for row in next_chunk.data_chunks {
                let mut new_row = Row::new();

                for item in &self.select {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let e = ExprEvaluator::evaluate(expr, &row, &self.output_schema)?;
                            new_row.push(TupleValue{value: e});
                        }
                        SelectItem::Wildcard(_) => {
                            for col in &row {
                                new_row.push(col.clone());
                            }
                        }
                        _ => {
                            return Err(Error {});
                        }
                    }
                }
                self.buffer.data_chunks.push(new_row);
            }
        }

        if self.buffer.data_chunks.len() == 0 {
            return Ok(Chunk::default());
        }

        let mut res_chunks = Vec::new();
        swap(&mut res_chunks, &mut self.buffer.data_chunks);
        return Ok(Chunk { data_chunks: res_chunks });
    }

    fn get_output_schema(&self) -> Vec<Column> {
        self.output_schema.clone()
    }
}
