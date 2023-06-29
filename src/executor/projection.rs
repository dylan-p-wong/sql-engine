use std::fmt::Error;

use sqlparser::ast::{SelectItem};
use crate::{types::{Chunk, Column, Row, TupleValue}, executor::expression::ExprEvaluator};

use super::Executor;

pub struct Projection {
    select : Vec<SelectItem>,
    child : Box<dyn Executor>,
}

impl Projection {
    pub fn new(child : Box<dyn Executor>, select : Vec<SelectItem>) -> Result<Box<Projection>, Error> {
        Ok(Box::new(Projection {
            select: select,
            child: child,
        }))
    }
}

impl Executor for Projection {
    fn execute(self: Box<Self>) -> Result<Chunk, Error> {
        let res = self.child.execute()?;
        println!("Executing Projection...");

        let mut out_headers : Vec<Column> = Vec::new();
        let mut out_data_chunks : Vec<Row> = Vec::new();
        
        if self.select.len() == 1 && self.select[0].to_string() == "*" {
            return Ok(Chunk { headers: res.headers.clone(), data_chunks: res.data_chunks }); 
        }

        for item in &self.select {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    out_headers.push(Column{name: expr.to_string()});
                }
                _ => {
                    return Err(Error {});
                }
            }
        }

        for row in res.data_chunks {
            let mut new_row = Row::new();
            for item in &self.select {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let e = ExprEvaluator::evaluate(expr, &row, &res.headers)?;
                        new_row.push(TupleValue{value: e});
                    }
                    _ => {
                        return Err(Error {});
                    }
                }
            }
            out_data_chunks.push(new_row);
        }
    
        return Ok(Chunk { headers: out_headers, data_chunks: out_data_chunks });
    }
}
