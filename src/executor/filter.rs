use std::fmt::Error;
use sqlparser::ast::Expr;
use crate::executor::expression::ExprEvaluator;
use crate::types::{Chunk};
use crate::executor::Executor;

pub struct Filter {
    filter : Expr,
    child : Box<dyn Executor>,
}

impl Filter {
    pub fn new(child : Box<dyn Executor>, filter : Expr) -> Result<Box<Filter>, Error> {
        Ok(Box::new(Filter {
            filter: filter,
            child: child,
        }))
    }
}

impl Executor for Filter {
    fn execute(self: Box<Self>) -> Result<Chunk, Error> {
        let res = self.child.execute()?;
        println!("Executing Filter...");

        let helper_chunks: Result<Vec<_>, _> = res.data_chunks.into_iter().map(|row| {
            let e = ExprEvaluator::evaluate(&self.filter, &row, &res.headers)?;
            return Ok((row, e))
        }).collect();

        let filtered_chunks = helper_chunks?.into_iter().filter(|(_, field)| {
            return ExprEvaluator::is_truthy(field);
        }).map(|(row, _)| row).collect();

        return Ok(Chunk{headers: res.headers, data_chunks: filtered_chunks});
    }
}
