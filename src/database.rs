
use std::fmt::Error;

use crate::parser;
use crate::planner;
use crate::optimizer;
use crate::executor;
use crate::types::ResultSet;

pub struct Database {
    parser: parser::SQLParser,
    planner: planner::Planner,
    optimizer: optimizer::Optimizer,
    executor: executor::ExecutionEngine,
}

impl Database {
    pub fn new() -> Result<Database, String> {
        let parser = parser::SQLParser::new();
        let planner = planner::Planner::new();
        let optimizer = optimizer::Optimizer::new();
        let executor = executor::ExecutionEngine::new();

        return Ok(Database {
            parser,
            planner,
            optimizer,
            executor,
        })
    }

    pub fn execute(&self, sql : &str) -> Result<ResultSet, Error> {
        let ast = self.parser.parse(sql)?;
        let plan = self.planner.build(&ast)?;
        let optimized_plan = self.optimizer.optimize(plan)?;
        let result_set = self.executor.execute(optimized_plan)?;
        return Ok(result_set)
    }
}
