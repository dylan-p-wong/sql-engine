mod empty;
mod expression;
mod filter;
mod projection;
mod scan;

use std::fmt::Error;

use crate::{
    planner::{Node, Plan, PlanNode},
    types::{Chunk, Column, ResultSet},
};

use self::{empty::Empty, filter::Filter, projection::Projection, scan::Scan};

const VECTOR_SIZE_THRESHOLD: usize = 1024;

pub trait Executor {
    fn get_output_schema(&self) -> Vec<Column>;
    fn next_chunk(&mut self) -> Result<Chunk, Error>;
}

struct ExecutorBuilder {}

impl ExecutorBuilder {
    fn build_from_plan(plan: Plan) -> Result<Box<dyn Executor>, Error> {
        return Self::build(plan.root);
    }

    fn build(plan_node: PlanNode) -> Result<Box<dyn Executor>, Error> {
        match plan_node.node {
            Node::Scan { table_name, filter } => {
                match Scan::new(table_name, filter, plan_node.output_schema.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
            Node::Filter { filter, child } => {
                let child = Self::build(*child)?;

                match Filter::new(child, filter, plan_node.output_schema.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
            Node::Projection { select, child } => {
                let child = Self::build(*child)?;

                match Projection::new(child, select, plan_node.output_schema.clone())
                {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
            Node::Empty {} => {
                match Empty::new() {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
        }
    }
}

pub struct ExecutionEngine {}

impl ExecutionEngine {
    pub fn new() -> ExecutionEngine {
        ExecutionEngine {}
    }

    pub fn execute(&self, plan: Plan) -> Result<ResultSet, Error> {
        let mut executor = ExecutorBuilder::build_from_plan(plan)?;
        let mut result = ResultSet::default();
        result.output_schema = executor.get_output_schema();

        loop {
            let chunk = executor.next_chunk()?;

            if chunk.data_chunks.is_empty() {
                break;
            }

            result.data_chunks.push(chunk);
        }

        Ok(result)
    }
}
