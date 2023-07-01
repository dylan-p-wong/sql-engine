mod scan;
mod filter;
mod projection;
mod expression;
mod empty;

use std::fmt::Error;

use crate::{types::{Chunk, ResultSet, Column}, planner::{Plan, Node, PlanNode}};

use self::{scan::Scan, filter::Filter, projection::Projection, empty::Empty};

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
                match Scan::new(table_name, filter.clone(), plan_node.output_schema.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
            Node::Filter { filter, child } => {
                let child = Self::build(*child)?;
                
                return match Filter::new(child, filter.clone(), plan_node.output_schema.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                };
            }
            Node::Projection { select, child } => {
                let child = Self::build(*child)?;
                
                return match Projection::new(child, select.clone(), plan_node.output_schema.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                };
            }
            Node::Empty {} => { 
                return match Empty::new() {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                };
            }
            _ => {
                return Err(Error {})
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
        println!("Executing...");
        let mut executor = ExecutorBuilder::build_from_plan(plan)?;
        let mut result = ResultSet::default();
        result.output_schema = executor.get_output_schema();

        loop {
            let chunk = executor.next_chunk()?;

            if chunk.data_chunks.len() == 0 {
                break;
            }

            result.data_chunks.push(chunk);
        }

        Ok(result)
    }
}
