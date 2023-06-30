mod scan;
mod filter;
mod projection;
mod expression;
mod empty;

use std::fmt::Error;

use crate::{types::Chunk, planner::{Plan, Node}};

use self::{scan::Scan, filter::Filter, projection::Projection, empty::Empty};

pub trait Executor {
    fn execute(self: Box<Self>) -> Result<Chunk, Error>;
}

struct ExecutorBuilder {}

impl ExecutorBuilder {
    fn build_from_plan(plan: Plan) -> Result<Box<dyn Executor>, Error> {
        return Self::build(plan.root);
    }

    fn build(node: Node) -> Result<Box<dyn Executor>, Error> {
        match node {
            Node::Scan { table_name, filter } => {
                match Scan::new(table_name, filter.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                }
            }
            Node::Filter { filter, child } => {
                let child = Self::build(*child)?;
                
                return match Filter::new(child, filter.clone()) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(Error {}),
                };
            }
            Node::Projection { select, child } => {
                let child = Self::build(*child)?;
                
                return match Projection::new(child, select.clone()) {
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

    pub fn execute(&self, plan: Plan) -> Result<Chunk, Error> {
        println!("Executing...");
        let executor = ExecutorBuilder::build_from_plan(plan)?;
        return executor.execute();
    }
}
