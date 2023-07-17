mod aggregation;
mod empty;
mod expression;
mod filter;
mod nested_join;
mod projection;
mod scan;

use std::cmp;

use crate::{
    planner::{Node, OutputSchema, Plan, PlanNode},
    types::{error::Error, Chunk, ResultSet, Row},
};

use self::{
    aggregation::Aggregation, empty::Empty, filter::Filter, nested_join::NestedLoopJoin,
    projection::Projection, scan::Scan,
};

const VECTOR_SIZE_THRESHOLD: usize = 1024;

pub trait Executor {
    fn get_output_schema(&self) -> OutputSchema;
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
                    Err(e) => Err(e),
                }
            }
            Node::Filter { filter, child } => {
                let child = Self::build(*child)?;

                match Filter::new(child, filter, plan_node.output_schema) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(e),
                }
            }
            Node::Projection { select, child } => {
                let child = Self::build(*child)?;

                match Projection::new(child, select, plan_node.output_schema) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(e),
                }
            }
            Node::Empty {} => match Empty::new() {
                Ok(e) => Ok(e),
                Err(e) => Err(e),
            },
            Node::NestedLoopJoin {
                child_left,
                child_right,
                predicate,
            } => {
                let child_left = Self::build(*child_left)?;
                let child_right = Self::build(*child_right)?;

                match NestedLoopJoin::new(
                    child_left,
                    child_right,
                    predicate,
                    plan_node.output_schema.clone(),
                ) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(e),
                }
            }
            Node::Aggregate {
                child,
                aggregates,
                group_by,
                non_aggregates,
            } => {
                let child = Self::build(*child)?;

                match Aggregation::new(
                    child,
                    aggregates,
                    non_aggregates,
                    group_by,
                    plan_node.output_schema,
                ) {
                    Ok(e) => Ok(e),
                    Err(e) => Err(e),
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
        let mut result = ResultSet::new(executor.get_output_schema());

        loop {
            let chunk = executor.next_chunk()?;

            if chunk.is_empty() {
                break;
            }

            result.data_chunks.push(chunk);
        }

        Ok(result)
    }
}

struct Buffer {
    rows: Vec<Row>,
}

impl Buffer {
    fn new() -> Buffer {
        Buffer { rows: Vec::new() }
    }

    fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    fn size(&self) -> usize {
        self.rows.len()
    }

    pub fn get_sized_chunk(&mut self, size: usize) -> Chunk {
        let mut res = Chunk::new();

        let n = cmp::min(size, self.size());

        self.rows.drain(0..n).for_each(|row| {
            res.add_row(row);
        });

        res
    }
}
