use std::collections::HashMap;
use std::f32::consts::E;

use parquet::record::Field;
use sqlparser::ast::{Function, Expr};

use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::{Chunk, Row, TupleValue};

use super::expression::ExprEvaluator;

pub trait Accumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error>;
    fn aggregate(&self) -> Field;
}

pub struct Aggregation {
    output_schema: OutputSchema,
    child: Box<dyn Executor>,
    aggregates: Vec<Function>,
    group_by: Vec<Expr>,

    rows: Option<HashMap<Vec<String>, (Vec<Box<dyn Accumulator>>, Vec<Field>)>>, // [group_by_values] -> ([accumulators], [non_aggregated_values])
}

impl Aggregation {
    pub fn new(
        child: Box<dyn Executor>,
        aggregates: Vec<Function>,
        group_by: Vec<Expr>,
        output_schema: OutputSchema,
    ) -> Result<Box<Aggregation>, Error> {
        Ok(Box::new(Aggregation {
            child,
            output_schema,
            group_by,
            aggregates,
            rows: None,
        }))
    }
    
    fn init_accumulators(&mut self) -> Result<(), Error> {
        // TODO: consider when right rows is too large to fit in memory
        if self.rows.is_some() {
            return Ok(());
        }

        let mut rows : HashMap<Vec<String>, (Vec<Box<dyn Accumulator>>, Vec<Field>)> = HashMap::new();
        
        loop {
            let chunk = self.child.next_chunk()?;
            if chunk.data_chunks.is_empty() {
                break;
            }
            for row in chunk.data_chunks {
                let group_by_values: Vec<Field> = self
                    .group_by
                    .iter()
                    .map(|expr| ExprEvaluator::evaluate(expr, &row, &self.child.get_output_schema()))
                    .collect::<Result<Vec<Field>, Error>>()?;

                // TODO(Dylan): See is there is some other better method to generate key
                let key = group_by_values.iter().map(|field| field.to_string()).collect::<Vec<String>>();
                if rows.contains_key(&key) {
                    let value = rows.get_mut(&key).unwrap();
                    
                    for (i, function) in self.aggregates.iter().enumerate() {
                        let field = ExprEvaluator::evaluate(&self.get_expr(function)?, &row, &self.child.get_output_schema())?;
                        value.0[i].accumulate(&field)?;
                    }
                } else {
                    let mut accumulators: Vec<Box<dyn Accumulator>> = self.aggregates.iter().map(|a| self.new_accumulator(a)).collect();
                    for (i, function) in self.aggregates.iter().enumerate() {
                        let field = ExprEvaluator::evaluate(&self.get_expr(function)?, &row, &self.child.get_output_schema())?;
                        accumulators[i].accumulate(&field)?;
                    }
                    
                    // TODO(Dylan): Implement other non aggregate values
                    let non_aggregated_values: Vec<Field> = Vec::new();

                    rows.insert(key, (accumulators, non_aggregated_values));
                }
            }
        }

        self.rows = Some(rows);
        Ok(())
    }

    fn new_accumulator(&self, function : &Function) -> Box<dyn Accumulator> {
        // TODO(Dylan): Implement other functions
        match function.name.to_string().as_str() {
            "max" => {
                return Box::new(MaxAccumulator::new());
            },
            "min" => {
                return Box::new(MinAccumulator::new());
            },
            _ => {
                panic!("Unsupported function: {}", function.name.to_string()); // TODO(Dylan): Error handling
            }
        }
    }

    fn get_expr(&self, function : &Function) -> Result<Expr, Error> {
        if function.args.len() != 1 {
            return Err(Error::Expression(format!(
                "Unsupported number of parameteres: {}",
                function.args.len()
            )));
        }

        match &function.args[0] {
            sqlparser::ast::FunctionArg::Unnamed(fa) => {
                match fa {
                    sqlparser::ast::FunctionArgExpr::Expr(e) => Ok(e.clone()),
                    sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => todo!(),
                    sqlparser::ast::FunctionArgExpr::Wildcard => todo!(),
                }
            },
            _ => {
                Err(Error::Expression(format!(
                    "Unsupported function : {}",
                    function.args[0].to_string()
                )))
            }
        }
    }
}

impl Executor for Aggregation {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        // TODO(Dylan): Improve this handling and add vectorized processing
        self.init_accumulators()?;

        let mut res = Chunk::default();

        for (key, value) in self.rows.as_ref().unwrap() {
            let mut row : Row = Vec::new();
            for (i, expr) in self.aggregates.iter().enumerate() {
                row.push(TupleValue {
                    value: value.0[i].aggregate(),
                });
            }
            for v in value.1.iter() {
                row.push(TupleValue {
                    value: v.clone(),
                });
            }
            res.data_chunks.push(row);
        }
        self.rows = None;
        Ok(res)
    }

    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}

struct MaxAccumulator {
    max: Option<Field>,
}

impl MaxAccumulator {
    fn new() -> MaxAccumulator {
        MaxAccumulator { max: None }
    }
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error> {
        match self.max {
            Some(ref max) => {
                let x = ExprEvaluator::evaluate_binary_op(field, &sqlparser::ast::BinaryOperator::GtEq, max)?;
                if ExprEvaluator::to_boolean(&x) {
                    self.max = Some(field.clone());
                }
            }
            None => {
                self.max = Some(field.clone());
            }
        }
        Ok(())
    }

    fn aggregate(&self) -> Field {
        self.max.clone().unwrap()
    }
}

struct MinAccumulator {
    min: Option<Field>,
}

impl MinAccumulator {
    fn new() -> MinAccumulator {
        MinAccumulator { min: None }
    }
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error> {
        match self.min {
            Some(ref min) => {
                let x = ExprEvaluator::evaluate_binary_op(field, &sqlparser::ast::BinaryOperator::LtEq, min)?;
                if ExprEvaluator::to_boolean(&x) {
                    self.min = Some(field.clone());
                }
            }
            None => {
                self.min = Some(field.clone());
            }
        }
        Ok(())
    }

    fn aggregate(&self) -> Field {
        self.min.clone().unwrap()
    }
}
