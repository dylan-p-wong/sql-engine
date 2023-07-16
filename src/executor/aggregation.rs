use std::cmp;
use std::collections::HashMap;

use parquet::record::Field;
use sqlparser::ast::{Expr, Function, SelectItem};

use crate::executor::Executor;
use crate::planner::OutputSchema;
use crate::types::error::Error;
use crate::types::{Chunk, Row, TupleValue};

use super::VECTOR_SIZE_THRESHOLD;
use super::expression::{Caster, ExprEvaluator};

type GroupByKey = Vec<String>;
type AggregationColumns = Vec<Box<dyn Accumulator>>;
type NonAggregationColumns = Vec<Field>;

pub trait Accumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error>;
    fn aggregate(&self) -> Result<Field, Error>;
}

pub struct Aggregation {
    output_schema: OutputSchema,
    child: Box<dyn Executor>,
    aggregates: Vec<Function>,
    non_aggregates: Vec<SelectItem>,
    group_by: Vec<Expr>,

    rows: Option<Vec<Row>>,
}

impl Aggregation {
    pub fn new(
        child: Box<dyn Executor>,
        aggregates: Vec<Function>,
        non_aggregates: Vec<SelectItem>,
        group_by: Vec<Expr>,
        output_schema: OutputSchema,
    ) -> Result<Box<Aggregation>, Error> {
        Ok(Box::new(Aggregation {
            child,
            output_schema,
            group_by,
            aggregates,
            non_aggregates,
            rows: None,
        }))
    }

    fn init_accumulators(&mut self) -> Result<(), Error> {
        // TODO: consider when right rows is too large to fit in memory
        if self.rows.is_some() {
            return Ok(());
        }

        let mut rows_map: HashMap<GroupByKey, (AggregationColumns, NonAggregationColumns)> =
            HashMap::new();

        loop {
            let chunk = self.child.next_chunk()?;
            if chunk.data_chunks.is_empty() {
                break;
            }
            for row in chunk.data_chunks {
                let group_by_values: Vec<Field> = self
                    .group_by
                    .iter()
                    .map(|expr| {
                        ExprEvaluator::evaluate(expr, &row, &self.child.get_output_schema())
                    })
                    .collect::<Result<Vec<Field>, Error>>()?;

                // TODO(Dylan): See is there is some other better method to generate key
                let key = group_by_values
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<GroupByKey>();

                if let std::collections::hash_map::Entry::Vacant(e) = rows_map.entry(key.clone()) {
                    let mut accumulators: AggregationColumns = self
                        .aggregates
                        .iter()
                        .map(|a| self.new_accumulator(a))
                        .collect();
                    for (i, function) in self.aggregates.iter().enumerate() {
                        let field = ExprEvaluator::evaluate(
                            &self.get_expr(function)?,
                            &row,
                            &self.child.get_output_schema(),
                        )?;
                        accumulators[i].accumulate(&field)?;
                    }

                    let mut non_aggregated_values: NonAggregationColumns = Vec::new();
                    for expr in self.non_aggregates.iter() {
                        match expr {
                            SelectItem::UnnamedExpr(e) => {
                                let field = ExprEvaluator::evaluate(
                                    e,
                                    &row,
                                    &self.child.get_output_schema(),
                                )?;
                                non_aggregated_values.push(field);
                            }
                            SelectItem::Wildcard(_) => {
                                for col in &row {
                                    non_aggregated_values.push(col.value.clone());
                                }
                            }
                            _ => {
                                return Err(Error::Execution(format!(
                                    "Unsupported select item: {}",
                                    expr
                                )));
                            }
                        }
                    }

                    e.insert((accumulators, non_aggregated_values));
                } else {
                    let value = rows_map.get_mut(&key).unwrap();

                    for (i, function) in self.aggregates.iter().enumerate() {
                        let field = ExprEvaluator::evaluate(
                            &self.get_expr(function)?,
                            &row,
                            &self.child.get_output_schema(),
                        )?;
                        value.0[i].accumulate(&field)?;
                    }
                }
            }
        }

        //  if there are no rows we need to insert an empty row with empty accumulators
        if rows_map.keys().len() == 0 {
            let accumulators: AggregationColumns = self
                .aggregates
                .iter()
                .map(|a| self.new_accumulator(a))
                .collect();
            let mut non_aggregated_values: NonAggregationColumns = Vec::new();
            for _ in 0..self.non_aggregates.len() {
                non_aggregated_values.push(Field::Null);
            }
            rows_map.insert(Vec::new(), (accumulators, non_aggregated_values));
        }

        let mut rows = Vec::new();

        // calculate the rows
        for aggregate_row in rows_map.values() {
            let mut row: Row = Vec::new();
            for (i, _function) in self.aggregates.iter().enumerate() {
                row.push(TupleValue {
                    value: aggregate_row.0[i].aggregate()?,
                });
            }
            for v in aggregate_row.1.iter() {
                row.push(TupleValue { value: v.clone() });
            }
            rows.push(row);
        }

        self.rows = Some(rows);
        Ok(())
    }

    fn new_accumulator(&self, function: &Function) -> Box<dyn Accumulator> {
        // TODO(Dylan): Implement other functions
        match function.name.to_string().as_str() {
            "max" => Box::new(MaxAccumulator::new()),
            "min" => Box::new(MinAccumulator::new()),
            "sum" => Box::new(SumAccumulator::new()),
            "count" => Box::new(CountAccumulator::new()),
            "avg" => Box::new(AvgAccumulator::new()),
            _ => {
                panic!("Unsupported function: {}", function.name); // TODO(Dylan): Error handling
            }
        }
    }

    fn get_expr(&self, function: &Function) -> Result<Expr, Error> {
        if function.args.len() != 1 {
            return Err(Error::Expression(format!(
                "Unsupported number of parameteres: {}",
                function.args.len()
            )));
        }

        match &function.args[0] {
            sqlparser::ast::FunctionArg::Unnamed(fa) => match fa {
                sqlparser::ast::FunctionArgExpr::Expr(e) => Ok(e.clone()),
                sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => todo!(),
                sqlparser::ast::FunctionArgExpr::Wildcard => {
                    if function.name.to_string() == "count" {
                        return Ok(Expr::Value(sqlparser::ast::Value::Boolean(true)));
                    }
                    Err(Error::Expression(format!(
                        "Unsupported argument {} for function {}",
                        function.args[0], function.name
                    )))
                }
            },
            _ => Err(Error::Expression(format!(
                "Unsupported function : {}",
                function.args[0]
            ))),
        }
    }
}

impl Executor for Aggregation {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        // TODO(Dylan): Improve this handling and add vectorized processing
        self.init_accumulators()?;

        let mut res = Chunk::default();

        let n = cmp::min(VECTOR_SIZE_THRESHOLD, self.rows.as_ref().unwrap().len());

        self.rows.as_mut().unwrap().drain(0..n).for_each(|row| {
            res.data_chunks.push(row);
        });

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
        if *field == Field::Null {
            return Ok(());
        }

        match self.max {
            Some(ref max) => {
                let x = ExprEvaluator::evaluate_binary_op(
                    field,
                    &sqlparser::ast::BinaryOperator::GtEq,
                    max,
                )?;
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

    fn aggregate(&self) -> Result<Field, Error> {
        if self.max.is_none() {
            return Ok(Field::Null);
        }
        Ok(self.max.clone().unwrap())
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
        if *field == Field::Null {
            return Ok(());
        }

        match self.min {
            Some(ref min) => {
                let x = ExprEvaluator::evaluate_binary_op(
                    field,
                    &sqlparser::ast::BinaryOperator::LtEq,
                    min,
                )?;
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

    fn aggregate(&self) -> Result<Field, Error> {
        if self.min.is_none() {
            return Ok(Field::Null);
        }
        Ok(self.min.clone().unwrap())
    }
}

struct SumAccumulator {
    sum: Option<Field>,
}

impl SumAccumulator {
    fn new() -> SumAccumulator {
        SumAccumulator { sum: None }
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error> {
        if *field == Field::Null {
            return Ok(());
        }

        match self.sum {
            Some(ref sum) => {
                let x = ExprEvaluator::evaluate_binary_op(
                    field,
                    &sqlparser::ast::BinaryOperator::Plus,
                    sum,
                )?;
                self.sum = Some(x);
            }
            None => {
                self.sum = Some(field.clone());
            }
        }
        Ok(())
    }

    fn aggregate(&self) -> Result<Field, Error> {
        if self.sum.is_none() {
            return Ok(Field::Null);
        }
        Ok(self.sum.clone().unwrap())
    }
}

struct CountAccumulator {
    count: i32,
}

impl CountAccumulator {
    fn new() -> CountAccumulator {
        CountAccumulator { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error> {
        if *field == Field::Null {
            return Ok(());
        }
        self.count += 1;
        Ok(())
    }

    fn aggregate(&self) -> Result<Field, Error> {
        Ok(Field::Int(self.count))
    }
}

struct AvgAccumulator {
    count: i32,
    sum: Option<Field>,
}

impl AvgAccumulator {
    fn new() -> AvgAccumulator {
        AvgAccumulator {
            count: 0,
            sum: None,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, field: &Field) -> Result<(), Error> {
        if *field == Field::Null {
            return Ok(());
        }
        match self.sum {
            Some(ref sum) => {
                let x = ExprEvaluator::evaluate_binary_op(
                    field,
                    &sqlparser::ast::BinaryOperator::Plus,
                    sum,
                )?;
                self.sum = Some(x);
            }
            None => {
                self.sum = Some(field.clone());
            }
        }
        self.count += 1;
        Ok(())
    }

    fn aggregate(&self) -> Result<Field, Error> {
        if self.sum.is_none() {
            return Ok(Field::Null);
        }
        ExprEvaluator::evaluate_binary_op(
            &Caster::cast(
                self.sum.as_ref().unwrap(),
                &sqlparser::ast::DataType::Float(None),
            )?,
            &sqlparser::ast::BinaryOperator::Divide,
            &Field::Float(self.count as f32),
        )
    }
}
