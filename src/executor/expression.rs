use parquet::record::Field;
use sqlparser::ast::{BinaryOperator, Expr, Ident};

use crate::{
    planner::OutputSchema,
    types::{error::Error, Row},
};

pub struct ExprEvaluator;

impl ExprEvaluator {
    pub fn is_truthy(field: &Field) -> bool {
        match field {
            Field::Bool(b) => *b,
            Field::Int(i) => *i != 0,
            Field::Long(l) => *l != 0,
            Field::Float(f) => *f != 0.0,
            Field::Double(d) => *d != 0.0,
            Field::Str(s) => !s.is_empty(),
            Field::Null => false,
            _ => false,
        }
    }

    pub fn evaluate(expr: &Expr, row: &Row, columns: &OutputSchema) -> Result<Field, Error> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = Self::evaluate(left, row, columns)?;
                let right = Self::evaluate(right, row, columns)?;

                Self::evaluate_binary_op(&left, op, &right)
            }
            Expr::Identifier(ident) => Self::evaluate_identifier(ident, row, columns),
            Expr::Value(value) => Self::evaluate_value(value),
            Expr::CompoundIdentifier(idents) => Self::evaluate_identifier(
                &Ident::new(
                    idents
                        .iter()
                        .map(|i| i.value.clone())
                        .collect::<Vec<String>>()
                        .join("."),
                ),
                row,
                columns,
            ),
            _ => Err(Error::Expression(format!(
                "Unsupported expression: {}",
                expr
            ))),
        }
    }

    pub fn evaluate_identifier(
        ident: &Ident,
        row: &Row,
        output_schema: &OutputSchema,
    ) -> Result<Field, Error> {
        let index = output_schema.resolve(&ident.value)?;
        Ok(row[index].value.clone())
    }

    pub fn evaluate_binary_op(
        left: &Field,
        op: &BinaryOperator,
        right: &Field,
    ) -> Result<Field, Error> {
        match op {
            BinaryOperator::NotEq => Ok(Field::Bool(left != right)),
            BinaryOperator::Eq => Ok(Field::Bool(left == right)),
            _ => Err(Error::Expression(
                "Binary operation not supported".to_string(),
            )),
        }
    }

    // Converts from sqlparser::ast::Value to parquet::record::Field
    pub fn evaluate_value(value: &sqlparser::ast::Value) -> Result<Field, Error> {
        match value {
            sqlparser::ast::Value::Number(n, _b) => {
                if n.parse::<i32>().is_ok() {
                    Ok(Field::Int(n.parse::<i32>().unwrap()))
                } else if n.parse::<i64>().is_ok() {
                    Ok(Field::Long(n.parse::<i64>().unwrap()))
                } else if n.parse::<f32>().is_ok() {
                    Ok(Field::Float(n.parse::<f32>().unwrap()))
                } else {
                    Err(Error::Expression("Unable to parse Number".to_string()))
                }
            }
            sqlparser::ast::Value::Boolean(b) => Ok(Field::Bool(*b)),
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(Field::Str(s.to_string())),
            sqlparser::ast::Value::Null => Ok(Field::Null),
            _ => Err(Error::Expression(format!("Unsupported value: {}", value))),
        }
    }
}
