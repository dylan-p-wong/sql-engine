use std::fmt::Error;

use sqlparser::ast::{Expr, Ident, BinaryOperator};
use parquet::record::Field;

use crate::types::{Row, Column};

pub struct ExprEvaluator;

impl ExprEvaluator {
    pub fn is_truthy(field : &Field) -> bool {
        match field {
            Field::Bool(b) => {
                return *b;
            }
            Field::Int(i) => {
                return *i != 0;
            }
            Field::Long(l) => {
                return *l != 0;
            }
            Field::Float(f) => {
                return *f != 0.0;
            }
            Field::Double(d) => {
                return *d != 0.0;
            }
            Field::Str(s) => {
                return s != "";
            }
            Field::Null => {
                return false;
            }
            _ => {
                return false;
            }
        }
    }

    pub fn evaluate(expr : &Expr, row : &Row, columns : &Vec<Column>) -> Result<Field, Error> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = Self::evaluate(left, row, columns)?;
                let right = Self::evaluate(right, row, columns)?;

                return Self::evaluate_binary_op(&left, op, &right);
            }
            Expr::Identifier(ident) => {
                return Self::evaluate_identifier(ident, row, columns);
            }
            Expr::Value(value) => {
                return Self::evaluate_value(value);
            }
            _ => {
                return Err(Error {})
            }
        }
    }

    pub fn evaluate_identifier(ident : &Ident, row : &Row, columns : &Vec<Column>) -> Result<Field, Error> {
        for (i, column) in columns.iter().enumerate() {
            if column.name == ident.value {
                return Ok(row[i].value.clone());
            }
        }

        return Err(Error {});
    }

    pub fn evaluate_binary_op(left : &Field, op : &BinaryOperator, right : &Field) -> Result<Field, Error> {
        match op {
            BinaryOperator::NotEq => {
                return Ok(Field::Bool(left != right));
            }
            BinaryOperator::Eq => {
                return Ok(Field::Bool(left == right));
            }
            _ => {
                return Err(Error {})
            }
        }
    }

    // Converts from sqlparser::ast::Value to parquet::record::Field
    pub fn evaluate_value(value : &sqlparser::ast::Value) -> Result<Field, Error> {
        match value {
            sqlparser::ast::Value::Number(n, b) => {
                if n.parse::<i32>().is_ok() {
                    return Ok(Field::Int(n.parse::<i32>().unwrap()));
                } else if n.parse::<i64>().is_ok() {
                    return Ok(Field::Long(n.parse::<i64>().unwrap()));
                } else if n.parse::<f32>().is_ok() {
                    return Ok(Field::Float(n.parse::<f32>().unwrap()));
                } else {
                    return Err(Error {})
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                return Ok(Field::Str(s.to_string()));
            }
            sqlparser::ast::Value::Null => {
                return Ok(Field::Null);
            }
            _ => {
                return Err(Error {});
            }
        }
    }
}
