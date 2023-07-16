use parquet::record::Field;
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator};

use crate::{
    planner::OutputSchema,
    types::{error::Error, Row},
};

pub struct ExprEvaluator;

impl ExprEvaluator {
    pub fn to_boolean(field: &Field) -> bool {
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
            Expr::Nested(expr) => Self::evaluate(expr, row, columns),
            Expr::UnaryOp { op, expr } => Self::evaluate_unary_op(op, expr, row, columns),
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
        assert!(output_schema.columns.len() == row.len());
        let index = output_schema.resolve(&ident.value)?;
        Ok(row[index].value.clone())
    }

    pub fn evaluate_unary_op(
        op: &UnaryOperator,
        expr: &Expr,
        row: &Row,
        columns: &OutputSchema,
    ) -> Result<Field, Error> {
        let field = Self::evaluate(expr, row, columns)?;
        match op {
            UnaryOperator::Not => Ok(Field::Bool(!Self::to_boolean(&field))),
            UnaryOperator::Plus => match field {
                Field::Int(i) => Ok(Field::Int(i)),
                Field::Long(l) => Ok(Field::Long(l)),
                Field::Float(f) => Ok(Field::Float(f)),
                Field::Double(d) => Ok(Field::Double(d)),
                _ => Err(Error::Expression(format!(
                    "Unsupported unary operation: {} {}",
                    op, field
                ))),
            },
            UnaryOperator::Minus => match field {
                Field::Int(i) => Ok(Field::Int(-i)),
                Field::Long(l) => Ok(Field::Long(-l)),
                Field::Float(f) => Ok(Field::Float(-f)),
                Field::Double(d) => Ok(Field::Double(-d)),
                _ => Err(Error::Expression(format!(
                    "Unsupported unary operation: {} {}",
                    op, field
                ))),
            },
            _ => Err(Error::Expression(format!(
                "Unsupported unary operation: {} {}",
                op, field
            ))),
        }
    }

    pub fn evaluate_binary_op(
        left: &Field,
        op: &BinaryOperator,
        right: &Field,
    ) -> Result<Field, Error> {
        match op {
            BinaryOperator::NotEq => Ok(Field::Bool(left != right)),
            BinaryOperator::Eq => Ok(Field::Bool(left == right)),
            BinaryOperator::And => Ok(Field::Bool(
                Self::to_boolean(left) && Self::to_boolean(right),
            )),
            BinaryOperator::Or => Ok(Field::Bool(
                Self::to_boolean(left) || Self::to_boolean(right),
            )),
            BinaryOperator::Xor => Ok(Field::Bool(
                (Self::to_boolean(left) && !Self::to_boolean(right))
                    || (!Self::to_boolean(left) && Self::to_boolean(right)),
            )),
            BinaryOperator::Plus => Ok(BinaryOpEvaluator::add(left, right)?),
            BinaryOperator::Minus => Ok(BinaryOpEvaluator::subtract(left, right)?),
            BinaryOperator::Multiply => Ok(BinaryOpEvaluator::multipy(left, right)?),
            BinaryOperator::Divide => Ok(BinaryOpEvaluator::divide(left, right)?),
            BinaryOperator::Lt => Ok(BinaryOpEvaluator::less_than(left, right)?),
            BinaryOperator::LtEq => Ok(BinaryOpEvaluator::less_than_or_equal(left, right)?),
            BinaryOperator::Gt => Ok(BinaryOpEvaluator::greater_than(left, right)?),
            BinaryOperator::GtEq => Ok(BinaryOpEvaluator::greater_than_or_equal(left, right)?),
            _ => Err(Error::Expression(format!(
                "Binary operation {} not supported",
                op
            ))),
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

pub struct BinaryOpEvaluator;

impl BinaryOpEvaluator {
    fn add(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Int(l + r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Long(l + r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Float(l + r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Double(l + r)),
            (Field::Str(l), Field::Str(r)) => Ok(Field::Str(format!("{}{}", l, r))),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} + {}",
                left, right
            ))),
        }
    }

    fn subtract(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Int(l - r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Long(l - r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Float(l - r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Double(l - r)),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} - {}",
                left, right
            ))),
        }
    }

    fn multipy(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Int(l * r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Long(l * r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Float(l * r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Double(l * r)),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} * {}",
                left, right
            ))),
        }
    }

    fn divide(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Int(l / r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Long(l / r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Float(l / r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Double(l / r)),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} / {}",
                left, right
            ))),
        }
    }

    fn less_than(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Bool(l < r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Bool(l < r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Bool(l < r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Bool(l < r)),
            (Field::Str(l), Field::Str(r)) => Ok(Field::Bool(l < r)),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} < {}",
                left, right
            ))),
        }
    }

    fn greater_than(left: &Field, right: &Field) -> Result<Field, Error> {
        Self::less_than(right, left)
    }

    fn less_than_or_equal(left: &Field, right: &Field) -> Result<Field, Error> {
        match (left, right) {
            (Field::Int(l), Field::Int(r)) => Ok(Field::Bool(l <= r)),
            (Field::Long(l), Field::Long(r)) => Ok(Field::Bool(l <= r)),
            (Field::Float(l), Field::Float(r)) => Ok(Field::Bool(l <= r)),
            (Field::Double(l), Field::Double(r)) => Ok(Field::Bool(l <= r)),
            (Field::Str(l), Field::Str(r)) => Ok(Field::Bool(l <= r)),
            _ => Err(Error::Expression(format!(
                "Unsupported binary operation: {} =< {}",
                left, right
            ))),
        }
    }

    fn greater_than_or_equal(left: &Field, right: &Field) -> Result<Field, Error> {
        Self::less_than_or_equal(right, left)
    }
}

pub struct Caster {}

impl Caster {
    pub fn cast(field: &Field, data_type: &sqlparser::ast::DataType) -> Result<Field, Error> {
        match data_type {
            sqlparser::ast::DataType::Int(_) => match field {
                Field::Int(i) => Ok(Field::Int(*i)),
                Field::Long(l) => Ok(Field::Int(*l as i32)),
                Field::Float(f) => Ok(Field::Int(*f as i32)),
                Field::Double(d) => Ok(Field::Int(*d as i32)),
                Field::Str(s) => Ok(Field::Int(s.parse::<i32>().unwrap())),
                _ => Err(Error::Expression(format!(
                    "Unable to cast {} to Int",
                    field
                ))),
            },
            sqlparser::ast::DataType::Float(_) => match field {
                Field::Int(i) => Ok(Field::Float(*i as f32)),
                Field::Long(l) => Ok(Field::Float(*l as f32)),
                Field::Float(f) => Ok(Field::Float(*f)),
                Field::Double(d) => Ok(Field::Float(*d as f32)),
                Field::Str(s) => Ok(Field::Float(s.parse::<f32>().unwrap())),
                _ => Err(Error::Expression(format!(
                    "Unable to cast {} to Float",
                    field
                ))),
            },
            sqlparser::ast::DataType::Double => match field {
                Field::Int(i) => Ok(Field::Double(*i as f64)),
                Field::Long(l) => Ok(Field::Double(*l as f64)),
                Field::Float(f) => Ok(Field::Double(*f as f64)),
                Field::Double(d) => Ok(Field::Double(*d)),
                Field::Str(s) => Ok(Field::Double(s.parse::<f64>().unwrap())),
                _ => Err(Error::Expression(format!(
                    "Unable to cast {} to Float",
                    field
                ))),
            },
            _ => Err(Error::Expression(format!(
                "Unsupported cast data type: {}",
                data_type
            ))),
        }
    }
}
