use std::fmt::Error;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SQLParser {
    dialect: GenericDialect,
}

impl SQLParser {
    pub fn new() -> SQLParser {
        SQLParser {
            dialect: GenericDialect {},
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, Error> {
        let ast = Parser::parse_sql(&self.dialect, sql);
        if !ast.is_ok() {
            return Err(Error {});
        }
        return Ok(ast.unwrap());
    }
}
