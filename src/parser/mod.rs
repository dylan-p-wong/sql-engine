use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::types::error::Error;

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
        if ast.is_err() {
            return Err(Error::Parser(ast.err().unwrap().to_string()));
        }
        Ok(ast.unwrap())
    }
}
