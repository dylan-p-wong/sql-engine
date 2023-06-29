use std::fmt::Error;

use sqlparser::ast::Expr;

use crate::types::{Chunk};
use crate::storage::parquet::ParquetReader;

use super::Executor;

pub struct Scan {
    table : String,
    filter : Option<Expr>,
}

impl Scan {
    pub fn new(table : String, filter : Option<Expr>) -> Result<Box<Scan>, Error> {
        Ok(Box::new(Scan {
            table: table,
            filter: filter,
        }))
    }

    pub fn get_table_path(s : &str) -> String {
        return if s.starts_with("'") && s.ends_with("'") {
            s[1..s.len()-1].to_string()
        } else {
            s.to_string()
        };
    }
}

impl Executor for Scan {
    fn execute(self: Box<Self>) -> Result<Chunk, Error> {
        println!("Executing Scan...");

        if self.filter != None {
            return Err(Error {});
        } else {
            return ParquetReader::read(&Scan::get_table_path(&self.table));
        }
    }
}
