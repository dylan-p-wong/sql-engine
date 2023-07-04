use std::fmt::Error;

use sqlparser::ast::Expr;

use crate::storage::parquet::ParquetReader;
use crate::storage::{get_table_path, StorageReader};
use crate::types::{Chunk, Column};

use super::Executor;

pub struct Scan {
    table: String,
    filter: Option<Expr>,
    output_schema: Vec<Column>,
    reader: Box<dyn StorageReader>,
}

impl Scan {
    pub fn new(
        table: String,
        filter: Option<Expr>,
        output_schema: Vec<Column>,
    ) -> Result<Box<Self>, Error> {
        let table_path = get_table_path(&table);

        Ok(Box::new(Scan {
            table: table.clone(),
            reader: Box::new(ParquetReader::new(table_path)?),
            filter: filter,
            output_schema: output_schema,
        }))
    }
}

impl Executor for Scan {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        self.reader.next_chunk()
    }
    fn get_output_schema(&self) -> Vec<Column> {
        self.output_schema.clone()
    }
}
