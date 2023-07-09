use sqlparser::ast::Expr;

use crate::planner::OutputSchema;
use crate::storage::parquet::ParquetReader;
use crate::storage::{get_table_path, StorageReader};
use crate::types::error::Error;
use crate::types::Chunk;

use super::Executor;

pub struct Scan {
    _table: String,
    _filter: Option<Expr>,
    output_schema: OutputSchema,
    reader: Box<dyn StorageReader>,
}

impl Scan {
    pub fn new(
        table: String,
        filter: Option<Expr>,
        output_schema: OutputSchema,
    ) -> Result<Box<Self>, Error> {
        let table_path = get_table_path(&table);

        Ok(Box::new(Scan {
            _table: table,
            reader: Box::new(ParquetReader::new(table_path)?),
            _filter: filter,
            output_schema,
        }))
    }
}

impl Executor for Scan {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        self.reader.next_chunk()
    }
    fn get_output_schema(&self) -> OutputSchema {
        self.output_schema.clone()
    }
}
