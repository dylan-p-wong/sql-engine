use std::fmt::Error;

use parquet::record::reader::RowIter;
use sqlparser::ast::Expr;

use crate::storage::{StorageReader, get_table_path};
use crate::types::{Chunk, Column};
use crate::storage::parquet::{ParquetReader};

use super::Executor;

pub struct Scan {
    table : String,
    filter : Option<Expr>,
    output_schema : Vec<Column>,

    reader : Box<dyn StorageReader>,

    // temporary until parquet iterator is implemented
    polled : bool,
}

impl Scan {
    pub fn new(table : String, filter : Option<Expr>, output_schema : Vec<Column>) -> Result<Box<Scan>, Error> {
        let table_path = get_table_path(&table);

        Ok(Box::new(Scan {
            table: table.clone(),
            reader: Box::new(ParquetReader::new(&table_path)?),
            filter: filter,
            output_schema: output_schema,
            polled: false,
        }))
    }
}

impl Executor for Scan {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        if self.polled {
            return Ok(Chunk::default());
        }
        self.polled = true;
        self.reader.next_chunk()
    }
    fn get_output_schema(&self) -> Vec<Column> {
        self.output_schema.clone()
    }
}
