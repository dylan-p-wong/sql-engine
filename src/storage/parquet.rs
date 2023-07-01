use parquet::{file::reader::{FileReader, SerializedFileReader}, record::reader::RowIter};
use std::{fs::File, path::Path, fmt::Error};

use crate::types::{Column, Chunk, TupleValue};

use super::StorageReader;

pub struct ParquetReader {
    reader : SerializedFileReader<File>,
}

impl ParquetReader {
    pub fn new(table : &str) -> Result<ParquetReader, Error> {
        let path = Path::new(table.clone());

        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file).unwrap();            
            
            
            Ok(ParquetReader{ reader })
        } else {
            println!("File not found");
            return Err(Error {});
        }
    }

    pub fn read_metadata(table : &str) -> Result<Vec<Column>, Error> {
        println!("Reading {}...", table);
        let path = Path::new(table);

        let mut headers = Vec::new();

        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file).unwrap();

            reader.metadata().file_metadata().schema_descr().columns().iter().for_each(|x| {
                headers.push(Column { name: x.name().to_string() });
            });

            return Ok(headers);
        } else {
            println!("File not found");
            return Err(Error {});
        }
    }
}

impl StorageReader for ParquetReader {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        let mut chunk = Chunk::default();

        let mut iter = self.reader.get_row_iter(None).unwrap();

        while let Some(record) = iter.next() {
            let row = record.get_column_iter().map(|x| TupleValue{ value: x.1.clone() } ).collect::<Vec<TupleValue>>();
            chunk.data_chunks.push(row);
        }

        Ok(chunk)
    }
}
