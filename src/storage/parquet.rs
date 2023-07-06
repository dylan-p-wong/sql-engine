use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::reader::RowIter,
};
use std::{fs::File, path::Path};

use crate::types::{error::Error, Chunk, Column, TupleValue};

use super::StorageReader;

pub struct ParquetReader {
    iter: RowIter<'static>,
}

impl StorageReader for ParquetReader {
    fn next_chunk(&mut self) -> Result<Chunk, Error> {
        let mut chunk = Chunk::default();

        for record in self.iter.by_ref() {
            let row = record
                .get_column_iter()
                .map(|x| TupleValue { value: x.1.clone() })
                .collect::<Vec<TupleValue>>();
            chunk.data_chunks.push(row);

            // TODO add an exeuction context with this information
            if chunk.data_chunks.len() >= 1024 {
                break;
            }
        }

        Ok(chunk)
    }
}

impl ParquetReader {
    pub fn new(table: String) -> Result<ParquetReader, Error> {
        let path = Path::new(table.as_str());

        if let Ok(file) = File::open(path) {
            let reader = SerializedFileReader::new(file).unwrap();
            Ok(ParquetReader {
                iter: reader.into_iter(),
            })
        } else {
            Err(Error::Storage(
                "Could not open file to read table metadata".to_string(),
            ))
        }
    }

    pub fn read_metadata(table: &str) -> Result<Vec<Column>, Error> {
        let path = Path::new(table);

        let mut headers = Vec::new();

        if let Ok(file) = File::open(path) {
            let reader = SerializedFileReader::new(file).unwrap();

            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .for_each(|x| {
                    headers.push(Column {
                        name: x.name().to_string(),
                    });
                });

            Ok(headers)
        } else {
            Err(Error::Storage(
                "Could not open file to read table data".to_string(),
            ))
        }
    }
}
