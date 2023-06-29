use parquet::{file::reader::{FileReader, SerializedFileReader}};
use std::{fs::File, path::Path, fmt::Error};

use crate::types::{TupleValue, Row, Column, Chunk};

pub struct ParquetReader {}

impl ParquetReader {
    pub fn read(table : &str) -> Result<Chunk, Error> {
        println!("Reading {}...", table);
        let path = Path::new(table);

        let mut headers = Vec::new();

        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file).unwrap();

            reader.metadata().file_metadata().schema_descr().columns().iter().for_each(|x| {
                headers.push(Column { name: x.name().to_string() });
            });

            let mut iter = reader.get_row_iter(None).unwrap();

            let mut data_chunks : Vec<Row> = Vec::new();
            while let Some(record) = iter.next() {
                let row : Row = record.get_column_iter().map(|x| TupleValue{ value: x.1.clone() } ).collect::<Vec<TupleValue>>();
                data_chunks.push(row);
            }

            return Ok(Chunk { headers,  data_chunks });
        } else {
            println!("File not found");
            return Err(Error {});
        }
    }
}
