use std::fmt;

use parquet::record::Field;
use tabled::{builder::Builder, settings::Style};

#[derive(Debug, Clone)]
pub struct TupleValue {
    pub value: Field,
}

impl Into<std::string::String> for &TupleValue {
    fn into(self) -> std::string::String {
        match &self.value {
            Field::Bool(b) => b.to_string(),
            Field::Int(i) => i.to_string(),
            Field::Float(f) => f.to_string(),
            Field::Double(d) => d.to_string(),
            Field::Null => String::from("NULL"),
            _ => String::from("-"),
        }
    }
}

pub type Row = Vec<TupleValue>;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

#[derive(Default, Clone)]
pub struct Chunk {
    pub data_chunks: Vec<Row>,
}

#[derive(Default)]
pub struct ResultSet {
    pub output_schema: Vec<Column>,
    pub data_chunks: Vec<Chunk>,
}

impl fmt::Display for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = Builder::default();

        let headers = self
            .output_schema
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>();
        builder.set_header(headers);

        for chunk in self.data_chunks.iter() {
            for row in chunk.data_chunks.iter() {
                builder.push_record(row);
            }
        }

        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{}", table);
        Ok(())
    }
}
