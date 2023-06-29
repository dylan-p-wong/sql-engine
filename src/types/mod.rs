
use std::fmt;

use parquet::record::Field;
use tabled::{builder::Builder, settings::Style};

#[derive(Debug)]
pub struct TupleValue {
    pub value : Field,
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
    pub name : String,
}

pub struct Chunk {
    pub headers : Vec<Column>,
    pub data_chunks : Vec<Row>
}

impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = Builder::default();
        let cols = self.headers.iter().map(|x| x.name.clone()).collect::<Vec<String>>();
        builder.set_header(cols);

        for row in self.data_chunks.iter() {
            builder.push_record(row);
        }

        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{}", table);
        Ok(())
    }
}
