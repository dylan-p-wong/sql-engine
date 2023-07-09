use std::fmt;

use parquet::record::Field;
use tabled::{builder::Builder, settings::Style};

use crate::planner::OutputSchema;

pub mod error;

#[derive(Debug, Clone)]
pub struct TupleValue {
    pub value: Field,
}

impl fmt::Display for TupleValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(self.value.to_string().as_str())?;
        Ok(())
    }
}

impl From<&TupleValue> for std::string::String {
    fn from(val: &TupleValue) -> Self {
        match &val.value {
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
    pub label: Option<String>,
    pub table: Option<String>,
    pub column_name: String,
}

impl Column {
    pub fn new(label: Option<String>, name: String) -> Column {
        let mut table = None;
        let column_name;

        if name.contains('.') {
            let parts: Vec<_> = name.split('.').collect();
            table = Some(parts[0].to_string());
            column_name = parts[1];
        } else {
            column_name = &name;
        }

        Column {
            label,
            table,
            column_name: column_name.to_string(),
        }
    }
}

#[derive(Default, Clone)]
pub struct Chunk {
    pub data_chunks: Vec<Row>,
}

#[derive(Default)]
pub struct ResultSet {
    pub output_schema: OutputSchema,
    pub data_chunks: Vec<Chunk>,
}

impl ResultSet {
    pub fn new(output_schema: OutputSchema) -> ResultSet {
        ResultSet {
            output_schema,
            data_chunks: Vec::new(),
        }
    }
}

impl fmt::Display for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = Builder::default();

        let headers = self.output_schema.get_headers();
        builder.set_header(headers);

        for chunk in self.data_chunks.iter() {
            for row in chunk.data_chunks.iter() {
                builder.push_record(row);
            }
        }

        let mut table = builder.build();
        table.with(Style::rounded());
        writeln!(f, "{}", table)?;
        Ok(())
    }
}
