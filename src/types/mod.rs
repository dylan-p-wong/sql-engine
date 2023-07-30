use std::fmt;

use parquet::record::Field;
use regex::Regex;
use sqlparser::ast::SelectItem;
use tabled::{builder::Builder, settings::Style};

use crate::planner::OutputSchema;

use self::error::Error;

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
            Field::Str(s) => s.to_string(),
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
    pub fn new(label: Option<String>, name: String) -> Result<Column, Error> {
        let (column_name, table) = parse_identifer(&name)?;

        Ok(Column {
            label,
            table,
            column_name,
        })
    }

    #[allow(dead_code)]
    pub fn as_select_item(&self) -> SelectItem {
        let mut ident_name = self.column_name.clone();
        if let Some(table) = &self.table {
            ident_name = format!("{}.{}", table, self.column_name);
        }

        let ident = sqlparser::ast::Ident::new(ident_name);

        if let Some(label) = &self.label {
            let alias = sqlparser::ast::Ident::new(label);

            SelectItem::ExprWithAlias {
                expr: sqlparser::ast::Expr::Identifier(ident),
                alias,
            }
        } else {
            SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(ident))
        }
    }
}

pub fn parse_identifer(name: &str) -> Result<(String, Option<String>), Error> {
    let mut table_name = None;
    let field_name;

    if name.contains('.') {
        if name.starts_with('\'') {
            let re = Regex::new(r"(?<table>'.+')\.(?<column>.+)").unwrap();

            let Some(caps) = re.captures(name) else {
                    return Err(Error::Planner(format!("Invalid field name: {}", name)));
                };

            if caps.name("table").is_none() || caps.name("column").is_none() {
                return Err(Error::Planner(format!("Invalid field name: {}", name)));
            }

            table_name = Some(caps.name("table").unwrap().as_str());
            field_name = Some(caps.name("column").unwrap().as_str());
        } else {
            let parts: Vec<_> = name.split('.').collect();

            if parts.len() != 2 {
                return Err(Error::Planner(format!("Invalid field name: {}", name)));
            }

            table_name = Some(parts[0]);
            field_name = Some(parts[1]);
        }
    } else {
        field_name = Some(name);
    }

    Ok((
        field_name.unwrap().to_string(),
        table_name.map(|s| s.to_string()),
    ))
}

#[derive(Default, Clone)]
pub struct Chunk {
    data_chunks: Vec<Row>,
}

impl Chunk {
    pub fn new() -> Chunk {
        Chunk {
            data_chunks: Vec::new(),
        }
    }
    pub fn add_row(&mut self, row: Row) {
        self.data_chunks.push(row);
    }
    pub fn size(&self) -> usize {
        self.data_chunks.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data_chunks.is_empty()
    }
    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data_chunks
    }
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
