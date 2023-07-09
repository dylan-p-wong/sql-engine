use sqlengine::{database::Database, types::error::Error};
use sqllogictest::{self, DBOutput, DefaultColumnType};

pub struct DatabaseTestHelper(pub Database);

impl sqllogictest::DB for DatabaseTestHelper {
    type Error = Error;
    type ColumnType = DefaultColumnType;
    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let result_set = self.0.execute(sql)?;
        let types = vec![DefaultColumnType::Any; result_set.output_schema.columns.len()];
        let rows = result_set
            .data_chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .data_chunks
                    .iter()
                    .map(|row| row.iter().map(|cell| cell.to_string()).collect())
            })
            .collect();
        Ok(DBOutput::Rows { types, rows })
    }
}
