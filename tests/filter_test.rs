use common::DatabaseTestHelper;
use sqlengine::database::Database;

mod common;

#[test]
fn test_filter() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester.run_file("tests/resources/sql/filter.slt").unwrap();
}
