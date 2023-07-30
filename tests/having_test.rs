use common::DatabaseTestHelper;
use sqlengine::database::Database;

mod common;

#[test]
fn test_having() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester.run_file("tests/resources/sql/having.slt").unwrap();
}
