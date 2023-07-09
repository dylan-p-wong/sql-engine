use common::DatabaseTestHelper;
use sqlengine::database::Database;

mod common;

#[test]
fn test_expression() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/expression.slt")
        .unwrap();
}

#[test]
fn test_binary_operators() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/binary_operators.slt")
        .unwrap();
}
