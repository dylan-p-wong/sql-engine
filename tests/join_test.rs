use common::DatabaseTestHelper;
use sqlengine::database::Database;

mod common;

#[test]
fn test_cartesian_join() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/cartesian.slt")
        .unwrap();
}

#[test]
fn test_inner_join() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/inner_join.slt")
        .unwrap();
}
