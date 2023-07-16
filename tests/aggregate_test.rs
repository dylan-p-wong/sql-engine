use common::DatabaseTestHelper;
use sqlengine::database::Database;

mod common;

#[test]
fn test_aggregates() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/aggregates.slt")
        .unwrap();
}

#[test]
fn test_aggregates_2() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/aggregates2.slt")
        .unwrap();
}

#[test]
fn test_aggregates_3() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/aggregates3.slt")
        .unwrap();
}

#[test]
fn test_aggregates_4() {
    let db = Database::new().unwrap();
    let db_helper = DatabaseTestHelper(db);
    let mut tester = sqllogictest::Runner::new(db_helper);
    tester
        .run_file("tests/resources/sql/aggregates4.slt")
        .unwrap();
}
