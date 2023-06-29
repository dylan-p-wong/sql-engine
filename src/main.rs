use rustyline::DefaultEditor;

mod parser;
mod planner;
mod optimizer;
mod executor;
mod storage;
mod types;

mod database;

fn main() {
    let db = database::Database::new().unwrap();

    let mut rl = DefaultEditor::new().unwrap();

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                let res = db.execute(line.as_str());

                if res.is_err() {
                    println!("{}", res.err().unwrap());
                    continue
                } else {
                    println!("{}", res.unwrap());
                }
            },
            Err(e) => {
                println!("{}", e);
                break
            }
        }
    }
}
