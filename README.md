# Vectorized SQL Engine󠁓

This is a project made to learn to implement query planning, optimization and execution algorithms. It is written in Rust and operates on the Parquet file format.

## 📖 Table of Contents

- [Vectorized SQL Engine󠁓](#vectorized-sql-engine)
  - [📖 Table of Contents](#-table-of-contents)
  - [⚙️ Installation](#️-installation)
  - [📊 Usage](#-usage)
  - [📝 About](#-about)
  - [🧪 Testing](#-testing)
  - [⏱️ Benchmarks](#️-benchmarks)
  - [💻 Contributing](#-contributing)

## ⚙️ Installation

You can build the project from source with the standard Rust toolchain.
```sh
git clone https://github.com/dylan-p-wong/sql-engine.git
cd sql-engine
cargo build --release
```

## 📊 Usage

After building from source you can run the executable.
```sh
./target/release/sqlengine
```

This will start an read–eval–print loop you can use to execute queries.

```
>> select * from 'tests/resources/data/movies1.parquet'
╭──────────────────────────┬────────┬────────┬────────┬──────╮
│ movie                    │ score1 │ score2 │ score3 │ year │
├──────────────────────────┼────────┼────────┼────────┼──────┤
│ taxi driver              │ 9      │ 8      │ 7      │ 1976 │
│ lion king                │ 2      │ 3      │ 5      │ 1994 │
│ drive                    │ 10     │ 9      │ 4      │ 2011 │
│ avengers                 │ 2      │ 1      │ 6      │ 2012 │
│ django                   │ 8      │ 8      │ 8      │ 2012 │
│ the shawshank redemption │ 10     │ 9      │ 10     │ 1994 │
│ a star is born           │ 5      │ 6      │ 6      │ 1976 │
│ carrie                   │ 2      │ 3      │ 0      │ 1976 │
╰──────────────────────────┴────────┴────────┴────────┴──────╯
```

Many SQL statements are supported and since this project is still having features added here are a few examples.

**Aggregates**
```
>> select min(score1) as min, max(score1) as max, sum(score1) as sum, avg(score1) as avg, year from 'tests/resources/data/movies1.parquet' group by year
╭─────┬─────┬─────┬───────────┬──────╮
│ min │ max │ sum │ avg       │ year │
├─────┼─────┼─────┼───────────┼──────┤
│ 2   │ 10  │ 12  │ 6         │ 1994 │
│ 2   │ 8   │ 10  │ 5         │ 2012 │
│ 2   │ 9   │ 16  │ 5.3333335 │ 1976 │
│ 10  │ 10  │ 10  │ 10        │ 2011 │
╰─────┴─────┴─────┴───────────┴──────╯
```

**Joins**
```
>> select movies1.movie, movies2.movie, movies1.score1 from 'tests/resources/data/movies1.parquet' as movies1 join 'tests/resources/data/movies1.parquet' as movies2 where movies1.score1=movies2.score1 and movies1.movie != movies2.movie
╭──────────────────────────┬──────────────────────────┬────────╮
│ movie                    │ movie                    │ score1 │
├──────────────────────────┼──────────────────────────┼────────┤
│ lion king                │ avengers                 │ 2      │
│ lion king                │ carrie                   │ 2      │
│ drive                    │ the shawshank redemption │ 10     │
│ avengers                 │ lion king                │ 2      │
│ avengers                 │ carrie                   │ 2      │
│ the shawshank redemption │ drive                    │ 10     │
│ carrie                   │ lion king                │ 2      │
│ carrie                   │ avengers                 │ 2      │
╰──────────────────────────┴──────────────────────────┴────────╯
```
**And more...**

## 📝 About

This project uses [sqlparser](https://docs.rs/sqlparser/latest/sqlparser/) for parsing queries, [parquet](https://docs.rs/parquet/latest/parquet/) for reading from disk, the project revolves around creating everything inbetween. It operates on vectors of rows of size **1024** in a pull-based manner. It focuses on planning, optimizing and executing analytical queries.

## 🧪 Testing

This project uses [sqllogictest](https://docs.rs/sqllogictest/latest/sqllogictest/) for testing SQL queries. The tests reside the the ```/tests``` directory. There are only these integration tests currently to maintain internal flexibility for faster development.

You can run the test suite by simply running ```cargo test```.

## ⏱️ Benchmarks

📝 TODO

## 💻 Contributing

This project is a personal project for learning but if you want to contribute new features or fixes you can [open a pull request](https://github.com/dylan-p-wong/sql-engine/compare/).

The project requires code to be formatted using ```cargo fmt``` and linted using ```cargo clippy```.
