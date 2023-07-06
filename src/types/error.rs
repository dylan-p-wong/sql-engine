use std::fmt::{Display, Formatter, Result};

#[derive(Clone, Debug)]
pub enum Error {
    Parser(String),
    Planner(String),
    Execution(String),
    Expression(String),
    Storage(String),
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::Parser(msg) => msg,
            Error::Planner(msg) => msg,
            Error::Execution(msg) => msg,
            Error::Expression(msg) => msg,
            Error::Storage(msg) => msg,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Error::Parser(msg) => write!(f, "Parser Error: {}", msg),
            Error::Planner(msg) => write!(f, "Planner Error: {}", msg),
            Error::Execution(msg) => write!(f, "Execution Error: {}", msg),
            Error::Expression(msg) => write!(f, "Expression Error: {}", msg),
            Error::Storage(msg) => write!(f, "Storage Error: {}", msg),
        }
    }
}
