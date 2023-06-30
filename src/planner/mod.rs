use std::fmt::Error;

use sqlparser::ast::{Statement, Query, SetExpr, Select, Expr, SelectItem};

pub enum Node {
    Scan {
        table_name: String,
        filter: Option<Expr>,
    },
    Filter {
        filter: Expr,
        child: Box<Node>,
    },
    Projection {
        select: Vec<SelectItem>,
        child: Box<Node>,
    },
    Empty {},
}

pub struct Plan {
    pub root: Node,
}

impl Plan {
    pub fn new(statement : &Statement) -> Result<Plan, Error> {
        match statement {
            Statement::Query(query) => {
                println!("Query Statement");
                
                let Query { ref body, ref order_by, ref limit, ref offset, .. } = **query;

                match &**body {
                    SetExpr::Select(select) => {
                        println!("Select Statement");
                        let Select { from, projection, selection, group_by, .. } = &**select;
                        
                        // Build FROM
                        let node = if from.len() > 1 {
                            println!("Unsupported");
                            return Err(Error {})
                        } else if from.len() == 0 {
                            Node::Empty {}
                        } else {
                            let table_name = match &from[0].relation {
                                sqlparser::ast::TableFactor::Table { name, .. } => {
                                    name.to_string()
                                },
                                _ => {
                                    println!("Unsupported");
                                    return Err(Error {})
                                },
                            };

                            Node::Scan {
                                table_name: table_name,
                                filter: None,
                            }
                        };

                        // Build WHERE
                        let node = if selection.is_some() {
                            let filter = selection.as_ref().unwrap();
                            Node::Filter { filter: filter.clone(), child: Box::new(node) }
                        } else {
                            node
                        };

                        // Build PROJECTION
                        let node = if projection.len() > 0 {
                            let select = projection.clone();
                            Node::Projection { select: select, child: Box::new(node) }
                        } else {
                            node
                        };

                        Ok(Plan {
                            root: node,
                        })
                    },
                    _ => {
                        return Err(Error {})
                    },
                }
            },
            _ => {
                println!("Unsupported");
                return Err(Error {});
            }
        }
    }
}

pub struct Planner {}

impl Planner {
    pub fn new() -> Planner {
        Planner {}
    }

    pub fn build(&self, statements : &Vec<Statement>) -> Result<Plan, Error> {
        println!("Building Plan...");

        let mut plans : Vec<Plan> = Vec::new();

        for statement in statements {
            plans.push(Plan::new(&statement)?);
        }

        Ok(plans.pop().unwrap())
    }
}
