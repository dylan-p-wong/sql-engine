use std::fmt::Error;

use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement};

use crate::{
    storage::{get_table_path, parquet::ParquetReader},
    types::Column,
};

pub struct PlanNode {
    pub output_schema: Vec<Column>,
    pub node: Node,
}

pub enum Node {
    Scan {
        table_name: String,
        filter: Option<Expr>,
    },
    Filter {
        filter: Expr,
        child: Box<PlanNode>,
    },
    Projection {
        select: Vec<SelectItem>,
        child: Box<PlanNode>,
    },
    Empty {},
}

pub struct Plan {
    pub root: PlanNode,
}

impl Plan {
    pub fn new(statement: &Statement) -> Result<Plan, Error> {
        match statement {
            Statement::Query(query) => {
                let Query {
                    ref body,
                    ref order_by,
                    ref limit,
                    ref offset,
                    ..
                } = **query;

                match &**body {
                    SetExpr::Select(select) => {
                        let Select {
                            from,
                            projection,
                            selection,
                            group_by,
                            ..
                        } = &**select;

                        // Build FROM
                        let node = if from.len() > 1 {
                            return Err(Error {});
                        } else if from.is_empty() {
                            PlanNode {
                                output_schema: Vec::new(),
                                node: Node::Empty {},
                            }
                        } else {
                            let table_name = match &from[0].relation {
                                sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                                _ => {
                                    return Err(Error {});
                                }
                            };

                            let table_path = get_table_path(&table_name);

                            PlanNode {
                                output_schema: ParquetReader::read_metadata(&table_path)?,
                                node: Node::Scan {
                                    table_name,
                                    filter: None,
                                },
                            }
                        };

                        // Build WHERE
                        let node = if selection.is_some() {
                            let filter = selection.as_ref().unwrap();
                            PlanNode {
                                output_schema: node.output_schema.clone(),
                                node: Node::Filter {
                                    filter: filter.clone(),
                                    child: Box::new(node),
                                },
                            }
                        } else {
                            node
                        };

                        // Build PROJECTION
                        let node = if !projection.is_empty() {
                            let select = projection.clone();
                            let headers = if select.len() == 1 && select[0].to_string() == "*" {
                                node.output_schema.clone()
                            } else {
                                select
                                    .iter()
                                    .map(|x| Column {
                                        name: x.to_string(),
                                    })
                                    .collect::<Vec<Column>>()
                            };

                            PlanNode {
                                output_schema: headers,
                                node: Node::Projection {
                                    select,
                                    child: Box::new(node),
                                },
                            }
                        } else {
                            node
                        };

                        Ok(Plan { root: node })
                    }
                    _ => Err(Error {}),
                }
            }
            _ => Err(Error {}),
        }
    }
}

pub struct Planner {}

impl Planner {
    pub fn new() -> Planner {
        Planner {}
    }

    pub fn build(&self, statements: &Vec<Statement>) -> Result<Plan, Error> {
        let mut plans: Vec<Plan> = Vec::new();

        for statement in statements {
            plans.push(Plan::new(statement)?);
        }

        // TODO
        Ok(plans.pop().unwrap())
    }
}
