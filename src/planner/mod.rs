use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};

use crate::{
    storage::{get_table_path, parquet::ParquetReader},
    types::{error::Error, Column},
};

#[derive(Debug)]
pub struct PlanNode {
    pub output_schema: Vec<Column>,
    pub node: Node,
}

#[derive(Debug)]
pub enum Node {
    Scan {
        table_name: String,
        filter: Option<Expr>,
    },
    NestedLoopJoin {
        child_left: Box<PlanNode>,
        child_right: Box<PlanNode>,
        predicate: Option<Expr>,
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

pub struct Planner {}

impl Planner {
    pub fn new() -> Planner {
        Planner {}
    }

    pub fn build_statements(&self, statements: &Vec<Statement>) -> Result<Plan, Error> {
        let mut plans: Vec<Plan> = Vec::new();

        for statement in statements {
            plans.push(self.build_statement(statement)?);
        }

        // TODO
        Ok(plans.pop().unwrap())
    }

    fn build_statement(&self, statement: &Statement) -> Result<Plan, Error> {
        match statement {
            Statement::Query(query) => {
                let Query { ref body, .. } = **query;

                match &**body {
                    SetExpr::Select(select) => {
                        let Select {
                            from,
                            projection,
                            selection,
                            ..
                        } = &**select;

                        // Build FROM
                        let node = self.build_from_clause(from)?;

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
                    _ => Err(Error::Planner("Only SELECT is supported".to_string())),
                }
            }
            _ => Err(Error::Planner(
                "Only Query operations are supported".to_string(),
            )),
        }
    }

    fn build_from_clause(&self, from: &Vec<TableWithJoins>) -> Result<PlanNode, Error> {
        if from.is_empty() {
            return Ok(PlanNode {
                output_schema: Vec::new(),
                node: Node::Empty {},
            });
        }

        let mut node = self.build_table_with_joins(&from[0])?;

        for table in &from[1..] {
            let right = self.build_table_with_joins(table)?;

            let mut output_schema = node.output_schema.clone();
            output_schema.append(&mut right.output_schema.clone());

            node = PlanNode {
                output_schema,
                node: Node::NestedLoopJoin {
                    child_left: Box::new(node),
                    child_right: Box::new(right),
                    predicate: None,
                },
            };
        }

        Ok(node)
    }

    fn build_table_with_joins(&self, table: &TableWithJoins) -> Result<PlanNode, Error> {
        let mut node = self.build_table_factor(&table.relation)?;

        for join in &table.joins {
            let right = self.build_table_factor(&join.relation)?;

            let mut output_schema = node.output_schema.clone();
            output_schema.append(&mut right.output_schema.clone());

            match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(join_constraint) => {
                    node = PlanNode {
                        output_schema,
                        node: Node::NestedLoopJoin {
                            child_left: Box::new(node),
                            child_right: Box::new(right),
                            predicate: match &join_constraint {
                                sqlparser::ast::JoinConstraint::On(ref expr) => Some(expr.clone()),
                                sqlparser::ast::JoinConstraint::None => None,
                                _ => return Err(Error::Planner("Only ON supported".to_string())),
                            },
                        },
                    };
                }
                _ => return Err(Error::Planner("Only INNER JOIN supported".to_string())),
            }
        }

        Ok(node)
    }

    fn build_table_factor(&self, table: &TableFactor) -> Result<PlanNode, Error> {
        match table {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let table_path = get_table_path(&table_name);

                Ok(PlanNode {
                    output_schema: ParquetReader::read_metadata(&table_path)?,
                    node: Node::Scan {
                        table_name,
                        filter: None,
                    },
                })
            }
            _ => Err(Error::Planner("JOIN not supported".to_string())),
        }
    }
}
