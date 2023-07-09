use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};

use crate::{
    storage::{get_table_path, parquet::ParquetReader},
    types::{error::Error, Column},
};

#[derive(Debug, Default, Clone)]
pub struct OutputSchema {
    pub columns: Vec<Column>,
}

impl OutputSchema {
    pub fn new() -> OutputSchema {
        OutputSchema {
            columns: Vec::new(),
        }
    }

    pub fn get_headers(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|x| x.label.as_ref().unwrap_or(&x.column_name).clone())
            .collect::<Vec<String>>()
    }

    pub fn append(&mut self, output_schema: &OutputSchema) -> Result<(), Error> {
        for column in &output_schema.columns {
            self.add_column(column.clone())?;
        }
        Ok(())
    }

    pub fn add_column(&mut self, column: Column) -> Result<(), Error> {
        self.columns.push(column);
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Result<usize, Error> {
        let mut table_name = None;
        let field_name;

        if name.contains('.') {
            let parts: Vec<_> = name.split('.').collect();

            if parts.len() != 2 {
                return Err(Error::Planner(format!("Invalid field name: {}", name)));
            }

            table_name = Some(parts[0]);
            field_name = Some(parts[1]);
        } else {
            field_name = Some(name);
        }

        let mut result_index = None;

        for (i, column) in self.columns.iter().enumerate() {
            if table_name.is_some()
                && (column.table.is_none() || table_name.unwrap() != column.table.as_ref().unwrap())
            {
                continue;
            }

            if field_name.is_some() && field_name.unwrap() != column.column_name {
                continue;
            }

            if result_index.is_some() {
                return Err(Error::Planner("Ambiguous field name".to_string()));
            }

            result_index = Some(i);
        }

        if result_index.is_none() {
            println!("{:?}", self.columns);
            return Err(Error::Planner(format!("Field not found: {}", name)));
        }

        Ok(result_index.unwrap())
    }
}

#[derive(Debug)]
pub struct PlanNode {
    pub output_schema: OutputSchema,
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
                                let mut output_schema = OutputSchema::new();
                                for item in &select {
                                    match item {
                                        SelectItem::UnnamedExpr(expr) => {
                                            output_schema
                                                .add_column(Column::new(None, expr.to_string()))?;
                                        }
                                        SelectItem::ExprWithAlias { expr, alias } => {
                                            output_schema.add_column(Column::new(
                                                Some(alias.value.clone()),
                                                expr.to_string(),
                                            ))?;
                                        }
                                        _ => {
                                            return Err(Error::Planner(
                                                "Only UnnamedExpr and ExprWithAlias supported"
                                                    .to_string(),
                                            ))
                                        }
                                    }
                                }
                                output_schema
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
                output_schema: OutputSchema::new(),
                node: Node::Empty {},
            });
        }

        let mut node = self.build_table_with_joins(&from[0])?;

        for table in &from[1..] {
            let right = self.build_table_with_joins(table)?;

            let mut output_schema = node.output_schema.clone();
            output_schema.append(&right.output_schema)?;

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
            output_schema.append(&right.output_schema)?;

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
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_path = get_table_path(&table_name);

                let mut output_schema = ParquetReader::read_metadata(&table_path)?;

                // we add a table name to each column
                for column in &mut output_schema.columns {
                    if alias.is_some() {
                        column.table = Some(alias.as_ref().unwrap().to_string());
                    } else {
                        column.table = Some(table_name.clone());
                    }
                }

                Ok(PlanNode {
                    output_schema,
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
