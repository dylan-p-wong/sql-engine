use sqlparser::ast::{
    Expr, Function, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
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
                return Err(Error::Planner(format!("Ambiguous field name: {}", name)));
            }

            result_index = Some(i);
        }

        if result_index.is_none() {
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
    Aggregate {
        child: Box<PlanNode>,
        aggregates: Vec<Function>,
        non_aggregates: Vec<SelectItem>,
        group_by: Vec<Expr>,
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

        // TODO(Dylan): We are only using the last plan for now
        Ok(plans.pop().unwrap())
    }

    // changes the expression to swap an aggreate function with an internal identifier that can be used to reference the aggregate later
    fn extract_aggregates(
        &self,
        item: &mut Expr,
        next_aggregate_number: &mut i32,
    ) -> Result<Vec<Function>, Error> {
        match item {
            Expr::Function(function) => {
                // TODO(Dylan): verify that there are no nested aggregates
                let function = function.clone();
                // we replace the function with a new identifier
                *item = Expr::Identifier(Ident::new(format!("#agg{}", next_aggregate_number))); // todo fix the number
                *next_aggregate_number += 1;
                Ok(vec![function])
            }
            Expr::UnaryOp { op, expr } => self.extract_aggregates(expr, next_aggregate_number),
            Expr::BinaryOp { left, op, right } => {
                let mut l = self.extract_aggregates(left, next_aggregate_number)?;
                let mut r = self.extract_aggregates(right, next_aggregate_number)?;

                l.append(&mut r);
                Ok(l)
            }
            _ => Ok(vec![]),
        }
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
                            group_by,
                            having,
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

                        // we need to extract the aggregate functions and handle those separately
                        // we replace the aggregates with an internal identifier #agg0, #agg1, etc.
                        let mut all_aggregates = Vec::new();
                        let mut total_aggregates = 0;
                        let mut non_aggregate_projections = Vec::new();

                        let mut select = projection.clone();

                        for item in select.iter_mut() {
                            match item {
                                SelectItem::UnnamedExpr(ref mut expr) => {
                                    let mut aggregates =
                                        self.extract_aggregates(expr, &mut total_aggregates)?;
                                    if aggregates.is_empty() {
                                        non_aggregate_projections.push(item.clone());
                                    } else {
                                        all_aggregates.append(&mut aggregates);
                                        non_aggregate_projections.append(
                                            &mut self.extract_identifiers_as_select_items(expr),
                                        );
                                    }
                                }
                                SelectItem::ExprWithAlias {
                                    ref mut expr,
                                    alias,
                                } => {
                                    let mut aggregates =
                                        self.extract_aggregates(expr, &mut total_aggregates)?;
                                    if aggregates.is_empty() {
                                        non_aggregate_projections.push(item.clone());
                                    } else {
                                        all_aggregates.append(&mut aggregates);
                                        non_aggregate_projections.append(
                                            &mut self.extract_identifiers_as_select_items(expr),
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }

                        // Build PROJECTION
                        let node = if all_aggregates.is_empty() {
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
                                    SelectItem::Wildcard(_) => {
                                        output_schema.append(&node.output_schema.clone())?;
                                    }
                                    _ => {
                                        return Err(Error::Planner(
                                            "Only UnnamedExpr and ExprWithAlias supported"
                                                .to_string(),
                                        ))
                                    }
                                }
                            }

                            PlanNode {
                                output_schema: self
                                    .get_output_schema_from_projection(&select, &node)?,
                                node: Node::Projection {
                                    select,
                                    child: Box::new(node),
                                },
                            }
                        } else {
                            self.build_aggregate_statement(
                                node,
                                &select.clone(),
                                &mut non_aggregate_projections,
                                &all_aggregates,
                                group_by,
                                having,
                            )?
                        };

                        // Build ORDER BY

                        // Build OFFSET

                        // Build LIMIT

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

    fn extract_identifiers_as_select_items(&self, expr: &Expr) -> Vec<SelectItem> {
        let mut literals = Vec::new();

        match expr {
            Expr::Identifier(ident) => {
                // TODO(Dylan): This is a hack since we do not want to include the aggregates here
                if ident.value.starts_with("#agg") {
                    return literals;
                }
                literals.push(SelectItem::UnnamedExpr(Expr::Identifier(ident.clone())));
            }
            Expr::CompoundIdentifier(ident) => {
                literals.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(
                    ident.clone(),
                )));
            }
            Expr::BinaryOp { left, op, right } => {
                literals.append(&mut self.extract_identifiers_as_select_items(left));
                literals.append(&mut self.extract_identifiers_as_select_items(right));
            }
            Expr::UnaryOp { op, expr } => {
                literals.append(&mut self.extract_identifiers_as_select_items(expr));
            }
            _ => {}
        }

        literals
    }

    // resolves the aggregates, group by and having
    fn build_aggregate_statement(
        &self,
        child: PlanNode,
        end_projection: &Vec<SelectItem>,
        non_aggregate_projections: &mut Vec<SelectItem>,
        aggregates: &Vec<Function>,
        group_by: &Vec<Expr>,
        having: &Option<Expr>,
    ) -> Result<PlanNode, Error> {
        assert!(!aggregates.is_empty());

        // aggregates functions (#agg0, #agg1, etc.) followed by group by followed by non-aggregates we need
        let mut first_projection_with_aggregates_output_schema = OutputSchema::new();

        // add aggregates to the output schema
        for (i, item) in aggregates.iter().enumerate() {
            first_projection_with_aggregates_output_schema.add_column(Column {
                label: Some(item.to_string()),
                table: None,
                column_name: format!("#agg{}", i),
            })?;
        }

        // add non aggregates to the output schema
        first_projection_with_aggregates_output_schema.append(
            &mut self.get_output_schema_from_projection(non_aggregate_projections, &child)?,
        )?;

        // plan the aggregate node
        let mut node = PlanNode {
            output_schema: first_projection_with_aggregates_output_schema,
            node: Node::Aggregate {
                child: Box::new(child),
                aggregates: aggregates.clone(),
                group_by: group_by.clone(),
                non_aggregates: non_aggregate_projections.clone(),
            },
        };

        // TODO(Dylan): plan a filter based on having clause

        // plan a projection to get to the original projection
        node = PlanNode {
            output_schema: self.get_output_schema_from_projection(end_projection, &node)?, // change this
            node: Node::Projection {
                select: end_projection.clone(),
                child: Box::new(node),
            },
        };

        return Ok(node);
    }

    fn get_output_schema_from_projection(
        &self,
        projection: &Vec<SelectItem>,
        child: &PlanNode,
    ) -> Result<OutputSchema, Error> {
        let mut output_schema = OutputSchema::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    output_schema.add_column(Column::new(None, expr.to_string()))?;
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    output_schema
                        .add_column(Column::new(Some(alias.value.clone()), expr.to_string()))?;
                }
                SelectItem::Wildcard(_) => {
                    output_schema.append(&child.output_schema.clone())?;
                }
                _ => {
                    return Err(Error::Planner(
                        "Only UnnamedExpr and ExprWithAlias supported".to_string(),
                    ))
                }
            }
        }

        Ok(output_schema)
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
