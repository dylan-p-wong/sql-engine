use std::collections::HashSet;

use sqlparser::ast::{
    Expr, Function, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};

use crate::{
    storage::{get_table_path, parquet::ParquetReader},
    types::{error::Error, parse_identifer, Column},
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
        let (field_name, table_name) = parse_identifer(name)?;

        let mut result_index = None;

        for (i, column) in self.columns.iter().enumerate() {
            if table_name.is_some()
                && (column.table.is_none()
                    || table_name.as_ref().unwrap() != column.table.as_ref().unwrap())
            {
                continue;
            }

            if field_name != column.column_name {
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
    Limit {
        limit: u64,
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

        if plans.is_empty() {
            return Err(Error::Planner("No statements found".to_string()));
        }

        // TODO(Dylan): We are only using the last plan for now
        Ok(plans.pop().unwrap())
    }

    fn build_statement(&self, statement: &Statement) -> Result<Plan, Error> {
        match statement {
            Statement::Query(query) => {
                let Query {
                    ref body,
                    ref limit,
                    ..
                } = **query;

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
                        let node = self.build_where_clause(selection, node)?;

                        // Build PROJECTION
                        let mut select_items = projection.clone();
                        let mut having_items = having.clone();

                        // We extract the aggregates and the select items and the having clause
                        let (all_aggregates, non_aggregate_projections) =
                            self.extract_aggregates(&mut select_items, &mut having_items)?;

                        let node = if !all_aggregates.is_empty() || !(*group_by).is_empty() {
                            self.build_aggregate_statement(
                                node,
                                &select_items.clone(),
                                &non_aggregate_projections,
                                &all_aggregates,
                                group_by,
                                &having_items,
                            )?
                        } else {
                            if having.is_some() {
                                return Err(Error::Planner(
                                    "HAVING clause without aggregates not supported".to_string(),
                                ));
                            }

                            self.build_non_aggregate_statement(node, &select_items)?
                        };

                        // Build ORDER BY

                        // Build OFFSET

                        // Build LIMIT
                        let node = self.build_limit_clause(node, limit.clone())?;

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

    // this function returns the aggregate functions and the select items without aggregates if aggregates are found or else None
    #[allow(clippy::type_complexity)]
    fn extract_aggregates(
        &self,
        select_items: &mut [SelectItem],
        having: &mut Option<Expr>,
    ) -> Result<(Vec<Function>, Vec<SelectItem>), Error> {
        // we need to extract the aggregate functions and handle those separately and extract the identifiers in the select items with aggregate functions
        // this allows to to get all the values we need to perform the aggregate functions and projections
        // we replace the aggregates with an internal identifier #agg0, #agg1, etc.
        // alias is handled later in the final projection

        let mut all_aggregates = Vec::new();
        let mut total_aggregates = 0;
        let mut non_aggregate_projections = Vec::new();
        let mut seen = HashSet::new();

        for item in select_items.iter_mut() {
            match item {
                SelectItem::UnnamedExpr(ref mut expr) => {
                    let mut aggregates =
                        Self::extract_aggregates_from_expr(expr, &mut total_aggregates)?;
                    if aggregates.is_empty() {
                        non_aggregate_projections.append(
                            &mut Self::extract_identifiers_as_select_items(expr, &mut seen),
                        );
                    } else {
                        all_aggregates.append(&mut aggregates);
                        non_aggregate_projections.append(
                            &mut Self::extract_identifiers_as_select_items(expr, &mut seen),
                        );
                    }
                }
                SelectItem::ExprWithAlias { ref mut expr, .. } => {
                    let mut aggregates =
                        Self::extract_aggregates_from_expr(expr, &mut total_aggregates)?;
                    if aggregates.is_empty() {
                        non_aggregate_projections.append(
                            &mut Self::extract_identifiers_as_select_items(expr, &mut seen),
                        );
                    } else {
                        all_aggregates.append(&mut aggregates);
                        non_aggregate_projections.append(
                            &mut Self::extract_identifiers_as_select_items(expr, &mut seen),
                        );
                    }
                }
                _ => {}
            }
        }

        if having.is_some() {
            let mut aggregates = Self::extract_aggregates_from_expr(
                having.as_mut().unwrap(),
                &mut total_aggregates,
            )?;
            if aggregates.is_empty() {
                non_aggregate_projections.append(&mut Self::extract_identifiers_as_select_items(
                    having.as_ref().unwrap(),
                    &mut seen,
                ));
            } else {
                all_aggregates.append(&mut aggregates);
                non_aggregate_projections.append(&mut Self::extract_identifiers_as_select_items(
                    having.as_ref().unwrap(),
                    &mut seen,
                ));
            }
        }

        Ok((all_aggregates, non_aggregate_projections))
    }

    // builds a projection for select items without aggregates, group by or having
    fn build_non_aggregate_statement(
        &self,
        child: PlanNode,
        end_projection: &Vec<SelectItem>,
    ) -> Result<PlanNode, Error> {
        let node = PlanNode {
            output_schema: self.get_output_schema_from_projection(end_projection, &child)?,
            node: Node::Projection {
                select: end_projection.clone(),
                child: Box::new(child),
            },
        };
        Ok(node)
    }

    // resolves the aggregates, group by and having
    fn build_aggregate_statement(
        &self,
        child: PlanNode,
        end_projection: &Vec<SelectItem>,
        non_aggregate_projections: &Vec<SelectItem>,
        aggregates: &Vec<Function>,
        group_by: &[Expr],
        having: &Option<Expr>,
    ) -> Result<PlanNode, Error> {
        assert!(!aggregates.is_empty() || !group_by.is_empty());

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
        first_projection_with_aggregates_output_schema
            .append(&self.get_output_schema_from_projection(non_aggregate_projections, &child)?)?;

        // plan the aggregate node
        let mut node = PlanNode {
            output_schema: first_projection_with_aggregates_output_schema,
            node: Node::Aggregate {
                child: Box::new(child),
                aggregates: aggregates.clone(),
                group_by: group_by.to_vec(),
                non_aggregates: non_aggregate_projections.clone(),
            },
        };

        // plan a filter based on having clause
        if having.is_some() {
            node = PlanNode {
                output_schema: node.output_schema.clone(),
                node: Node::Filter {
                    filter: having.as_ref().unwrap().clone(),
                    child: Box::new(node),
                },
            };
        }

        // plan a projection to get to the original projection
        node = PlanNode {
            output_schema: self.get_output_schema_from_projection(end_projection, &node)?,
            node: Node::Projection {
                select: end_projection.clone(),
                child: Box::new(node),
            },
        };

        Ok(node)
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
                    match expr {
                        // we only add the name if it is an identifier or compound identifier
                        Expr::Identifier(_) => {
                            output_schema.add_column(Column::new(
                                Some(expr.to_string()),
                                expr.to_string(),
                            )?)?;
                        }
                        Expr::CompoundIdentifier(_) => {
                            output_schema.add_column(Column::new(
                                Some(expr.to_string()),
                                expr.to_string(),
                            )?)?;
                        }
                        _ => {
                            output_schema
                                .add_column(Column::new(Some(expr.to_string()), "".to_string())?)?;
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    match expr {
                        // we only add the name if it is an identifier or compound identifier
                        Expr::Identifier(_) => {
                            output_schema.add_column(Column::new(
                                Some(alias.value.clone()),
                                expr.to_string(),
                            )?)?;
                        }
                        Expr::CompoundIdentifier(_) => {
                            output_schema.add_column(Column::new(
                                Some(alias.value.clone()),
                                expr.to_string(),
                            )?)?;
                        }
                        _ => {
                            output_schema.add_column(Column::new(
                                Some(alias.value.clone()),
                                "".to_string(),
                            )?)?;
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    output_schema.append(&child.output_schema.clone())?;
                }
                _ => return Err(Error::Planner(format!("{} not supported", item))),
            }
        }

        Ok(output_schema)
    }

    #[allow(dead_code)]
    fn replace_wildcards(
        &self,
        projection: Vec<SelectItem>,
        child: &PlanNode,
    ) -> Result<Vec<SelectItem>, Error> {
        let mut res = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(_) => res.push(item.clone()),
                SelectItem::ExprWithAlias { .. } => res.push(item.clone()),
                SelectItem::Wildcard(_) => {
                    for column in &child.output_schema.columns {
                        res.push(column.as_select_item());
                    }
                }
                _ => return Err(Error::Planner(format!("{} not supported", item))),
            }
        }

        Ok(res)
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

    fn build_where_clause(
        &self,
        selection: &Option<Expr>,
        child: PlanNode,
    ) -> Result<PlanNode, Error> {
        if selection.is_some() {
            let filter = selection.as_ref().unwrap();
            Ok(PlanNode {
                output_schema: child.output_schema.clone(),
                node: Node::Filter {
                    filter: filter.clone(),
                    child: Box::new(child),
                },
            })
        } else {
            Ok(child)
        }
    }

    // changes the expression to swap an aggreate function with an internal identifier that can be used to reference the aggregate later
    fn extract_aggregates_from_expr(
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
            Expr::UnaryOp { op: _op, expr } => {
                Self::extract_aggregates_from_expr(expr, next_aggregate_number)
            }
            Expr::BinaryOp {
                left,
                op: _op,
                right,
            } => {
                let mut l = Self::extract_aggregates_from_expr(left, next_aggregate_number)?;
                let mut r = Self::extract_aggregates_from_expr(right, next_aggregate_number)?;

                l.append(&mut r);
                Ok(l)
            }
            _ => Ok(vec![]),
        }
    }

    // extracts the identifiers from an expression
    fn extract_identifiers_as_select_items(
        expr: &Expr,
        seen: &mut HashSet<String>,
    ) -> Vec<SelectItem> {
        let mut literals = Vec::new();

        match expr {
            Expr::Identifier(ident) => {
                // TODO(Dylan): This is a hack since we do not want to include the aggregates here
                if ident.value.starts_with("#agg") {
                    return literals;
                }

                // This removes duplicates identifiers
                if !seen.contains(&ident.value) {
                    literals.push(SelectItem::UnnamedExpr(Expr::Identifier(ident.clone())));
                    seen.insert(ident.value.clone());
                }
            }
            Expr::CompoundIdentifier(..) => {
                if !seen.contains(expr.to_string().as_str()) {
                    literals.push(SelectItem::UnnamedExpr(expr.clone()));
                    seen.insert(expr.to_string());
                }
            }
            Expr::BinaryOp { left, op: _, right } => {
                literals.append(&mut Self::extract_identifiers_as_select_items(left, seen));
                literals.append(&mut Self::extract_identifiers_as_select_items(right, seen));
            }
            Expr::UnaryOp { op: _, expr } => {
                literals.append(&mut Self::extract_identifiers_as_select_items(expr, seen));
            }
            _ => {}
        }

        literals
    }

    fn build_limit_clause(&self, child: PlanNode, limit: Option<Expr>) -> Result<PlanNode, Error> {
        if limit.is_some() {
            let limit = limit.as_ref().unwrap();
            let limit = match limit {
                Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<u64>().unwrap(),
                _ => {
                    return Err(Error::Planner(
                        "Only numbers supported for limit clause".to_string(),
                    ))
                }
            };

            Ok(PlanNode {
                output_schema: child.output_schema.clone(),
                node: Node::Limit {
                    limit,
                    child: Box::new(child),
                },
            })
        } else {
            Ok(child)
        }
    }
}
