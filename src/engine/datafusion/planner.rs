//! Translate the supported Spark Connect relations directly into DataFusion plans.

use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use ::datafusion::common::{Column, ScalarValue};
use ::datafusion::dataframe::DataFrame;
use ::datafusion::datasource::MemTable;
use ::datafusion::execution::context::SQLOptions;
use ::datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use ::datafusion::logical_expr::expr_fn::cast;
use ::datafusion::logical_expr::{Expr, ExprSchemable, SortExpr};
use ::datafusion::prelude::{col, lit, SessionContext};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use spark_connect_proto as proto;
use tonic::Status;

use crate::schema::{arrow_type, local_schema};

const MAX_DEPTH: usize = 64;
type PlanFuture<'a> = Pin<Box<dyn Future<Output = Result<DataFrame, Status>> + Send + 'a>>;

pub async fn plan(ctx: &SessionContext, relation: &proto::Relation) -> Result<DataFrame, Status> {
    plan_at(ctx, relation, 0).await
}

fn plan_at<'a>(
    ctx: &'a SessionContext,
    relation: &'a proto::Relation,
    depth: usize,
) -> PlanFuture<'a> {
    Box::pin(async move {
        if depth > MAX_DEPTH {
            return Err(invalid("relation nesting exceeds 64"));
        }
        use proto::relation::RelType;
        match relation
            .rel_type
            .as_ref()
            .ok_or_else(|| invalid("missing relation type"))?
        {
            RelType::Sql(sql) => {
                #[allow(deprecated)]
                if !sql.args.is_empty()
                    || !sql.pos_args.is_empty()
                    || !sql.named_arguments.is_empty()
                    || !sql.pos_arguments.is_empty()
                {
                    return Err(unsupported("SQL parameters"));
                }
                let options = SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false);
                ctx.sql_with_options(&sql.query, options)
                    .await
                    .map_err(df_error)
            }
            RelType::Range(range) => range_plan(ctx, range),
            RelType::Project(project) => {
                let input = match project.input.as_deref() {
                    Some(input) => plan_at(ctx, input, depth + 1).await?,
                    None => ctx.read_empty().map_err(df_error)?,
                };
                if project.expressions.is_empty() {
                    return Err(invalid("Project requires expressions"));
                }
                let mut expressions = Vec::new();
                for expression in &project.expressions {
                    let wildcard = match &expression.expr_type {
                        Some(proto::expression::ExprType::UnresolvedStar(star)) => {
                            if star.unparsed_target.is_some() || star.plan_id.is_some() {
                                return Err(unsupported("qualified or plan-bound star"));
                            }
                            true
                        }
                        Some(proto::expression::ExprType::UnresolvedAttribute(attribute))
                            if attribute.unparsed_identifier == "*" =>
                        {
                            if attribute.plan_id.is_some()
                                || attribute.is_metadata_column == Some(true)
                            {
                                return Err(unsupported("plan-bound or metadata star"));
                            }
                            true
                        }
                        Some(proto::expression::ExprType::UnresolvedAttribute(attribute))
                            if attribute.unparsed_identifier.ends_with(".*") =>
                        {
                            return Err(unsupported("qualified star"));
                        }
                        _ => false,
                    };
                    if wildcard {
                        expressions.extend(input.schema().columns().into_iter().map(Expr::Column));
                    } else {
                        expressions.push(expr(&input, expression, 0)?);
                    }
                }
                input.select(expressions).map_err(df_error)
            }
            RelType::ToDf(rename) => {
                let input = plan_at(
                    ctx,
                    required(rename.input.as_deref(), "ToDF input")?,
                    depth + 1,
                )
                .await?;
                if rename.column_names.len() != input.schema().fields().len() {
                    return Err(invalid("ToDF requires one name per input column"));
                }
                let distinct: std::collections::HashSet<_> = rename.column_names.iter().collect();
                if distinct.len() != rename.column_names.len() {
                    return Err(unsupported("duplicate ToDF column names"));
                }
                let expressions: Vec<_> = input
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(&rename.column_names)
                    .map(|(column, name)| Expr::Column(column).alias(name))
                    .collect();
                input.select(expressions).map_err(df_error)
            }
            RelType::Filter(filter) => {
                let input = plan_at(
                    ctx,
                    required(filter.input.as_deref(), "Filter input")?,
                    depth + 1,
                )
                .await?;
                let predicate = expr(
                    &input,
                    required(filter.condition.as_ref(), "Filter condition")?,
                    0,
                )?;
                input.filter(predicate).map_err(df_error)
            }
            RelType::Limit(limit) => {
                let count = usize::try_from(limit.limit).map_err(|_| invalid("negative limit"))?;
                plan_at(
                    ctx,
                    required(limit.input.as_deref(), "Limit input")?,
                    depth + 1,
                )
                .await?
                .limit(0, Some(count))
                .map_err(df_error)
            }
            RelType::Sort(sort) => {
                if sort.is_global == Some(false) {
                    return Err(unsupported("partition-local sort"));
                }
                let input = plan_at(
                    ctx,
                    required(sort.input.as_deref(), "Sort input")?,
                    depth + 1,
                )
                .await?;
                let orders = sort
                    .order
                    .iter()
                    .map(|order| sort_expr(&input, order))
                    .collect::<Result<Vec<_>, _>>()?;
                input.sort(orders).map_err(df_error)
            }
            RelType::Aggregate(aggregate) => {
                if aggregate.group_type != proto::aggregate::GroupType::Groupby as i32
                    || aggregate.pivot.is_some()
                    || !aggregate.grouping_sets.is_empty()
                {
                    return Err(unsupported("aggregate grouping mode"));
                }
                let input = plan_at(
                    ctx,
                    required(aggregate.input.as_deref(), "Aggregate input")?,
                    depth + 1,
                )
                .await?;
                let groups = aggregate
                    .grouping_expressions
                    .iter()
                    .map(|value| expr(&input, value, 0))
                    .collect::<Result<Vec<_>, _>>()?;
                let aggregates = aggregate
                    .aggregate_expressions
                    .iter()
                    .map(|value| expr(&input, value, 0))
                    .collect::<Result<Vec<_>, _>>()?;
                input.aggregate(groups, aggregates).map_err(df_error)
            }
            RelType::LocalRelation(local) => local_plan(ctx, local),
            RelType::Read(read) => {
                if read.is_streaming {
                    return Err(unsupported("streaming read"));
                }
                match required(read.read_type.as_ref(), "read type")? {
                    proto::read::ReadType::NamedTable(table) if table.options.is_empty() => ctx
                        .table(table.unparsed_identifier.as_str())
                        .await
                        .map_err(df_error),
                    _ => Err(unsupported("read source/options")),
                }
            }
            RelType::SubqueryAlias(alias) => {
                if !alias.qualifier.is_empty() {
                    return Err(unsupported("subquery alias qualifier"));
                }
                plan_at(
                    ctx,
                    required(alias.input.as_deref(), "SubqueryAlias input")?,
                    depth + 1,
                )
                .await?
                .alias(&alias.alias)
                .map_err(df_error)
            }
            RelType::Sample(_) => Err(unsupported("sample relation")),
            _ => Err(unsupported("relation node")),
        }
    })
}

fn range_plan(ctx: &SessionContext, range: &proto::Range) -> Result<DataFrame, Status> {
    if range.step == 0 {
        return Err(invalid("range step cannot be zero"));
    }
    if range
        .num_partitions
        .is_some_and(|partitions| partitions != 1)
    {
        return Err(unsupported("range partition count other than one"));
    }
    let start = range.start.unwrap_or(0);
    let distance = if range.step > 0 {
        i128::from(range.end) - i128::from(start)
    } else {
        i128::from(start) - i128::from(range.end)
    };
    if distance <= 0 {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        return memory_frame(ctx, schema.clone(), vec![RecordBatch::new_empty(schema)]);
    }
    // DataFusion 53 advances its i64 generator after yielding the final value.
    // Reject its overflow edge explicitly instead of returning a wrapped sequence.
    let step = i128::from(range.step);
    let count = (distance - 1) / step.abs() + 1;
    let next = i128::from(start) + count * step;
    if i64::try_from(next).is_err() {
        return Err(unsupported("range final increment exceeds i64"));
    }
    let function = ctx.table_function("range").map_err(df_error)?;
    let provider = function
        .create_table_provider(&[lit(start), lit(range.end), lit(range.step)])
        .map_err(df_error)?;
    ctx.read_table(provider)
        .map_err(df_error)?
        .select(vec![col("value").alias("id")])
        .map_err(df_error)
}

fn local_plan(ctx: &SessionContext, local: &proto::LocalRelation) -> Result<DataFrame, Status> {
    let declared = local.schema.as_deref().map(local_schema).transpose()?;
    let Some(data) = &local.data else {
        let schema =
            Arc::new(declared.ok_or_else(|| invalid("LocalRelation requires data or schema"))?);
        return memory_frame(ctx, schema.clone(), vec![RecordBatch::new_empty(schema)]);
    };
    let reader = StreamReader::try_new(Cursor::new(data.as_ref()), None)
        .map_err(|error| invalid(format!("invalid Arrow IPC: {error}")))?;
    let ipc_schema = reader.schema();
    let mut batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("invalid Arrow IPC: {error}")))?;
    let schema = if let Some(declared) = declared {
        if declared.fields().len() != ipc_schema.fields().len()
            || declared
                .fields()
                .iter()
                .zip(ipc_schema.fields())
                .any(|(left, right)| {
                    left.data_type() != right.data_type()
                        || left.is_nullable() != right.is_nullable()
                })
        {
            return Err(unsupported(
                "LocalRelation schema conversion or nullability override",
            ));
        }
        let schema = Arc::new(declared);
        batches = batches
            .into_iter()
            .map(|batch| RecordBatch::try_new(schema.clone(), batch.columns().to_vec()))
            .collect::<Result<_, _>>()
            .map_err(|error| invalid(error.to_string()))?;
        schema
    } else {
        ipc_schema
    };
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema.clone()));
    }
    memory_frame(ctx, schema, batches)
}

fn memory_frame(
    ctx: &SessionContext,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<DataFrame, Status> {
    let table = MemTable::try_new(schema, vec![batches]).map_err(df_error)?;
    ctx.read_table(Arc::new(table)).map_err(df_error)
}

fn expr(input: &DataFrame, expression: &proto::Expression, depth: usize) -> Result<Expr, Status> {
    if depth > MAX_DEPTH {
        return Err(invalid("expression nesting exceeds 64"));
    }
    use proto::expression::ExprType;
    match required(expression.expr_type.as_ref(), "expression type")? {
        ExprType::UnresolvedAttribute(attribute) => {
            if attribute.plan_id.is_some() || attribute.is_metadata_column == Some(true) {
                return Err(unsupported("plan-bound or metadata column"));
            }
            let mut column =
                Column::from_qualified_name_ignore_case(&attribute.unparsed_identifier);
            if column.relation.is_none()
                && !input
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == &column.name)
            {
                let requested = column.name.to_lowercase();
                let mut matching = input
                    .schema()
                    .fields()
                    .iter()
                    .filter(|field| field.name().to_lowercase() == requested);
                if let Some(field) = matching.next() {
                    if matching.next().is_some() {
                        return Err(invalid(format!("ambiguous column {}", column.name)));
                    }
                    column.name = field.name().clone();
                }
            }
            Ok(Expr::Column(column))
        }
        ExprType::Literal(value) => literal(value),
        ExprType::ExpressionString(value) => {
            input.parse_sql_expr(&value.expression).map_err(df_error)
        }
        ExprType::Alias(alias) => {
            if alias.name.len() != 1 || alias.metadata.is_some() {
                return Err(unsupported("multi-part alias or alias metadata"));
            }
            Ok(expr(
                input,
                required(alias.expr.as_deref(), "alias expression")?,
                depth + 1,
            )?
            .alias(&alias.name[0]))
        }
        ExprType::UnresolvedFunction(function) => function_expr(input, function, depth + 1),
        ExprType::Cast(value) => {
            if !matches!(value.eval_mode, 0 | 2) {
                return Err(unsupported("legacy or try cast mode"));
            }
            let target = match required(value.cast_to_type.as_ref(), "cast target")? {
                proto::expression::cast::CastToType::Type(target) => arrow_type(target)?,
                _ => return Err(unsupported("cast type string")),
            };
            Ok(cast(
                expr(
                    input,
                    required(value.expr.as_deref(), "cast expression")?,
                    depth + 1,
                )?,
                target,
            ))
        }
        _ => Err(unsupported("expression node")),
    }
}

fn function_expr(
    input: &DataFrame,
    function: &proto::expression::UnresolvedFunction,
    depth: usize,
) -> Result<Expr, Status> {
    if depth > MAX_DEPTH {
        return Err(invalid("expression nesting exceeds 64"));
    }
    if function.is_distinct
        || function.is_user_defined_function
        || function.is_internal == Some(true)
    {
        return Err(unsupported("distinct, user-defined or internal function"));
    }
    let name = function.function_name.to_ascii_lowercase();
    if name == "count" && function.arguments.len() == 1 {
        if let Some(proto::expression::ExprType::UnresolvedStar(star)) =
            &function.arguments[0].expr_type
        {
            if star.unparsed_target.is_some() || star.plan_id.is_some() {
                return Err(unsupported("qualified or plan-bound count star"));
            }
            return Ok(count(lit(1_i32)));
        }
        if let Some(proto::expression::ExprType::UnresolvedAttribute(attribute)) =
            &function.arguments[0].expr_type
        {
            if attribute.unparsed_identifier == "*" {
                if attribute.plan_id.is_some() || attribute.is_metadata_column == Some(true) {
                    return Err(unsupported("plan-bound or metadata count star"));
                }
                return Ok(count(lit(1_i32)));
            }
            if attribute.unparsed_identifier.ends_with(".*") {
                return Err(unsupported("qualified count star"));
            }
        }
    }
    let args = function
        .arguments
        .iter()
        .map(|arg| expr(input, arg, depth))
        .collect::<Result<Vec<_>, _>>()?;
    match args.as_slice() {
        [value] => Ok(match name.as_str() {
            "count" => count(value.clone()),
            "sum" => sum(value.clone()),
            "avg" | "mean" => avg(value.clone()),
            "min" => min(value.clone()),
            "max" => max(value.clone()),
            "not" | "!" => !value.clone(),
            "isnull" => value.clone().is_null(),
            "isnotnull" => value.clone().is_not_null(),
            "negative" => -value.clone(),
            _ => return Err(unsupported(format!("function {}", function.function_name))),
        }),
        [left, right] => Ok(match name.as_str() {
            "+" => left.clone() + right.clone(),
            "-" => left.clone() - right.clone(),
            "*" => left.clone() * right.clone(),
            "/" => {
                let types = [
                    left.get_type(input.schema()).map_err(df_error)?,
                    right.get_type(input.schema()).map_err(df_error)?,
                ];
                if types.iter().any(|data_type| {
                    matches!(
                        data_type,
                        DataType::Decimal32(_, _)
                            | DataType::Decimal64(_, _)
                            | DataType::Decimal128(_, _)
                            | DataType::Decimal256(_, _)
                    )
                }) {
                    return Err(unsupported("decimal division"));
                }
                cast(left.clone(), DataType::Float64) / cast(right.clone(), DataType::Float64)
            }
            "%" => left.clone() % right.clone(),
            "=" | "==" => left.clone().eq(right.clone()),
            "!=" | "<>" => left.clone().not_eq(right.clone()),
            ">" => left.clone().gt(right.clone()),
            ">=" => left.clone().gt_eq(right.clone()),
            "<" => left.clone().lt(right.clone()),
            "<=" => left.clone().lt_eq(right.clone()),
            "and" => left.clone().and(right.clone()),
            "or" => left.clone().or(right.clone()),
            _ => return Err(unsupported(format!("function {}", function.function_name))),
        }),
        _ => Err(unsupported(format!(
            "function {} arity {}",
            function.function_name,
            args.len()
        ))),
    }
}

fn literal(value: &proto::expression::Literal) -> Result<Expr, Status> {
    use proto::expression::literal::LiteralType;
    let scalar = match required(value.literal_type.as_ref(), "literal value")? {
        LiteralType::Null(data_type) => {
            let data_type = value.data_type.as_ref().unwrap_or(data_type);
            ScalarValue::try_from(&arrow_type(data_type)?).map_err(df_error)?
        }
        LiteralType::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        LiteralType::Byte(value) => ScalarValue::Int8(Some(
            i8::try_from(*value).map_err(|_| invalid("byte literal overflow"))?,
        )),
        LiteralType::Short(value) => ScalarValue::Int16(Some(
            i16::try_from(*value).map_err(|_| invalid("short literal overflow"))?,
        )),
        LiteralType::Integer(value) => ScalarValue::Int32(Some(*value)),
        LiteralType::Long(value) => ScalarValue::Int64(Some(*value)),
        LiteralType::Float(value) => ScalarValue::Float32(Some(*value)),
        LiteralType::Double(value) => ScalarValue::Float64(Some(*value)),
        LiteralType::String(value) => ScalarValue::Utf8(Some(value.clone())),
        LiteralType::Binary(value) => ScalarValue::Binary(Some(value.to_vec())),
        LiteralType::Date(value) => ScalarValue::Date32(Some(*value)),
        LiteralType::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into()))
        }
        LiteralType::TimestampNtz(value) => ScalarValue::TimestampMicrosecond(Some(*value), None),
        _ => return Err(unsupported("literal type")),
    };
    Ok(lit(scalar))
}

fn sort_expr(input: &DataFrame, order: &proto::expression::SortOrder) -> Result<SortExpr, Status> {
    let ascending = match order.direction {
        1 => true,
        2 => false,
        _ => return Err(invalid("sort direction is unspecified")),
    };
    let nulls_first = match order.null_ordering {
        1 => true,
        2 => false,
        _ => return Err(invalid("null ordering is unspecified")),
    };
    Ok(expr(
        input,
        required(order.child.as_deref(), "sort expression")?,
        0,
    )?
    .sort(ascending, nulls_first))
}

fn required<'a, T>(value: Option<&'a T>, name: &str) -> Result<&'a T, Status> {
    value.ok_or_else(|| invalid(format!("missing {name}")))
}

pub(crate) fn df_error(error: ::datafusion::error::DataFusionError) -> Status {
    use ::datafusion::error::DataFusionError;
    use arrow::error::ArrowError;
    use tonic::Code;

    fn code(error: &DataFusionError) -> Code {
        match error {
            DataFusionError::NotImplemented(_) => Code::Unimplemented,
            DataFusionError::ResourcesExhausted(_) => Code::ResourceExhausted,
            DataFusionError::ArrowError(cause, _) => match cause.as_ref() {
                ArrowError::CastError(_)
                | ArrowError::ParseError(_)
                | ArrowError::ComputeError(_)
                | ArrowError::ArithmeticOverflow(_)
                | ArrowError::DivideByZero => Code::InvalidArgument,
                ArrowError::ExternalError(cause) => cause
                    .downcast_ref::<DataFusionError>()
                    .map(code)
                    .unwrap_or(Code::Internal),
                _ => Code::Internal,
            },
            DataFusionError::Execution(_) => Code::InvalidArgument,
            DataFusionError::Internal(_)
            | DataFusionError::External(_)
            | DataFusionError::ExecutionJoin(_) => Code::Internal,
            DataFusionError::Context(_, cause) | DataFusionError::Diagnostic(_, cause) => {
                code(cause)
            }
            DataFusionError::Shared(cause) => code(cause),
            DataFusionError::Collection(causes) => {
                causes.first().map(code).unwrap_or(Code::Internal)
            }
            _ => Code::InvalidArgument,
        }
    }

    Status::new(code(&error), error.to_string())
}

fn invalid(message: impl Into<String>) -> Status {
    Status::invalid_argument(message.into())
}

fn unsupported(message: impl Into<String>) -> Status {
    Status::unimplemented(format!("unsupported {}", message.into()))
}

#[cfg(test)]
mod tests {
    use ::datafusion::error::DataFusionError;
    use arrow::error::ArrowError;
    use tonic::Code;

    use super::df_error;

    #[test]
    fn arrow_wrapped_execution_error_retains_its_status_and_full_message() {
        let error = DataFusionError::ArrowError(
            Box::new(ArrowError::ExternalError(Box::new(
                DataFusionError::Execution("invalid value in a DataFusion worker".into()),
            ))),
            None,
        );
        let message = error.to_string();
        let status = df_error(error);
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), message);
    }

    #[test]
    fn arrow_wrapped_non_datafusion_error_remains_internal() {
        let error = DataFusionError::ArrowError(
            Box::new(ArrowError::ExternalError(Box::new(std::io::Error::other(
                "external worker failure",
            )))),
            None,
        );
        let message = error.to_string();
        let status = df_error(error);
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);
    }
}
