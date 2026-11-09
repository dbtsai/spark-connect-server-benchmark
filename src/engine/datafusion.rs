//! DataFusion execution adapter for the supported Spark Connect subset.

mod planner;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ::datafusion::dataframe::DataFrame;
use ::datafusion::prelude::{SessionConfig, SessionContext as DataFusionContext};
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use spark_connect_proto as proto;
use tonic::Status;

use self::planner::df_error;
use super::{
    BatchStream, ConfigResult, Engine, EngineSession, PreparedOperation, PreparedQuery,
    SessionContext,
};
use crate::schema;

/// Creates independent DataFusion sessions for Spark Connect clients.
#[derive(Default)]
pub struct DataFusionEngine;

impl Engine for DataFusionEngine {
    fn create_session(&self, _context: SessionContext) -> Result<Arc<dyn EngineSession>, Status> {
        Ok(Arc::new(DataFusionSession::new()))
    }
}

/// A session-local DataFusion catalog and its supported Spark configuration.
pub struct DataFusionSession {
    ctx: DataFusionContext,
    configs: Mutex<HashMap<String, String>>,
    catalog: tokio::sync::Mutex<()>,
}

impl DataFusionSession {
    fn new() -> Self {
        let defaults = [
            ("spark.sql.session.timeZone", "UTC"),
            ("spark.sql.timestampType", "TIMESTAMP_LTZ"),
            ("spark.sql.execution.arrow.useLargeVarTypes", "false"),
            (
                "spark.sql.execution.pandas.convertToArrowArraySafely",
                "false",
            ),
            ("spark.sql.execution.pandas.inferPandasDictAsMap", "false"),
            ("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false"),
            (
                "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled",
                "false",
            ),
            (
                "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled",
                "false",
            ),
            ("spark.sql.session.localRelationCacheThreshold", "67108864"),
            ("spark.sql.session.localRelationSizeLimit", "67108864"),
            ("spark.sql.session.localRelationChunkSizeRows", "10000"),
            ("spark.sql.session.localRelationChunkSizeBytes", "4194304"),
            (
                "spark.sql.session.localRelationBatchOfChunksSizeBytes",
                "4194304",
            ),
        ];
        Self {
            ctx: DataFusionContext::new_with_config(
                SessionConfig::new()
                    .with_target_partitions(2)
                    .with_batch_size(8192),
            ),
            configs: Mutex::new(
                defaults
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
            catalog: tokio::sync::Mutex::new(()),
        }
    }
}

struct DataFusionQuery {
    frame: DataFrame,
    schema: SchemaRef,
}

#[tonic::async_trait]
impl PreparedQuery for DataFusionQuery {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn execute(self: Box<Self>) -> Result<BatchStream, Status> {
        // Keep execution owned by the returned stream so the frontend can cancel
        // by dropping this future or stream without leaving detached work here.
        let batches = self.frame.execute_stream().await.map_err(df_error)?;
        Ok(Box::pin(batches.map(|batch| batch.map_err(df_error))))
    }
}

#[tonic::async_trait]
impl EngineSession for DataFusionSession {
    async fn prepare(&self, op: proto::plan::OpType) -> Result<PreparedOperation, Status> {
        let _catalog = self.catalog.lock().await;
        let relation = match op {
            proto::plan::OpType::Root(relation) => relation,
            proto::plan::OpType::Command(command) => match command.command_type {
                Some(proto::command::CommandType::SqlCommand(sql)) => {
                    #[allow(deprecated)]
                    if !sql.args.is_empty()
                        || !sql.pos_args.is_empty()
                        || !sql.named_arguments.is_empty()
                        || !sql.pos_arguments.is_empty()
                    {
                        return Err(Status::unimplemented("SQL command parameters"));
                    }
                    #[allow(deprecated)]
                    let relation = match sql.input {
                        Some(relation) => relation,
                        None if !sql.sql.is_empty() => proto::Relation {
                            rel_type: Some(proto::relation::RelType::Sql(proto::Sql {
                                query: sql.sql,
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        None => return Err(Status::invalid_argument("missing SQL command input")),
                    };
                    planner::plan(&self.ctx, &relation).await?;
                    return Ok(PreparedOperation::Command(vec![
                        proto::execute_plan_response::ResponseType::SqlCommandResult(
                            proto::execute_plan_response::SqlCommandResult {
                                relation: Some(relation),
                            },
                        ),
                    ]));
                }
                Some(proto::command::CommandType::CreateDataframeView(view)) => {
                    if view.is_global {
                        return Err(Status::unimplemented("global temporary views"));
                    }
                    let relation = view
                        .input
                        .ok_or_else(|| Status::invalid_argument("missing view input"))?;
                    let frame = planner::plan(&self.ctx, &relation).await?;
                    if !view.replace
                        && self.ctx.table_exist(view.name.as_str()).map_err(df_error)?
                    {
                        return Err(Status::already_exists("view already exists"));
                    }
                    let previous = if view.replace {
                        self.ctx
                            .deregister_table(view.name.as_str())
                            .map_err(df_error)?
                    } else {
                        None
                    };
                    if let Err(error) = self
                        .ctx
                        .register_table(view.name.as_str(), frame.into_view())
                    {
                        if let Some(previous) = previous {
                            self.ctx
                                .register_table(view.name.as_str(), previous)
                                .map_err(df_error)?;
                        }
                        return Err(df_error(error));
                    }
                    return Ok(PreparedOperation::Command(Vec::new()));
                }
                _ => return Err(Status::unimplemented("unsupported Spark Connect command")),
            },
            proto::plan::OpType::CompressedOperation(_) => {
                return Err(Status::unimplemented("compressed Spark Connect plans"));
            }
        };
        let frame = planner::plan(&self.ctx, &relation).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        Ok(PreparedOperation::Query(Box::new(DataFusionQuery {
            frame,
            schema,
        })))
    }

    async fn analyze(
        &self,
        input: proto::analyze_plan_request::Analyze,
    ) -> Result<proto::analyze_plan_response::Result, Status> {
        use proto::{analyze_plan_request::Analyze, analyze_plan_response as out};
        let _catalog = self.catalog.lock().await;
        let result = match input {
            Analyze::Schema(input) => {
                let frame = planner::plan(&self.ctx, &root(input.plan)?).await?;
                out::Result::Schema(out::Schema {
                    schema: Some(schema::spark_schema(frame.schema().as_arrow())?),
                })
            }
            Analyze::IsLocal(input) => {
                let relation = root(input.plan)?;
                planner::plan(&self.ctx, &relation).await?;
                // Spark Dataset.isLocal checks the root LocalRelation/CommandResult node.
                let is_local = matches!(
                    relation.rel_type,
                    Some(proto::relation::RelType::LocalRelation(_))
                );
                out::Result::IsLocal(out::IsLocal { is_local })
            }
            Analyze::IsStreaming(input) => {
                planner::plan(&self.ctx, &root(input.plan)?).await?;
                out::Result::IsStreaming(out::IsStreaming {
                    is_streaming: false,
                })
            }
            _ => return Err(Status::unimplemented("unsupported Spark Connect analysis")),
        };
        Ok(result)
    }

    async fn config(
        &self,
        input: proto::config_request::operation::OpType,
    ) -> Result<ConfigResult, Status> {
        use proto::config_request::operation::OpType;
        let mut configs = self
            .configs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pair = |key: String, value: Option<String>| proto::KeyValue { key, value };
        let mut warnings = Vec::new();
        let pairs = match input {
            OpType::Get(input) => input
                .keys
                .into_iter()
                .map(|key| {
                    let value = configs
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| Status::not_found(format!("unknown config: {key}")))?;
                    Ok(pair(key, Some(value)))
                })
                .collect::<Result<Vec<_>, Status>>()?,
            OpType::GetOption(input) => input
                .keys
                .into_iter()
                .map(|key| {
                    let value = configs.get(&key).cloned();
                    pair(key, value)
                })
                .collect(),
            OpType::GetWithDefault(input) => input
                .pairs
                .into_iter()
                .map(|pair_input| {
                    let value = configs.get(&pair_input.key).cloned().or(pair_input.value);
                    pair(pair_input.key, value)
                })
                .collect(),
            OpType::GetAll(input) => configs
                .iter()
                .filter(|(key, _)| {
                    input
                        .prefix
                        .as_ref()
                        .is_none_or(|prefix| key.starts_with(prefix))
                })
                .map(|(key, value)| pair(key.clone(), Some(value.clone())))
                .collect(),
            OpType::IsModifiable(input) => input
                .keys
                .into_iter()
                .map(|key| {
                    let value = (!key.starts_with("spark.")).to_string();
                    pair(key, Some(value))
                })
                .collect(),
            OpType::Set(input) => {
                let mut accepted = Vec::new();
                for pair in &input.pairs {
                    let error = if pair.value.is_none() {
                        Some(Status::invalid_argument("config value is required"))
                    } else if pair.key.starts_with("spark.")
                        && configs.get(&pair.key) != pair.value.as_ref()
                    {
                        Some(Status::unimplemented(format!(
                            "Spark configuration is fixed or unsupported: {}",
                            pair.key
                        )))
                    } else {
                        None
                    };
                    if let Some(error) = error {
                        if input.silent == Some(true) {
                            warnings.push(error.message().to_owned());
                        } else {
                            return Err(error);
                        }
                    } else {
                        accepted.push(pair);
                    }
                }
                for pair in accepted {
                    configs.insert(pair.key.clone(), pair.value.clone().unwrap());
                }
                Vec::new()
            }
            OpType::Unset(input) => {
                if input.keys.iter().any(|key| key.starts_with("spark.")) {
                    return Err(Status::unimplemented("Spark configuration is fixed"));
                }
                for key in input.keys {
                    configs.remove(&key);
                }
                Vec::new()
            }
        };
        Ok(ConfigResult { pairs, warnings })
    }
}

fn root(plan: Option<proto::Plan>) -> Result<proto::Relation, Status> {
    match plan.and_then(|plan| plan.op_type) {
        Some(proto::plan::OpType::Root(relation)) => Ok(relation),
        _ => Err(Status::invalid_argument("expected a root relation plan")),
    }
}
