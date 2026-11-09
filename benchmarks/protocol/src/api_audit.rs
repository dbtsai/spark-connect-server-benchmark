//! Inspect serialization contracts through public APIs in the pinned Rust client fork.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::builder::{Int64Builder, MapBuilder, StringBuilder};
use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use prost::Message;
use serde_json::{json, Value};
use spark_connect::expression::LiteralExpression;
use spark_connect::{DataType as SparkType, SparkSession};
use spark_connect_proto as proto;

pub const CLIENT_REVISION: &str = "87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce";
pub const OFFICIAL_BASE_REVISION: &str = "925dc876f63f1f779fe176f62575a0aa96cc6886";

/// An audit uses actual pinned-client output. No Spark process or Python is involved.
/// Protocol coverage through generated gRPC bindings is identified separately
/// from the public logical-plan builders and is not a high-level API parity claim.
/// The literal fixture's projection/schema, column count, aliases, literal shape,
/// and declared field/array types are setup prerequisites. A failed prerequisite
/// returns Err and stops corpus export, even if an upstream API change caused it.
/// Observed type mismatches and public Arrow import regressions become client gaps.
pub fn audit() -> Result<Value, String> {
    let raw = crate::corpus::raw_typed_literals();
    let Some(proto::relation::RelType::Project(project)) = raw.relation.rel_type.as_ref() else {
        return Err("pinned-client typed literal plan is not a projection".into());
    };
    let Some(proto::data_type::Kind::Struct(schema)) = raw.expected_schema.kind.as_ref() else {
        return Err("typed literal fixture schema is not a struct".into());
    };
    if project.expressions.len() != schema.fields.len() {
        return Err(
            "pinned-client typed literal column count differs from its declared schema".into(),
        );
    }
    let mut observations = vec![];
    let mut gaps = vec![];
    for (expression, field) in project.expressions.iter().zip(&schema.fields) {
        let Some(proto::expression::ExprType::Alias(alias)) = expression.expr_type.as_ref() else {
            return Err("pinned-client typed literal output has no named alias".into());
        };
        if alias.name.len() != 1 || alias.name[0] != field.name {
            return Err(
                "pinned-client typed literal alias differs from its declared schema".into(),
            );
        }
        let Some(proto::expression::ExprType::Literal(literal)) = alias
            .expr
            .as_deref()
            .and_then(|value| value.expr_type.as_ref())
        else {
            return Err("pinned-client typed literal output is not a literal".into());
        };
        let expected = field
            .data_type
            .as_ref()
            .ok_or("typed literal field has no declared type")?;
        inspect_literal(&field.name, literal, expected, &mut observations, &mut gaps)?;
    }

    // These probes are audit-only: they do not add cases to the timed corpus.
    let mut array_observations = vec![];
    for (name, value, expected) in array_probes() {
        let Some(proto::expression::ExprType::Literal(literal)) = value.to_proto().expr_type else {
            return Err("pinned-client array expression did not produce a literal".into());
        };
        inspect_literal(
            name,
            &literal,
            &expected.to_proto(),
            &mut array_observations,
            &mut gaps,
        )?;
    }
    let literal_contract_passed = gaps.is_empty();
    let (map, map_gaps) = audit_public_arrow_import()?;
    let map_contract_passed = map_gaps.is_empty();
    gaps.extend(map_gaps);
    Ok(json!({
        "schema_version":2,
        "client_revision":CLIENT_REVISION,
        "client_provenance":{
            "official_repository":"https://github.com/apache/spark-connect-rust",
            "official_base_revision":OFFICIAL_BASE_REVISION,
            "fork_repository":"https://github.com/dbtsai/spark-connect-rust",
            "fork_revision":CLIENT_REVISION,
            "map_patch_source":format!("https://github.com/dbtsai/spark-connect-rust/compare/{OFFICIAL_BASE_REVISION}...{CLIENT_REVISION}"),
            "map_patch_paths":[
                "crates/spark-connect/src/arrow_input.rs",
                "crates/spark-connect/src/session.rs",
                "crates/spark-connect/tests/arrow_map_input.rs",
            ],
            "map_patch_upstream_pr":"https://github.com/apache/spark-connect-rust/pull/396",
            "map_patch_upstream_issue":"SPARK-61212",
            "map_patch_upstream_status":"upstream_pr_open_not_merged",
        },
        "status":if gaps.is_empty(){"no_observed_client_gaps"}else{"client_gaps_observed"},
        "has_client_gaps":!gaps.is_empty(),
        "client_gaps":gaps,
        "resolved_contracts":[
            {"id":"array_literal_declared_element_type","upstream_fix":"SPARK-61119",
                "upstream_fix_commit":"70688ec07ff87ea5cd4d685247074feeaa2c2b7f",
                "source":"Array literal fix included in the official base revision.",
                "passed":literal_contract_passed,
                "behavior":"Populated, empty, and nested arrays preserve declared types through Literal.data_type or Array.element_type; every present annotation must match."},
            {"id":"arrow_map_canonical_child_names",
                "source":"Arrow map normalization proposed upstream in PR #396 (SPARK-61212); not merged.",
                "source_revision":CLIENT_REVISION,
                "passed":map_contract_passed,
                "behavior":"The fork's public Arrow import preserves the zero-row map contract and serializes canonical entries/key/value names."},
        ],
        "unmodified_literal_observations":observations,
        "array_literal_probes":array_observations,
        "unmodified_relation_bytes":raw.relation.encoded_len(),
        "coverage_layers":{
            "logical_plans":"Public logical-plan/expression builders from the pinned fork, with unmodified literal output.",
            "rpc_routes":{"source":"Upstream-derived generated protobuf/gRPC bindings in the pinned fork","routes":crate::probes::ROUTES,
                "high_level_session_api_parity":"not established by generated-binding probes"},
            "interoperability":[map],
        },
        "scope":"Observed serialization contracts at the pinned revision; not an exhaustive inventory of missing PySpark APIs or a Spark execution test.",
    }))
}

fn array_probes() -> Vec<(&'static str, LiteralExpression, SparkType)> {
    let array_type = |element_type| SparkType::Array {
        element_type: Box::new(element_type),
        contains_null: true,
    };
    vec![
        (
            "populated_array",
            LiteralExpression::Array {
                element_type: Box::new(SparkType::Integer),
                elements: vec![
                    LiteralExpression::Integer(1),
                    LiteralExpression::Null(SparkType::Integer),
                    LiteralExpression::Integer(3),
                ],
            },
            array_type(SparkType::Integer),
        ),
        (
            "empty_array",
            LiteralExpression::Array {
                element_type: Box::new(SparkType::Long),
                elements: vec![],
            },
            array_type(SparkType::Long),
        ),
        (
            "nested_array",
            LiteralExpression::Array {
                element_type: Box::new(array_type(SparkType::Integer)),
                elements: vec![
                    LiteralExpression::Array {
                        element_type: Box::new(SparkType::Integer),
                        elements: vec![
                            LiteralExpression::Integer(7),
                            LiteralExpression::Null(SparkType::Integer),
                        ],
                    },
                    LiteralExpression::Array {
                        element_type: Box::new(SparkType::Integer),
                        elements: vec![],
                    },
                ],
            },
            array_type(array_type(SparkType::Integer)),
        ),
    ]
}

/// Match only variants in typed_literals and the array probes. Extending the
/// corpus with Map, Struct, interval, char/varchar, or time literals requires
/// extending this helper; it is not a protocol-wide literal validator.
fn literal_kind_matches(literal: &proto::expression::Literal, expected: &proto::DataType) -> bool {
    use proto::data_type::Kind as K;
    use proto::expression::literal::LiteralType as L;
    match (literal.literal_type.as_ref(), expected.kind.as_ref()) {
        (Some(L::Null(actual)), _) => actual == expected,
        (Some(L::Decimal(actual)), Some(K::Decimal(expected))) => {
            actual.precision == expected.precision && actual.scale == expected.scale
        }
        (actual, expected) => matches!(
            (actual, expected),
            (Some(L::Boolean(_)), Some(K::Boolean(_)))
                | (Some(L::Byte(_)), Some(K::Byte(_)))
                | (Some(L::Short(_)), Some(K::Short(_)))
                | (Some(L::Integer(_)), Some(K::Integer(_)))
                | (Some(L::Long(_)), Some(K::Long(_)))
                | (Some(L::Float(_)), Some(K::Float(_)))
                | (Some(L::Double(_)), Some(K::Double(_)))
                | (Some(L::String(_)), Some(K::String(_)))
                | (Some(L::Binary(_)), Some(K::Binary(_)))
                | (Some(L::Date(_)), Some(K::Date(_)))
                | (Some(L::Timestamp(_)), Some(K::Timestamp(_)))
                | (Some(L::TimestampNtz(_)), Some(K::TimestampNtz(_)))
                | (Some(L::Array(_)), Some(K::Array(_)))
        ),
    }
}

fn inspect_literal(
    path: &str,
    literal: &proto::expression::Literal,
    expected: &proto::DataType,
    observations: &mut Vec<Value>,
    gaps: &mut Vec<Value>,
) -> Result<(), String> {
    inspect_literal_with_context(path, literal, expected, false, observations, gaps)
}

fn inspect_literal_with_context(
    path: &str,
    literal: &proto::expression::Literal,
    expected: &proto::DataType,
    type_supplied_by_parent: bool,
    observations: &mut Vec<Value>,
    gaps: &mut Vec<Value>,
) -> Result<(), String> {
    let root_matches = literal.data_type.as_ref().map(|actual| actual == expected);
    let mut matches = root_matches != Some(false) && literal_kind_matches(literal, expected);
    let mut observation = json!({
        "path":path,
        "expected_type":format!("{expected:?}"),
        "root_data_type_present":literal.data_type.is_some(),
        "root_data_type_matches_expected":root_matches,
    });
    if let Some(proto::expression::literal::LiteralType::Array(array)) =
        literal.literal_type.as_ref()
    {
        if let Some(proto::data_type::Kind::Array(expected_array)) = expected.kind.as_ref() {
            let expected_element = expected_array
                .element_type
                .as_deref()
                .ok_or("declared array schema has no element type")?;
            #[allow(deprecated)]
            let element_matches = array
                .element_type
                .as_ref()
                .map(|actual| actual == expected_element);
            #[allow(deprecated)]
            {
                observation["array_element_type"] =
                    json!(array.element_type.as_ref().map(|ty| format!("{ty:?}")));
            }
            observation["array_element_type_matches_expected"] = json!(element_matches);
            observation["element_count"] = json!(array.elements.len());
            // Spark 4.2 assembly source: LiteralValueProtoConverter.scala at
            // 250ccca3f2b5f043e20b415adf17bda5d202c60f. Root type selection is
            // at lines 489-497; deprecated Array.element_type fallback is at
            // 552-562. getScalaConverter dispatches arrays at 417-418, and
            // toScalaArrayInternal supplies the enclosing element schema:
            // https://github.com/apache/spark/blob/250ccca3f2b5f043e20b415adf17bda5d202c60f/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala#L599-L616
            // Thus a correct root schema also types unannotated nested arrays.
            // The audit still rejects each present incorrect annotation.
            let type_available = root_matches == Some(true)
                || element_matches == Some(true)
                || type_supplied_by_parent;
            observation["array_type_available"] = json!(type_available);
            observation["type_supplied_by_parent"] = json!(type_supplied_by_parent);
            matches &= element_matches != Some(false) && type_available;
            for (index, child) in array.elements.iter().enumerate() {
                inspect_literal_with_context(
                    &format!("{path}[{index}]"),
                    child,
                    expected_element,
                    type_available,
                    observations,
                    gaps,
                )?;
            }
        }
    }
    observation["type_contract_matches"] = json!(matches);
    if !matches {
        gaps.push(json!({
            "id":"literal_type_contract_regression",
            "layer":"pinned_fork_public_expression_api",
            "evidence":observation,
            "impact":"The serialized literal type differs from the declared Rust type.",
        }));
    }
    observations.push(observation);
    Ok(())
}

fn map_observation(data_type: &DataType) -> Result<Value, String> {
    let DataType::Map(entries, sorted) = data_type else {
        return Err("Arrow import did not produce a map".into());
    };
    let DataType::Struct(children) = entries.data_type() else {
        return Err("Arrow map has no entry struct".into());
    };
    if children.len() != 2 {
        return Err("Arrow map does not have exactly two child fields".into());
    }
    Ok(json!({
        "entry_name":entries.name(),
        "child_names":[children[0].name(),children[1].name()],
        "key_type":format!("{:?}",children[0].data_type()),
        "value_type":format!("{:?}",children[1].data_type()),
        "entry_nullable":entries.is_nullable(),
        "key_nullable":children[0].is_nullable(),
        "value_nullable":children[1].is_nullable(),
        "keys_sorted":sorted,
    }))
}

fn top_level_schema_observation(schema: &Schema) -> Value {
    json!({
        "fields":schema.fields().iter().map(|field| json!({
            "name":field.name(),
            "nullable":field.is_nullable(),
            "metadata":field.metadata(),
        })).collect::<Vec<_>>(),
        "metadata":schema.metadata(),
    })
}

/// Classify client observations without failing export when the contract changes.
/// The defaults come from the harness's valid, fixed MapBuilder input.
fn classify_map(defaults: &Value, observed: &Value) -> Vec<Value> {
    let expected_top_level = json!({
        "fields":[{"name":"mapping","nullable":true,"metadata":{}}],
        "metadata":{},
    });
    let mut expected_map = defaults.clone();
    expected_map["entry_name"] = json!("entries");
    expected_map["child_names"] = json!(["key", "value"]);
    let mut mismatches = vec![];
    if !observed["error"].is_null() {
        mismatches.push("public_arrow_import_error".to_owned());
    } else {
        if observed["serialized_top_level_schema"] != expected_top_level {
            mismatches.push("top_level_schema".to_owned());
        }
        if observed["serialized_rows"] != 0 {
            mismatches.push("serialized_rows".to_owned());
        }
        if !observed["declared_spark_schema"].is_null() {
            mismatches.push("declared_spark_schema".to_owned());
        }
        if let Some(expected_fields) = expected_map.as_object() {
            for (name, expected) in expected_fields {
                if &observed["client_serialized_map"][name] != expected {
                    mismatches.push(format!("map.{name}"));
                }
            }
        }
    }
    if mismatches.is_empty() {
        return vec![];
    }
    vec![json!({
        "id":"arrow_map_import_contract_regression",
        "layer":"pinned_fork_public_session_api",
        "api":"SparkSession::create_dataframe_from_arrow(zero_row_default_MapBuilder_batch, None)",
        "mismatches":mismatches,
        "expected":{"top_level_schema":expected_top_level,"map":expected_map,"serialized_rows":0,"declared_spark_schema":null},
        "evidence":observed,
        "impact":"The public client failed the declared zero-row Arrow map import contract.",
    })]
}

fn public_map_error(stage: &str, message: impl ToString) -> Value {
    json!({"error":{"stage":stage,"message":message.to_string()}})
}

/// Decode actual client output into deterministic evidence; malformed output is
/// an observed contract failure, rather than a harness setup error.
fn observe_map_relation(relation: proto::Relation) -> Value {
    let Some(proto::relation::RelType::LocalRelation(local)) = relation.rel_type else {
        return public_map_error(
            "relation_kind",
            "zero-row Arrow import did not produce a local relation",
        );
    };
    let Some(data) = local.data else {
        return public_map_error("ipc_data", "zero-row Arrow import has no IPC data");
    };
    let reader = match StreamReader::try_new(Cursor::new(&data), None) {
        Ok(reader) => reader,
        Err(error) => return public_map_error("ipc_header", error),
    };
    let schema = reader.schema();
    let mut observed = json!({
        "serialized_ipc_bytes":data.len(),
        "serialized_top_level_schema":top_level_schema_observation(schema.as_ref()),
        "declared_spark_schema":local.schema,
    });
    let Some(field) = schema.fields().first() else {
        observed["error"] = json!({"stage":"map_schema","message":"Arrow import has no map field"});
        return observed;
    };
    match map_observation(field.data_type()) {
        Ok(map) => observed["client_serialized_map"] = map,
        Err(error) => {
            observed["error"] = json!({"stage":"map_schema","message":error});
            return observed;
        }
    }
    let mut rows = 0usize;
    for batch in reader {
        match batch {
            Ok(batch) => {
                let Some(total) = rows.checked_add(batch.num_rows()) else {
                    observed["error"] =
                        json!({"stage":"ipc_batch","message":"IPC row count overflow"});
                    return observed;
                };
                rows = total;
            }
            Err(error) => {
                observed["error"] = json!({"stage":"ipc_batch","message":error.to_string()});
                return observed;
            }
        }
    }
    observed["serialized_rows"] = json!(rows);
    observed
}

fn observe_public_arrow_import(batch: RecordBatch) -> Value {
    // Client channels are lazy, and zero-row Arrow imports bypass config and
    // upload RPCs. A closed loopback endpoint keeps this audit execution-free.
    let session = match SparkSession::builder()
        .remote("sc://127.0.0.1:1")
        .get_or_create()
    {
        Ok(session) => session,
        Err(error) => return public_map_error("session_builder", error),
    };
    match session.create_dataframe_from_arrow(vec![batch], None) {
        Ok(frame) => observe_map_relation(frame.to_relation()),
        Err(error) => public_map_error("public_arrow_import", error),
    }
}

fn audit_public_arrow_import() -> Result<(Value, Vec<Value>), String> {
    // Arrow builder defaults are harness input evidence. Only constructing that
    // input and starting the isolation thread can fail as setup invariants.
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let default_map = builder.finish();
    let defaults = map_observation(default_map.data_type())?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "mapping",
        default_map.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(default_map)])
        .map_err(|error| format!("could not build audit input: {error}"))?;
    // The client's synchronous APIs call block_on, which cannot run inside a
    // current-thread Tokio runtime. Isolate the complete API call on an ordinary
    // thread so audit() works from both Tokio runtime flavors and sync callers.
    let worker = std::thread::Builder::new()
        .name("spark-connect-arrow-api-audit".into())
        .spawn(move || observe_public_arrow_import(batch))
        .map_err(|error| format!("could not start Arrow audit thread: {error}"))?;
    let observed = match worker.join() {
        Ok(observed) => observed,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|value| (*value).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "public Arrow import panicked".into());
            public_map_error("client_panic", message)
        }
    };
    let gaps = classify_map(&defaults, &observed);
    let canonical = observed["client_serialized_map"].as_object().map(|map| {
        map.get("entry_name") == Some(&json!("entries"))
            && map.get("child_names") == Some(&json!(["key", "value"]))
    });
    let mut report = observed;
    report["boundary"] = json!("Arrow Rust to Arrow Java map IPC");
    report["api"] = json!("SparkSession::builder().remote(...).get_or_create().create_dataframe_from_arrow(..., None).to_relation()");
    report["api_source"] = json!("Public APIs from the official base, used through the pinned fork with its unmerged map patch.");
    report["arrow_builder_defaults"] = defaults;
    report["canonical_client_map_names"] = json!(canonical);
    report["client_api_gap"] = json!(!gaps.is_empty());
    report["corpus_map_names"] = json!(["entries", "key", "value"]);
    report["corpus_map_origin"] =
        json!("Explicit canonical names in manually serialized Arrow fixtures.");
    Ok((report, gaps))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_deprecated_array_types(literal: &mut proto::expression::Literal) {
        if let Some(proto::expression::literal::LiteralType::Array(array)) =
            literal.literal_type.as_mut()
        {
            #[allow(deprecated)]
            {
                array.element_type = None;
            }
            for child in &mut array.elements {
                clear_deprecated_array_types(child);
            }
        }
    }

    fn valid_map_observation() -> (Value, Value) {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        let defaults = map_observation(builder.finish().data_type()).unwrap();
        let mut map = defaults.clone();
        map["entry_name"] = json!("entries");
        map["child_names"] = json!(["key", "value"]);
        let observed = json!({
            "serialized_top_level_schema":{
                "fields":[{"name":"mapping","nullable":true,"metadata":{}}],
                "metadata":{},
            },
            "client_serialized_map":map,
            "declared_spark_schema":null,
            "serialized_rows":0,
        });
        (defaults, observed)
    }

    #[test]
    fn map_contract_mutations_are_reported_as_gaps() {
        let (defaults, observed) = valid_map_observation();
        assert!(classify_map(&defaults, &observed).is_empty());
        for (pointer, replacement) in [
            ("/client_serialized_map/entry_name", json!("pairs")),
            (
                "/client_serialized_map/child_names",
                json!(["keys", "values"]),
            ),
            ("/client_serialized_map/key_type", json!("Int32")),
            ("/client_serialized_map/value_type", json!("Int32")),
            ("/client_serialized_map/entry_nullable", json!(true)),
            ("/client_serialized_map/key_nullable", json!(true)),
            ("/client_serialized_map/value_nullable", json!(false)),
            ("/client_serialized_map/keys_sorted", json!(true)),
            (
                "/serialized_top_level_schema/fields/0/name",
                json!("renamed"),
            ),
            (
                "/serialized_top_level_schema/fields/0/nullable",
                json!(false),
            ),
            (
                "/serialized_top_level_schema/fields/0/metadata",
                json!({"changed":"yes"}),
            ),
            (
                "/serialized_top_level_schema/metadata",
                json!({"changed":"yes"}),
            ),
            ("/declared_spark_schema", json!("mapping MAP<STRING, INT>")),
            ("/serialized_rows", json!(1)),
            ("/client_serialized_map", Value::Null),
        ] {
            let mut changed = observed.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            let gaps = classify_map(&defaults, &changed);
            assert_eq!(gaps.len(), 1, "{pointer}");
            assert_eq!(gaps[0]["id"], "arrow_map_import_contract_regression");
            assert!(!gaps[0]["mismatches"].as_array().unwrap().is_empty());
        }
        let mut extra_field = observed;
        extra_field["serialized_top_level_schema"]["fields"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"extra","nullable":true,"metadata":{}}));
        assert_eq!(classify_map(&defaults, &extra_field).len(), 1);
    }

    #[test]
    fn malformed_output_and_public_api_failures_are_reported_as_gaps() {
        let (defaults, _) = valid_map_observation();
        let malformed = proto::Relation {
            rel_type: Some(proto::relation::RelType::LocalRelation(
                proto::LocalRelation {
                    data: Some(vec![0xff, 0, 1].into()),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        for observed in [
            observe_map_relation(proto::Relation::default()),
            observe_map_relation(proto::Relation {
                rel_type: Some(proto::relation::RelType::LocalRelation(Default::default())),
                ..Default::default()
            }),
            observe_map_relation(malformed),
            public_map_error("session_builder", "builder regression"),
            public_map_error("public_arrow_import", "import regression"),
            public_map_error("client_panic", "client regression"),
            public_map_error("ipc_batch", "malformed IPC batch"),
        ] {
            assert!(!observed["error"].is_null());
            let gaps = classify_map(&defaults, &observed);
            assert_eq!(gaps.len(), 1);
            assert_eq!(gaps[0]["id"], "arrow_map_import_contract_regression");
            assert_eq!(gaps[0]["evidence"], observed);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn audit_calls_the_public_arrow_api_inside_a_current_thread_runtime() {
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread,
        );
        let value = audit().unwrap();
        assert_eq!(value["has_client_gaps"], false);
        let map = &value["coverage_layers"]["interoperability"][0];
        assert_eq!(map["canonical_client_map_names"], true);
        assert_eq!(map["client_serialized_map"]["value_type"], "Int64");
        assert_eq!(map["serialized_rows"], 0);
    }

    #[test]
    fn pinned_public_apis_satisfy_array_and_arrow_map_contracts() {
        let value = audit().unwrap();
        assert_eq!(value["has_client_gaps"], false);
        assert_eq!(value["client_gaps"], json!([]));
        assert_eq!(value["resolved_contracts"][0]["passed"], true);
        assert_eq!(value["resolved_contracts"][1]["passed"], true);
        assert_eq!(
            value["client_provenance"]["official_base_revision"],
            OFFICIAL_BASE_REVISION
        );
        assert_eq!(value["client_provenance"]["fork_revision"], CLIENT_REVISION);
        assert_eq!(
            value["client_provenance"]["map_patch_paths"],
            json!([
                "crates/spark-connect/src/arrow_input.rs",
                "crates/spark-connect/src/session.rs",
                "crates/spark-connect/tests/arrow_map_input.rs",
            ])
        );
        assert_eq!(
            value["client_provenance"]["map_patch_source"],
            json!(format!("https://github.com/dbtsai/spark-connect-rust/compare/{OFFICIAL_BASE_REVISION}...{CLIENT_REVISION}"))
        );
        assert_eq!(
            value["client_provenance"]["map_patch_upstream_pr"],
            "https://github.com/apache/spark-connect-rust/pull/396"
        );
        assert_eq!(
            value["client_provenance"]["map_patch_upstream_issue"],
            "SPARK-61212"
        );
        assert_eq!(
            value["client_provenance"]["map_patch_upstream_status"],
            "upstream_pr_open_not_merged"
        );
        for name in [
            "populated_array",
            "empty_array",
            "nested_array",
            "nested_array[0]",
            "nested_array[1]",
        ] {
            let observation = value["array_literal_probes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|observation| observation["path"] == name)
                .unwrap();
            assert_eq!(observation["type_contract_matches"], true);
            assert_eq!(observation["array_type_available"], true);
        }
        let map = &value["coverage_layers"]["interoperability"][0];
        assert_eq!(
            map["arrow_builder_defaults"]["child_names"],
            json!(["keys", "values"])
        );
        assert_eq!(map["client_serialized_map"]["entry_name"], "entries");
        assert_eq!(
            map["client_serialized_map"]["child_names"],
            json!(["key", "value"])
        );
        assert_eq!(map["serialized_rows"], 0);
        assert_eq!(
            value["coverage_layers"]["rpc_routes"]["routes"]
                .as_array()
                .unwrap()
                .len(),
            12
        );
    }

    #[test]
    fn audit_rejects_missing_wrong_and_nested_wrong_array_element_types() {
        for (name, expression, expected) in array_probes() {
            let Some(proto::expression::ExprType::Literal(original)) =
                expression.to_proto().expr_type
            else {
                panic!("array literal");
            };
            for replacement in [
                None,
                Some(
                    SparkType::String {
                        collation: "UTF8_BINARY".into(),
                    }
                    .to_proto(),
                ),
            ] {
                let mut literal = original.clone();
                if replacement.is_none() {
                    // This case removes every local type source. A correct
                    // root annotation alone is a valid representation.
                    literal.data_type = None;
                }
                let Some(proto::expression::literal::LiteralType::Array(array)) =
                    literal.literal_type.as_mut()
                else {
                    panic!("array value");
                };
                #[allow(deprecated)]
                {
                    array.element_type = replacement;
                }
                let mut gaps = vec![];
                inspect_literal(name, &literal, &expected.to_proto(), &mut vec![], &mut gaps)
                    .unwrap();
                assert!(!gaps.is_empty(), "{name}");
            }
            if name == "nested_array" {
                let mut literal = original.clone();
                let Some(proto::expression::literal::LiteralType::Array(outer)) =
                    literal.literal_type.as_mut()
                else {
                    panic!("outer array");
                };
                #[allow(deprecated)]
                {
                    outer.element_type = Some(
                        SparkType::Array {
                            element_type: Box::new(SparkType::Integer),
                            contains_null: false,
                        }
                        .to_proto(),
                    );
                }
                let mut gaps = vec![];
                inspect_literal(name, &literal, &expected.to_proto(), &mut vec![], &mut gaps)
                    .unwrap();
                assert_eq!(gaps.len(), 1);
                assert_eq!(gaps[0]["evidence"]["path"], "nested_array");

                let mut literal = original;
                let Some(proto::expression::literal::LiteralType::Array(outer)) =
                    literal.literal_type.as_mut()
                else {
                    panic!("outer array");
                };
                let Some(proto::expression::literal::LiteralType::Array(inner)) =
                    outer.elements[1].literal_type.as_mut()
                else {
                    panic!("inner array");
                };
                #[allow(deprecated)]
                {
                    inner.element_type = Some(SparkType::Long.to_proto());
                }
                let mut gaps = vec![];
                inspect_literal(name, &literal, &expected.to_proto(), &mut vec![], &mut gaps)
                    .unwrap();
                assert_eq!(gaps.len(), 1);
                assert_eq!(gaps[0]["evidence"]["path"], "nested_array[1]");
            }
        }
    }

    #[test]
    fn audit_accepts_root_only_and_dual_array_annotations_but_rejects_conflicts() {
        for (name, expression, expected) in array_probes() {
            let Some(proto::expression::ExprType::Literal(original)) =
                expression.to_proto().expr_type
            else {
                panic!("array literal");
            };
            let expected = expected.to_proto();
            let mut dual = original.clone();
            dual.data_type = Some(expected.clone());
            let mut root_only = dual.clone();
            clear_deprecated_array_types(&mut root_only);
            for valid in [&dual, &root_only] {
                let mut gaps = vec![];
                inspect_literal(name, valid, &expected, &mut vec![], &mut gaps).unwrap();
                assert!(gaps.is_empty(), "{name}: {gaps:?}");
            }

            let mut wrong_root = original;
            wrong_root.data_type = Some(SparkType::Long.to_proto());
            let mut wrong_deprecated = root_only.clone();
            let Some(proto::expression::literal::LiteralType::Array(array)) =
                wrong_deprecated.literal_type.as_mut()
            else {
                panic!("array value");
            };
            #[allow(deprecated)]
            {
                array.element_type = Some(SparkType::Boolean.to_proto());
            }
            for invalid in [&wrong_root, &wrong_deprecated] {
                let mut gaps = vec![];
                inspect_literal(name, invalid, &expected, &mut vec![], &mut gaps).unwrap();
                assert!(!gaps.is_empty(), "{name}");
            }

            if name == "nested_array" {
                let Some(proto::expression::literal::LiteralType::Array(outer)) =
                    root_only.literal_type.as_mut()
                else {
                    panic!("outer array");
                };
                let Some(proto::expression::literal::LiteralType::Array(inner)) =
                    outer.elements[1].literal_type.as_mut()
                else {
                    panic!("inner array");
                };
                #[allow(deprecated)]
                {
                    inner.element_type = Some(SparkType::Long.to_proto());
                }
                let mut gaps = vec![];
                inspect_literal(name, &root_only, &expected, &mut vec![], &mut gaps).unwrap();
                assert_eq!(gaps.len(), 1);
                assert_eq!(gaps[0]["evidence"]["path"], "nested_array[1]");
            }
        }
    }

    #[test]
    fn a_conflicting_parent_annotation_does_not_repeat_as_missing_child_types() {
        let (name, expression, expected) = array_probes()
            .into_iter()
            .find(|(name, _, _)| *name == "nested_array")
            .unwrap();
        let Some(proto::expression::ExprType::Literal(mut literal)) =
            expression.to_proto().expr_type
        else {
            panic!("nested array literal");
        };
        clear_deprecated_array_types(&mut literal);
        let expected = expected.to_proto();
        literal.data_type = Some(expected.clone());
        let Some(proto::expression::literal::LiteralType::Array(outer)) =
            literal.literal_type.as_mut()
        else {
            panic!("outer array");
        };
        #[allow(deprecated)]
        {
            outer.element_type = Some(SparkType::Boolean.to_proto());
        }
        let mut observations = vec![];
        let mut gaps = vec![];
        inspect_literal(name, &literal, &expected, &mut observations, &mut gaps).unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0]["evidence"]["path"], "nested_array");
        for path in ["nested_array[0]", "nested_array[1]"] {
            let child = observations
                .iter()
                .find(|value| value["path"] == path)
                .unwrap();
            assert_eq!(child["type_supplied_by_parent"], true);
            assert_eq!(child["type_contract_matches"], true);
        }
    }

    #[test]
    fn auditing_public_apis_is_deterministic_and_does_not_adjust_the_corpus() {
        assert_eq!(audit().unwrap(), audit().unwrap());
        let cases = crate::corpus::build("smoke").unwrap();
        let typed = cases
            .iter()
            .find(|case| case.name == "typed_literals")
            .unwrap();
        assert!(typed.metadata.get("rust_api_gap_ids").is_none());
        assert_eq!(
            typed.metadata["details"]["literal_data_type_fields"],
            "official_client_unmodified"
        );
        assert_eq!(typed.relation, crate::corpus::raw_typed_literals().relation);
    }
}
