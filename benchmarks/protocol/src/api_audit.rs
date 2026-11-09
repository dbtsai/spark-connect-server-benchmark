//! Inspect unmodified official Rust API output separately from compatibility fixes.

use arrow::array::builder::{Int64Builder, MapBuilder, StringBuilder};
use arrow::array::Array;
use arrow::datatypes::DataType;
use prost::Message;
use serde_json::{json, Value};
use spark_connect_proto as proto;

pub const CLIENT_REVISION: &str = "9ed82b874357cdff887977864026d98f8b00f434";

/// An audit uses actual pinned-client output. No Spark process or Python is involved.
/// Protocol coverage through generated gRPC bindings is identified separately
/// from the public logical-plan builders and is not a high-level API parity claim.
pub fn audit() -> Result<Value, String> {
    let raw = crate::corpus::raw_typed_literals();
    let Some(proto::relation::RelType::Project(project)) = raw.relation.rel_type.as_ref() else {
        return Err("official typed literal plan is not a projection".into());
    };
    let mut observations = vec![];
    let mut gaps = vec![];
    for expression in &project.expressions {
        let Some(proto::expression::ExprType::Alias(alias)) = expression.expr_type.as_ref() else {
            return Err("official typed literal output has no named alias".into());
        };
        let Some(proto::expression::ExprType::Literal(literal)) = alias
            .expr
            .as_deref()
            .and_then(|value| value.expr_type.as_ref())
        else {
            return Err("official typed literal output is not a literal".into());
        };
        observations.push(
            json!({"column":alias.name,"root_data_type_present":literal.data_type.is_some()}),
        );
        if let Some(proto::expression::literal::LiteralType::Array(array)) =
            literal.literal_type.as_ref()
        {
            #[allow(deprecated)]
            let element_type_present = array.element_type.is_some();
            if literal.data_type.is_none() && !element_type_present {
                gaps.push(json!({
                    "id":"typed_array_literal_annotations",
                    "layer":"official_rust_public_expression_api",
                    "api":"LiteralExpression::Array { element_type: Integer, elements: [1, NULL, 3] }",
                    "evidence":{"root_data_type_present":false,"array_element_type_present":false,
                        "element_count":array.elements.len()},
                    "impact":"The declared element type is lost during protobuf conversion; the tested Spark planner requires a complex literal type annotation.",
                    "workaround":"The typed_literals corpus case explicitly fills Literal.data_type from its declared Rust schema before timing.",
                    "benchmark_case":"typed_literals",
                }));
            }
        }
    }

    // Arrow's default physical map child names are a separate interoperability
    // issue, not evidence that a Spark Rust high-level API is missing.
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let default_map = builder.finish();
    let DataType::Map(entries, _) = default_map.data_type() else {
        return Err("Arrow map builder did not produce a map".into());
    };
    let DataType::Struct(children) = entries.data_type() else {
        return Err("Arrow map builder has no entry struct".into());
    };
    let child_names: Vec<_> = children.iter().map(|field| field.name().clone()).collect();
    Ok(json!({
        "schema_version":1,
        "client_revision":CLIENT_REVISION,
        "status":if gaps.is_empty(){"no_observed_client_gaps"}else{"client_gaps_observed"},
        "has_client_gaps":!gaps.is_empty(),
        "client_gaps":gaps,
        "unmodified_literal_observations":observations,
        "unmodified_relation_bytes":raw.relation.encoded_len(),
        "coverage_layers":{
            "logical_plans":"Official Rust public logical-plan/expression builders; compatibility adjustments are declared in case metadata.",
            "rpc_routes":{"source":"Official Rust generated protobuf/gRPC bindings","routes":crate::probes::ROUTES,
                "high_level_session_api_parity":"not established by generated-binding probes"},
            "interoperability":[{"boundary":"Arrow Rust to Arrow Java map IPC","default_child_names":child_names,
                "corpus_child_names":["key","value"],"client_api_gap":false}],
        },
        "scope":"Observed serialization contracts at the pinned revision; not an exhaustive inventory of missing PySpark APIs or a Spark execution test.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_official_array_loses_its_declared_type_and_is_reported() {
        let value = audit().unwrap();
        assert_eq!(value["has_client_gaps"], true);
        assert_eq!(
            value["client_gaps"][0]["id"],
            "typed_array_literal_annotations"
        );
        assert_eq!(value["client_gaps"][0]["evidence"]["element_count"], 3);
        assert_eq!(
            value["coverage_layers"]["rpc_routes"]["routes"]
                .as_array()
                .unwrap()
                .len(),
            12
        );
    }

    #[test]
    fn auditing_raw_api_does_not_silently_apply_corpus_adjustments() {
        assert_eq!(audit().unwrap(), audit().unwrap());
        let cases = crate::corpus::build("smoke").unwrap();
        let typed = cases
            .iter()
            .find(|case| case.name == "typed_literals")
            .unwrap();
        assert_eq!(
            typed.metadata["rust_api_gap_ids"],
            json!(["typed_array_literal_annotations"])
        );
        assert_eq!(
            typed.metadata["details"]["literal_data_type_fields"],
            "explicit_from_declared_schema"
        );
        assert_eq!(audit().unwrap()["has_client_gaps"], true);
    }
}
