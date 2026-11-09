//! The supported Spark schema boundary. Unsupported Arrow types fail explicitly.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use prost::Message;
use spark_connect_proto as proto;
use tonic::Status;

pub(crate) const MAX_RESULT_BYTES: usize = 3 * 1024 * 1024;

pub(crate) fn result_schema(schema: &Schema) -> Result<proto::DataType, Status> {
    let schema = spark_schema(schema)?;
    validate_schema_size(&schema)?;
    Ok(schema)
}

pub(crate) fn validate_schema_size(schema: &proto::DataType) -> Result<(), Status> {
    if schema.encoded_len() > MAX_RESULT_BYTES {
        return Err(Status::resource_exhausted(
            "Spark schema exceeds the 3 MiB result limit",
        ));
    }
    Ok(())
}

/// Compare Spark-visible fields and types, allowing equivalent Arrow physical
/// representations and differences in nullability or field/schema metadata.
pub(crate) fn comparable_schema(schema: &Schema) -> Result<proto::DataType, Status> {
    fn field(input: &FieldRef) -> FieldRef {
        Arc::new(Field::new(input.name(), data_type(input.data_type()), true))
    }

    fn data_type(input: &DataType) -> DataType {
        match input {
            DataType::List(child) => DataType::List(field(child)),
            DataType::LargeList(child) => DataType::LargeList(field(child)),
            DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
            DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
            other => other.clone(),
        }
    }

    let fields: Vec<_> = schema.fields().iter().map(field).collect();
    spark_schema(&Schema::new(fields))
}

pub fn spark_schema(schema: &Schema) -> Result<proto::DataType, Status> {
    spark_type(&DataType::Struct(schema.fields().clone()))
}

fn spark_field(field: &Field) -> Result<proto::data_type::StructField, Status> {
    if !field.metadata().is_empty() {
        return Err(Status::unimplemented(
            "Arrow field metadata in Spark schemas",
        ));
    }
    Ok(proto::data_type::StructField {
        name: field.name().clone(),
        data_type: Some(spark_type(field.data_type())?),
        nullable: field.is_nullable(),
        metadata: None,
    })
}

fn spark_type(data_type: &DataType) -> Result<proto::DataType, Status> {
    use proto::data_type::{self as pb, Kind};
    let kind = match data_type {
        DataType::Null => Kind::Null(pb::Null::default()),
        DataType::Boolean => Kind::Boolean(pb::Boolean::default()),
        DataType::Int8 => Kind::Byte(pb::Byte::default()),
        DataType::Int16 => Kind::Short(pb::Short::default()),
        DataType::Int32 => Kind::Integer(pb::Integer::default()),
        DataType::Int64 => Kind::Long(pb::Long::default()),
        DataType::Float32 => Kind::Float(pb::Float::default()),
        DataType::Float64 => Kind::Double(pb::Double::default()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Kind::String(pb::String {
            collation: "UTF8_BINARY".into(),
            ..Default::default()
        }),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Kind::Binary(pb::Binary::default())
        }
        DataType::Date32 => Kind::Date(pb::Date::default()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Kind::TimestampNtz(pb::TimestampNtz::default())
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            Kind::Timestamp(pb::Timestamp::default())
        }
        DataType::Decimal128(precision, scale) => Kind::Decimal(pb::Decimal {
            precision: Some(i32::from(*precision)),
            scale: Some(i32::from(*scale)),
            ..Default::default()
        }),
        DataType::List(field) | DataType::LargeList(field) => Kind::Array(Box::new(pb::Array {
            element_type: Some(Box::new(spark_type(field.data_type())?)),
            contains_null: field.is_nullable(),
            ..Default::default()
        })),
        DataType::Struct(fields) => Kind::Struct(pb::Struct {
            fields: fields
                .iter()
                .map(|field| spark_field(field))
                .collect::<Result<_, _>>()?,
            ..Default::default()
        }),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(Status::invalid_argument(
                    "Arrow map entries must be a struct",
                ));
            };
            if fields.len() != 2 {
                return Err(Status::invalid_argument(
                    "Arrow map entries need key and value fields",
                ));
            }
            Kind::Map(Box::new(pb::Map {
                key_type: Some(Box::new(spark_type(fields[0].data_type())?)),
                value_type: Some(Box::new(spark_type(fields[1].data_type())?)),
                value_contains_null: fields[1].is_nullable(),
                ..Default::default()
            }))
        }
        other => {
            return Err(Status::unimplemented(format!(
                "Arrow result type {other:?}"
            )))
        }
    };
    Ok(proto::DataType { kind: Some(kind) })
}

pub(crate) fn arrow_type(data_type: &proto::DataType) -> Result<DataType, Status> {
    use proto::data_type::Kind;
    Ok(
        match data_type
            .kind
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing type"))?
        {
            Kind::Null(_) => DataType::Null,
            Kind::Boolean(_) => DataType::Boolean,
            Kind::Byte(_) => DataType::Int8,
            Kind::Short(_) => DataType::Int16,
            Kind::Integer(_) => DataType::Int32,
            Kind::Long(_) => DataType::Int64,
            Kind::Float(_) => DataType::Float32,
            Kind::Double(_) => DataType::Float64,
            Kind::String(value)
                if value.collation.is_empty() || value.collation == "UTF8_BINARY" =>
            {
                DataType::Utf8
            }
            Kind::Binary(_) => DataType::Binary,
            Kind::Date(_) => DataType::Date32,
            Kind::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Kind::TimestampNtz(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            other => {
                return Err(Status::unimplemented(format!(
                    "Spark cast/literal type {other:?}"
                )))
            }
        },
    )
}

/// Parse the JSON struct schema emitted by the official Rust and Python clients.
/// DDL schemas and schema-driven conversion of local values are intentionally unsupported.
pub(crate) fn local_schema(json: &str) -> Result<Schema, Status> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|_| Status::unimplemented("LocalRelation schema must be Spark JSON"))?;
    match json_type(&value)? {
        DataType::Struct(fields) => Ok(Schema::new(fields)),
        _ => Err(Status::invalid_argument(
            "LocalRelation schema must describe a struct",
        )),
    }
}

fn json_type(value: &serde_json::Value) -> Result<DataType, Status> {
    if let Some(name) = value.as_str() {
        return match name {
            "void" => Ok(DataType::Null),
            "boolean" => Ok(DataType::Boolean),
            "byte" => Ok(DataType::Int8),
            "short" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "long" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "string" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            "timestamp" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            _ => Err(Status::unimplemented(format!(
                "LocalRelation JSON type {name}"
            ))),
        };
    }
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("struct") => {
            let fields = value
                .get("fields")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| Status::invalid_argument("missing struct fields"))?;
            let fields = fields
                .iter()
                .map(|field| {
                    let name = field
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| Status::invalid_argument("missing field name"))?;
                    let data_type = json_type(
                        field
                            .get("type")
                            .ok_or_else(|| Status::invalid_argument("missing field type"))?,
                    )?;
                    let nullable = field
                        .get("nullable")
                        .and_then(serde_json::Value::as_bool)
                        .ok_or_else(|| Status::invalid_argument("missing field nullability"))?;
                    if field.get("metadata").is_some_and(|metadata| {
                        metadata.as_object().is_none_or(|object| !object.is_empty())
                    }) {
                        return Err(Status::unimplemented("LocalRelation field metadata"));
                    }
                    Ok(Field::new(name, data_type, nullable))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Some("array") => Err(Status::unimplemented(
            "LocalRelation array schemas are not supported",
        )),
        _ => Err(Status::unimplemented("LocalRelation JSON complex type")),
    }
}
