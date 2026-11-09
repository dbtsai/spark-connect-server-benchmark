# Pinned-fork Rust API audit

The protocol benchmark uses a fork of the Apache Spark Connect Rust client at
revision `87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce`, based on official revision
[`925dc876f63f1f779fe176f62575a0aa96cc6886`](https://github.com/apache/spark-connect-rust/commit/925dc876f63f1f779fe176f62575a0aa96cc6886).
The public APIs and generated protocol bindings are derived from that official
base. Arrow map normalization comes from the
[complete fork diff](https://github.com/dbtsai/spark-connect-rust/compare/925dc876f63f1f779fe176f62575a0aa96cc6886...87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce)
in `crates/spark-connect/src/arrow_input.rs`, integrated through
`crates/spark-connect/src/session.rs`, with tests in
`crates/spark-connect/tests/arrow_map_input.rs`. The fix is proposed upstream in
[PR #396](https://github.com/apache/spark-connect-rust/pull/396)
([SPARK-61212](https://issues.apache.org/jira/browse/SPARK-61212)) and is not merged.
The audit JSON records both revisions and the patch source under
`client_provenance`. Rust owns the corpus, orchestration, validation, reporting,
and comparison gates. Python is not needed to run or test this profile.

Run the audit without starting Spark:

```sh
target/release/spark-connect-protocol-bench api-audit --output api-audit.json
target/release/spark-connect-protocol-bench api-audit --strict
```

The first command saves observations and exits successfully when the audit can
run, including when it observes a client contract regression. `--strict` exits
with status 1 when it observes a client gap. Every exported corpus manifest also
contains the audit under `rust_api_audit`.

The literal audit has setup prerequisites: a projection with a declared struct
schema, matching expression/field counts, one matching alias for each literal,
literal-shaped values and array probes, and declared field/array element types.
These are invariants of the harness's integration with the pinned builders.
Failing one returns `Err` and stops corpus export, including when an upstream
API change caused the unexpected shape. Non-strict mode preserves observed
client contract gaps; it does not suppress setup errors. Map output and public
Arrow API regressions are classified as gaps as described below.

## Resolved contract: typed array literal element types

SPARK-61119, commit
[`70688ec07ff87ea5cd4d685247074feeaa2c2b7f`](https://github.com/apache/spark-connect-rust/commit/70688ec07ff87ea5cd4d685247074feeaa2c2b7f),
is included in the official base revision. It preserves the public
`LiteralExpression::Array` element type in protobuf `Array.element_type`. The
root `Literal.data_type` remains unset. The pinned Spark 4.2 assembly source at
`250ccca3f2b5f043e20b415adf17bda5d202c60f` selects a
[root type when present](https://github.com/apache/spark/blob/250ccca3f2b5f043e20b415adf17bda5d202c60f/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala#L489-L497)
and otherwise supports the
[deprecated array element type](https://github.com/apache/spark/blob/250ccca3f2b5f043e20b415adf17bda5d202c60f/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala#L552-L562).
The audit accepts either a correct root `Literal.data_type` or a correct
deprecated `Array.element_type`, and checks every annotation that is present
against the complete expected type. A valid enclosing array schema can supply
the type for nested arrays without local annotations: Spark's
[array converter dispatch](https://github.com/apache/spark/blob/250ccca3f2b5f043e20b415adf17bda5d202c60f/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala#L417-L418)
uses `toScalaArrayInternal`, which
[converts each element with the enclosing element schema](https://github.com/apache/spark/blob/250ccca3f2b5f043e20b415adf17bda5d202c60f/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala#L599-L616).
Populated, empty, and nested arrays are covered, including each nested literal.
Within the current literal corpus, the audit also checks kinds, typed nulls,
decimal precision and scale, and root annotations against the expected schema.

The kind matcher covers the current `typed_literals` case and audit-only array
probes. It does not cover map, struct, interval, char/varchar, or time literals;
adding those to the corpus requires extending the matcher before interpreting
its result as a client gap. It is not a general Spark Connect literal or
protocol-parity checker.

The `typed_literals` benchmark case uses the client's unmodified output and has
no active client-gap IDs. Manifest metadata retains
`origin: "official_rust_logical_plan"` and
`details.literal_data_type_fields: "official_client_unmodified"` as historical
schema compatibility labels for public plan-builder output. These values do not
identify an official released client or claim the fork patch is merged. The
audit's `client_provenance`, under `rust_api_audit` in a corpus manifest, records
the actual fork and official-base revisions and upstream proposal. There is no
benchmark type-annotation workaround. Extra array audit probes do not change
timed corpus counts.

Resolved contracts are reported under `resolved_contracts`, separately from
active `client_gaps`. A missing or incorrect literal type is reported as a
`literal_type_contract_regression`; a present but incorrect type cannot pass the
audit merely because an annotation exists.

## Public Arrow map import contract

Arrow Rust's default `MapBuilder` uses `keys` and `values` child names. Those
builder defaults are recorded as input evidence. The audit then builds an actual
public `SparkSession`, imports a zero-row default map batch with
`create_dataframe_from_arrow(..., None)`, obtains its public `to_relation()`
representation, and decodes the serialized IPC schema. The client channel is
lazy and zero-row imports bypass configuration and upload RPCs, so the audit
needs no running server. `to_relation()` is public but documented as unstable.

The public session calls run on a dedicated standard thread so the synchronous
audit also works inside a Tokio `current_thread` runtime. Public API errors and
panics, missing or malformed relation/IPC output, altered top-level schemas,
unexpected Spark schema overrides, nonzero rows, and changed map names, types,
nullability, or sortedness become `arrow_map_import_contract_regression` entries
in `client_gaps`. A pure classifier compares the observation with the fixed
input contract. These failures remain evidence in a non-strict audit or corpus
export; they do not abort export. Failures constructing the harness input or
starting its isolation thread are setup errors.

The fork's client-serialized map must use `entries`, `key`, and `value` names.
Session and plan IDs are excluded from the audit JSON, keeping successful
observations deterministic. This audit inspects a zero-row, top-level map.
Nested maps, nonempty inline/upload paths, and buffer sharing are covered by the
client patch's tests. The timed corpus separately uses explicit canonical names
in its manually serialized Arrow fixtures; those fixtures do not establish the
public client's normalization behavior.

## Coverage boundaries

| Boundary | What this suite establishes |
| --- | --- |
| Public logical-plan and expression builders from the pinned fork | Representative plan shapes, literal types, and deterministic serialization used by the corpus. |
| Upstream-derived generated protobuf/gRPC bindings in the fork | Probes for all 12 pinned service routes, with supported and unsupported outcomes recorded explicitly. |
| High-level session/DataFrame API | Full parity with PySpark is not established by generated-binding route probes. |
| Arrow Rust/Java interoperability | Default Arrow builder names and the public client's serialized IPC schema are inspected separately. Canonical `entries`/`key`/`value` names are required at the client boundary. |

Real planning targets perform analysis and metadata operations with zero query
jobs. Fixture targets validate synthetic protocol behavior. Neither establishes
query execution correctness. The older executed PySpark benchmark and JVM
acceptance scripts are optional, separate compatibility checks; they are not
dependencies of the Rust protocol harness.
