# Official Rust API audit

The protocol benchmark uses the official Apache Spark Connect Rust client at
revision `9ed82b874357cdff887977864026d98f8b00f434`. Rust owns the corpus,
orchestration, validation, reporting, and comparison gates. Python is not needed
to run or test this profile.

Run the audit without starting Spark:

```sh
target/release/spark-connect-protocol-bench api-audit --output api-audit.json
target/release/spark-connect-protocol-bench api-audit --strict
```

The first command saves the observations and exits successfully when the audit
can run. `--strict` exits with status 1 when it observes a client gap. Every
exported corpus manifest also contains the audit under `rust_api_audit`.

## Observed gap: typed array literal annotations

The public expression
`LiteralExpression::Array { element_type: Integer, elements: [1, NULL, 3] }`
loses its declared element type during conversion to the generated protobuf.
Both `Literal.data_type` and `Array.element_type` are absent at the pinned
revision. The tested Spark planner requires a complex literal type annotation.

The audit inspects the unmodified public API output. The `typed_literals`
benchmark case fills root `Literal.data_type` fields from its declared Rust
schema before timing. Its metadata records
`rust_api_gap_ids: ["typed_array_literal_annotations"]` and
`details.literal_data_type_fields: "explicit_from_declared_schema"`. This keeps
the planning benchmark runnable while retaining evidence of the client gap.
The audit regression test will fail when the pinned client behavior changes;
review the evidence and remove the workaround when upgrading to a fixed client.

## Coverage boundaries

| Boundary | What this suite establishes |
| --- | --- |
| Public logical-plan and expression builders | Representative plan shapes, literal types, and deterministic serialization used by the corpus. |
| Generated protobuf/gRPC bindings | Probes for all 12 pinned service routes, with supported and unsupported outcomes recorded explicitly. |
| High-level session/DataFrame API | Full parity with PySpark is not established by generated-binding route probes. |
| Arrow Rust/Java interoperability | Default Arrow Rust map child names are recorded separately; the corpus uses `key`/`value` names accepted by Arrow Java. This is not classified as a missing Spark client API. |

Real planning targets perform analysis and metadata operations with zero query
jobs. Fixture targets validate synthetic protocol behavior. Neither establishes
query execution correctness. The older executed PySpark benchmark and JVM
acceptance scripts are optional, separate compatibility checks; they are not
dependencies of the Rust protocol harness.
