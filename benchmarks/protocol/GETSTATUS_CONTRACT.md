# Synthetic GetStatus contract and comparison limits

The Rust and JVM fixtures share a deterministic GetStatus contract for protocol
and concurrency testing. This contract is not a claim of exact Spark server
semantics. The planning profile excludes GetStatus; these fixture measurements
do not exercise the real Spark GetStatus handler.

The comparison below references Apache Spark commit
[`2fe65fefc106478ff3a4ba797e71a279644a54d7`](https://github.com/apache/spark/commit/2fe65fefc106478ff3a4ba797e71a279644a54d7).
The fixture compatibility descriptor follows the official Rust protocol schema
at [`925dc876`](https://github.com/apache/spark-connect-rust/blob/925dc876f63f1f779fe176f62575a0aa96cc6886/crates/spark-connect-proto/proto/spark/connect/base.proto#L1228-L1308).
Schema compatibility and server lifecycle behavior are separate contracts.

| Request or lifecycle event | Fixture contract | Referenced Spark behavior |
| --- | --- | --- |
| Absent `operation_status` | Return no operation statuses. | Same selector behavior: the handler populates statuses only when the selector is present. |
| Present selector with no IDs | List every retained fixture operation, sorted by ID. | Lists distinct active and inactive IDs. Sorting is not promised; collection order may differ. |
| Explicit IDs | Preserve first occurrence order and remove duplicates. | Uses Scala `distinct` on the requested sequence, preserving first occurrence order. |
| Unknown operation ID | Return an `UNKNOWN` entry. | Looks in active and inactive operations, then returns `UNKNOWN` if neither exists. |
| Normal fixture response fully drained | Operation is immediately `SUCCEEDED`. | Actual executions pass through `TERMINATING` until resources close; final state depends on the termination reason. |
| Held / interrupted fixture operation | `RUNNING` / immediately `CANCELLED`. | Running and cancellation transitions depend on the execution lifecycle; intermediate `TERMINATING` is possible. |
| `ReleaseExecute.release_all` | Forget the operation; a later explicit status lookup is `UNKNOWN`, and the all-ID selector omits it. | Retains inactive operation information and updates it at cleanup. GetStatus returns the recorded terminal state after cleanup, with inactive information taking priority over active state, until its `expireAfterAccess` timeout. Status reads refresh that timeout. |
| Previously unseen valid user/session identity | Create an empty fixture session lazily. | GetStatus requires an existing session; an unknown session raises `INVALID_HANDLE.SESSION_NOT_FOUND`. |
| Released session | Reject further fixture calls with `FAILED_PRECONDITION`. | Closed and unknown sessions have distinct Spark error classes. No exact gRPC status-code parity is asserted. |
| Server session identity and observed-ID mismatch | Echo the client session ID as `server_side_session_id`; reject a different observed ID with `FAILED_PRECONDITION`. | Return the independent Spark session UUID as `serverSessionId`; validate against that UUID and raise `INVALID_HANDLE.SESSION_CHANGED` on mismatch. |

Source references for those rules:

- [Selector presence, explicit-ID deduplication and filtering](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectGetStatusHandler.scala#L33-L72).
- [Active/inactive lookup, UNKNOWN fallback, and all-ID enumeration](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectGetStatusHandler.scala#L75-L118).
- [Execution status and termination-reason mapping](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectGetStatusHandler.scala#L121-L158).
- [Existing-session lookup and closed/not-found errors](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectSessionManager.scala#L109-L131).
- [Independent server-side session UUID](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SessionHolder.scala#L172-L182),
  [response session ID](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectGetStatusHandler.scala#L42-L45), and
  [observed-ID mismatch validation](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectSessionManager.scala#L79-L88).
- [Release-all removes the holder](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectReleaseExecuteHandler.scala#L46-L65),
  [removal records inactive information](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectExecutionManager.scala#L151-L173),
  [cleanup updates the retained terminal information](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/ExecuteHolder.scala#L283-L313), and
  [inactive cache `expireAfterAccess` configuration](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SessionHolder.scala#L104-L115).
  The collection and recording methods are at
  [SessionHolder lines 238–273](https://github.com/apache/spark/blob/2fe65fefc106478ff3a4ba797e71a279644a54d7/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SessionHolder.scala#L238-L273).

The fixture probes also validate identity isolation, observed-session mismatch,
missing user context, malformed session IDs, and released-session rejection.
They explicitly check UNKNOWN and all-ID omission after operation release in
both live fixtures; a Rust fixture unit test checks the same release contract.
These checks enforce fixture correctness; they do not establish Spark's exact
validation errors, plugin-extension semantics, cache retention, or lifecycle races.

## What the timed cell measures

The Rust fixture decodes and encodes GetStatus with prost. The JVM fixture uses
Spark's shaded protobuf `DynamicMessage` marshallers and reflection-based field
access. It does so even if a future Spark assembly supplies generated GetStatus
descriptors. On Spark 4.2, the fixture builds a compatibility descriptor because
the assembly lacks those generated messages. This route belongs only to the
synthetic JVM fixture.

Keep the timings, but interpret their ratio as **prost versus the JVM dynamic
compatibility implementation**. The ratio does not isolate generated-code
performance and does not compare production Spark frontends. Raw client cells
and capabilities carry a `protocol_implementation` compatibility label. Reporting
adds the per-target codec and observed JVM descriptor provenance to each
GetStatus capability and cell group, each matched pair, the JSON/CSV outputs, and
the Markdown tables.

New load reports set `settings.fixture_get_status_required = true` for fixture
mode. An `UNIMPLEMENTED` fixture GetStatus fails untimed probes and timed
capability validation. Reporting also requires observed GetStatus calls and JVM
descriptor provenance. Legacy reports that recorded an unavailable capability
without timing GetStatus remain readable; their original frozen reporter is the
authoritative historical analysis.

New settings record the actual fork pin as `client_revision` and its upstream
base as `official_base_revision`. The historical `official_rust_revision` key
remains a compatibility alias for `client_revision`; its name does not imply
that the fork SHA is an upstream commit. GetStatus reporting prefers the new
key and falls back to the legacy alias when reading older results.

## Wire compatibility regression checks

The shared `testdata/get-status/*.hex` vectors cover absent and present-empty
selectors, present-empty optional strings, duplicate selector IDs, a request
extension, and ordered response states. The Rust test
`get_status_shared_java_vectors_match_prost_encoding_and_presence` independently
constructs those messages and verifies prost encoding and decoding against the
vectors. `GetStatusCompatTest` reads the same vectors through the actual shaded
gRPC marshallers, checks descriptor presence and enum values, verifies response
bytes, and checks that binding GetStatus preserves unrelated routes.

After building the JVM tests with `jvm/build.sh --tests`, run the runtime check:

```sh
"$JAVA_HOME/bin/java" \
  -cp ".build/jvm/bridge-test.jar:$SPARK_HOME/jars/*" \
  org.apache.spark.sql.connect.nativebridge.benchmark.GetStatusCompatTest \
  benchmarks/protocol/testdata/get-status
```

For a source assembly, replace `$SPARK_HOME/jars/*` with
`$SPARK_HOME/assembly/target/scala-2.13/jars/*`. This test starts no Spark session
or network listener. The four-target suite separately checks the live route.
