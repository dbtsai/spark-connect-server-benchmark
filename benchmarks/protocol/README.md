# Execution-free Spark Connect concurrency benchmarks

This suite uses Apache Spark Connect Rust logical-plan builders and the
generated protobuf/gRPC client. It measures bounded concurrent RPCs with varied
plans, Arrow types, payload sizes, session sharing, and channel sharing.

The `spark-connect-protocol-bench` executable performs corpus export, fresh-process
orchestration, reporting, build comparison, and API auditing in Rust. Its public
library exposes the same `corpus`, `runner`, `report`, `compare`, and `api_audit`
modules for reuse. The protocol harness and its regression tests do not require
Python. The optional [executed-query benchmark](../README.md) and Python Spark
acceptance remain separate workflows. Rust replaces the core Python protocol
runner, reporter, comparator, and their regression tests.

Two profiles answer different questions:

| Profile | Rust target | JVM target | Work performed |
| --- | --- | --- | --- |
| Synthetic protocol | `rust-fixture` | `jvm-fixture` | Decode requests, maintain fixture session/operation state, replay identical prebuilt Arrow responses, and exercise all RPC routes. Neither fixture creates a Spark session or evaluates a plan. |
| Real planning | `rust-jvm-planning` | `jvm-planning` | Analyze real plans and access session configuration through the native Spark engine or upstream Spark Connect. A Spark listener requires zero job starts. |

The synthetic comparison measures these fixtures and their tonic/JVM shaded-gRPC
stacks. Their analysis responses select a canned schema; they do not resolve the
request plan. It does not measure the complete production Rust/JVM frontends.
The planning comparison uses the production paths, including Catalyst through JNI
for the Rust target. It sends only safe `AnalyzePlan`, `Config`, and session-release
calls. Local Arrow decoding and planning still consume CPU and memory. Spark task
execution and worker-to-driver result production are outside both profiles.

## Coverage and workload dimensions

Untimed correctness/capability probes touch all 12 Spark Connect RPC routes in the
synthetic profile: ExecutePlan, AnalyzePlan, Config, AddArtifacts, ArtifactStatus,
Interrupt, ReattachExecute, ReleaseExecute, ReleaseSession, FetchErrorDetails,
CloneSession, and GetStatus. They exercise 14 AnalyzePlan variants and all seven
Config variants, stream schema/batch/completion framing, reattachment cursors,
release/interrupt selectors, session cloning and isolation, structured errors,
artifact upload forms, and per-chunk CRC rejection. Unsupported variants and
deliberate error paths have explicit outcomes. Planning probes list excluded
routes and persistence-changing analysis variants separately.

The Rust client is pinned to fork revision
`87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce`, using the Spark 4.3 protocol.
It includes upstream array-literal fix SPARK-61119,
[`70688ec07ff87ea5cd4d685247074feeaa2c2b7f`](https://github.com/apache/spark-connect-rust/commit/70688ec07ff87ea5cd4d685247074feeaa2c2b7f),
and Arrow map-import normalization from the
[complete fork diff](https://github.com/dbtsai/spark-connect-rust/compare/925dc876f63f1f779fe176f62575a0aa96cc6886...87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce).
The map fix is proposed upstream in
[PR #396](https://github.com/apache/spark-connect-rust/pull/396)
([SPARK-61212](https://issues.apache.org/jira/browse/SPARK-61212)) and is not merged.
The local Spark assembly is 4.2. Its synthetic JVM fixture adds the pinned
GetStatus schema using Spark's shaded dynamic protobuf marshalling and gRPC
descriptors, reusing the fixture's session and operation state. Fixture statistics
identify the descriptor source. Both fixtures validate selector presence,
filtering, unknown IDs, deduplication, operation states, user/session isolation,
and session identity.
See [GetStatus fixture contract and comparison limits](GETSTATUS_CONTRACT.md)
for the pinned Spark source comparison, intentional lifecycle differences, and
prost versus DynamicMessage timing annotations.

The planning profile excludes GetStatus on the real Spark 4.2 server; it does not
probe that route. Coverage of every route does not imply conformance for every
protobuf field, relation, expression, command, extension, UDF, catalog operation,
or data type.

The corpus includes ranges, SQL, projection/filter chains, wide projections,
balanced joins, unions, aggregates, windows/sorts, literals, and local Arrow
relations. Six Arrow families cover primitives, Unicode strings/binary,
decimals/dates/timestamps, structs/lists, maps, and null-heavy values. Per-case
metadata records plan nodes/depth, encoded size, data dimensions, and type/variant
inventories.

Configuration probes follow Spark's `GetAll(prefix)` behavior: returned names
have the requested prefix removed, while an omitted or empty prefix preserves
full names. Both fixtures and the JNI Spark engine apply that behavior, and
regression checks cover filtered and unfiltered responses.

The typed-literal projection uses unmodified public client output. The upstream
array fix preserves the declared type in `Array.element_type`; the root
`Literal.data_type` can remain unset. Case metadata records
`details.literal_data_type_fields: "official_client_unmodified"`. The benchmark
no longer adds root type annotations.

Timed Arrow fixtures are serialized directly by the harness and explicitly use
the `entries`, `key`, and `value` map child names required by Arrow Java. The
fork-pinned client normalizes default Arrow map names. Its client-side unit and
live tests cover nested maps, shared input buffers, and nonempty inline/chunked
imports; this benchmark audit checks only a zero-row top-level map.
`api-audit` independently imports a zero-row default `MapBuilder` batch through
`SparkSession::create_dataframe_from_arrow`, calls `to_relation()`, and inspects
the resulting IPC schema. Default Arrow builder names alone are input evidence;
the audit judges the client's emitted names.

The audit also compares exact declared element types for populated, empty, and
nested array literals. Resolved contracts and active client gaps are recorded
separately, and generated-binding route coverage remains distinct from high-level
session API parity. See [Rust API gaps](RUST_API_GAPS.md) for scope and evidence.
Exported manifests retain these observations under `rust_api_audit`. Export a
fresh corpus after client or generator changes: typed-literal bytes and audit
evidence belong to the frozen dependency revision.

| Corpus | Plans | Example dimensions |
| --- | ---: | --- |
| `smoke` | 23 | Chain depth 4, width 8, four join/union leaves, 0/4 KiB local-data targets, 128-byte string/binary literals. |
| `standard` | 48 | Chains 1/8/16, widths 1/32/128, joins 2/8, unions 2/8/16, local-data targets through 1 MiB, literals through 1 MiB. |
| `stress` | 60 | Chains through 20, width 1,024, joins 32, unions 20, local-data/literal targets through 16 MiB. |

Size targets describe the generator input, not exact Arrow IPC message size;
recorded encoded lengths give the actual sizes. Fixture replay limits each IPC
payload to 8 MiB and all preloaded payloads to 64 MiB. Larger request cases remain
in the planning corpus; the manifest explains each response-payload exclusion.
The smoke export contains 13 replay payloads. Streams use one or eight batches,
with the larger payloads limited to one batch. Artifact workloads vary upload
size and batched/chunked framing.

Session and channel sharing vary independently. `shared` sessions put concurrent
workers in one session; `isolated` gives each worker a separate session. Shared
channels multiplex HTTP/2 streams; isolated channels establish one transport per
worker. Session-release workloads use a prepared session per request because a
session cannot be repeatedly released as independent live work.

## Timing and reporting

Workers use a closed loop with a bounded number of in-flight RPCs. `--requests`
is the total measured requests in each round and must cover the largest
concurrency. Each cell has fixed warmups and repetitions; warmup count is at least
the worker count. There are no application retries.

Corpus generation, plan construction, channel/session setup, and lifecycle
dependencies are prepared before the round barrier. Primary latency includes
client protobuf encoding, RPC transfer, response decoding, and full stream drain.
Throughput uses the primary round's wall time. Response assertions and lifecycle
cleanup run after all workers finish, outside that window; their time and errors
are recorded separately. Completed-round throughput additionally includes request
and lifecycle preparation plus validation/cleanup. It still excludes server
startup, channel/session establishment, and warmups. Neither rate is labeled
sustained capacity. First-message latency can refer to a schema header;
first-Arrow latency separately measures arrival of the first data batch.
Response byte counts are protobuf encoded
lengths, excluding HTTP/2/TLS framing.

Prepared requests and retained responses have a default estimated 256 MiB
per-cell budget. `--max-prepared-mib` changes it; excluded cells retain their
resource-budget reason. The fixtures also bound retained operations and artifacts
to 4,096 per session; rounds whose prepared state would exceed those limits are
explicitly skipped with that reason. This budget is not a bound on total JVM or
client RSS.

The Rust runner launches one target at a time with fresh server processes,
alternating forward/reverse target order between forks. It freezes and hashes the
harness executable, load generator, Java jar, native library when required, and
exported corpus.
It retains client/server logs, commands, versions, host load, process snapshots,
listener ownership, shutdown evidence, fixture execution counters, and the
planning profile's zero-Spark-jobs audit.

The frozen executable at `artifacts/harness` contains the runner and reporter.
Rust manifests set `metadata.orchestration` to `rust`, and their `runner_sha256`
and `reporter_sha256` both identify those executable bytes. Reports retain the
actual reporting executable's hash in `analysis_reporter_sha256`. Use the frozen
copy to regenerate a report with the original producer; the comparator requires
that identity to match. The Rust reporter can also read legacy Python manifests
and validate their original script hashes.

The reporter writes `REPORT.md`, `summary.json`, `cells.csv`, and
`comparisons.csv`. Accepted forks include median/p95/p99, first-message and
first-Arrow latency, request rate, response MiB/s, bytes, and client CPU for
successful cells. Unsupported and skipped cells remain explicit. The reporter
checks that the requested cell grid is complete, counting measured and explicitly
skipped cells, and rejects duplicated cells or settings that differ from the run
manifest. Comparisons match the same profile, case, sharing models, concurrency,
repetition identities, and counts within a
fresh-fork pair. Ratios are summarized across forks; individual RPC samples are
not treated as independent server replications. A smoke run checks correctness
and wiring. More forks, a quiet host, and client-saturation checks are needed to
interpret performance, and this closed-loop suite does not measure open-loop
arrival-rate SLOs.

A failed or incomplete client, failed coverage, invalid counters, or missing
shutdown/no-execution evidence rejects the entire fork from summarized
measurements and comparisons. Available raw per-fork `client.json`, `client.log`,
`server.log`, and `fork.json` remain in the run directory; report validation
errors identify the raw client JSON for diagnosis. Rejected forks' individual
samples are read from those raw files rather than copied into successful
`summary.json` groups. Within accepted forks, excluded unsupported/failing cell
counts and statuses remain under `non_success_cells`, and explicit budget
exclusions remain under `skipped_cells`. The reporter still writes its output
files for an invalid run and exits with a nonzero status. A startup failure may
precede client JSON creation; its server log and fork record provide the
diagnostic evidence.

## Memory and measurement limits

In the tested Spark assembly, driver-side local Arrow analysis can retain native
resources when no task-completion callback closes its reader. Repeated large
inputs have exhausted memory through both real planning frontends. Start with a
60 MiB prepared-cell budget and retain every explicit skip. This budget does not
fix the Spark reader lifecycle or bound total JVM native memory.

Identical-binary comparisons can also show substantial variation. The
[RSI workflow](RSI.md) checks measurement stability before evaluating candidate
performance and retains failed controls. No performance improvement is assumed.

## Build and run

Run commands from the repository root. Use Rust 1.88 or newer and `protoc`.
`suite` and `compare` process-group orchestration support macOS and Linux.
JVM targets additionally require JDK 21 and a matching Spark 4.2 assembly
containing the shaded Connect gRPC jars. The JNI engine uses Spark internal APIs
pinned to the assembly documented in the main README; other versions may require
changes. A `--targets rust-fixture` suite needs neither JDK nor Spark.

```sh
export JAVA_HOME=/path/to/jdk-21
export SPARK_HOME=/path/to/spark-4.2-assembly
cargo build --locked --release --manifest-path benchmarks/protocol/Cargo.toml --target-dir target
jvm/build.sh
cargo build --locked --release --manifest-path jni/Cargo.toml --target-dir target
```

Use `libspark_connect_jni.so` on Linux in place of the macOS
`.dylib` below. Supply a native library and jar from the same build.

Start with route/lifecycle validation for all four targets:

```sh
target/release/spark-connect-protocol-bench suite --output /absolute/new-protocol-check \
  --profile smoke --coverage-only --forks 1 \
  --spark-home "$SPARK_HOME" --java-home "$JAVA_HOME" \
  --library "$PWD/target/release/libspark_connect_jni.dylib"
/absolute/new-protocol-check/artifacts/harness report --input /absolute/new-protocol-check
```

A small timed smoke run exercises the full corpus and all sharing combinations:

```sh
target/release/spark-connect-protocol-bench suite --output /absolute/new-protocol-smoke \
  --profile smoke --forks 1 --concurrency 1,4 --requests 8 --warmups 4 --repetitions 1 \
  --max-prepared-mib 60 \
  --spark-home "$SPARK_HOME" --java-home "$JAVA_HOME" \
  --library "$PWD/target/release/libspark_connect_jni.dylib"
/absolute/new-protocol-smoke/artifacts/harness report --input /absolute/new-protocol-smoke
```

For a larger comparison:

```sh
target/release/spark-connect-protocol-bench suite --output /absolute/new-protocol-standard \
  --profile standard --forks 4 --concurrency 1,8,32 --requests 64 --warmups 16 --repetitions 3 \
  --max-prepared-mib 60 \
  --spark-home "$SPARK_HOME" --java-home "$JAVA_HOME" \
  --library "$PWD/target/release/libspark_connect_jni.dylib"
/absolute/new-protocol-standard/artifacts/harness report --input /absolute/new-protocol-standard
```

`--targets rust-fixture,jvm-fixture` runs just the synthetic comparison;
`--targets rust-jvm-planning,jvm-planning` runs just real planning.
`--case SUBSTRING` filters timed cases while preserving protocol probes.
`--payload-dir DIRECTORY` reuses an exported corpus, copied into the new result
directory. The output directory must be new, and all servers bind to loopback.

The binary also supports `export`, `serve-fixture`, `run`, and `codec` for manual
experiments. `codec` records local protobuf encode/decode costs without a server;
it is a separate measurement and is never subtracted from RPC timings. Audit the
unmodified public API output without starting a server:

```sh
target/release/spark-connect-protocol-bench api-audit --strict --output results/rust-api-audit.json
```

`--strict` makes an observed client gap a nonzero exit status. It is a regression
gate for the fixed array-element and public Arrow map-import contracts. Omitting
the flag still writes observed gaps without rejecting the command.

Rust runner, reporter, comparator, and API-audit regression tests run without
starting Spark or a JVM:

```sh
cargo test --locked --manifest-path benchmarks/protocol/Cargo.toml --target-dir target --lib
```

## Compare native engine builds with RSI

`compare` runs the production `rust-jvm-planning` target with two saved jar and
native-library pairs. Both variants keep that target name. The Rust coordinator
copies the harness executable, load generator, corpus, policy, and artifacts;
checks their hashes before and after each child; and alternates fresh JVMs in
AB/BA order. It launches the frozen executable's `suite` and `report` commands
and validates each summary's reporting identity. Save both builds outside the
build output directory before running.

```sh
target/release/spark-connect-protocol-bench compare --output /absolute/new-engine-comparison \
  --baseline-jar /absolute/baseline/bridge.jar \
  --baseline-library /absolute/baseline/libspark_connect_jni.dylib \
  --candidate-jar /absolute/candidate/bridge.jar \
  --candidate-library /absolute/candidate/libspark_connect_jni.dylib \
  --payload-dir /absolute/frozen-smoke-corpus --policy /absolute/policy.json \
  --profile smoke --concurrency 1,8,32 --requests 1024 --warmups 512 \
  --repetitions 3 --pairs 3 --start-order AB \
  --spark-home "$SPARK_HOME" --java-home "$JAVA_HOME"
```

First use `--control --pairs 2` with identical artifacts for both variants. A
failed control prevents a performance claim. Freeze the policy before candidate
measurement; its optional `matrix` must match the command, including case filters.
The gate requires declared target improvements and checks every measured cell's
latency, throughput, completed-round throughput, and client CPU per request.
Sharing topologies cannot inflate the count of distinct improved targets.

`COMPARE.md` and `compare.json` retain each fresh-fork pair and failed child.
Ratios use geometric means across paired forks. Passing this exploratory gate
still requires an independent run starting in the opposite order, broader
sharing/corpus holdouts, and executed-query, cancellation, and JNI ownership
validation. Keep builds, profiling, and unrelated heavy work outside timing.
