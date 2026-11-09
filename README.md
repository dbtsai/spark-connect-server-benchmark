# Spark Connect server benchmarks

Concurrency and protocol benchmarks driven directly by the official Apache Spark
Connect Rust client. Compare upstream JVM Spark Connect with a Rust frontend that
calls Spark Catalyst and execution APIs through JNI. The Rust frontend also has a
DataFusion backend for standalone experiments.

The repository includes the experimental server and JNI/Java adapters needed to
build the benchmark targets. Generated results and binaries are produced locally.
This is an independent experiment, not an Apache Spark distribution.

| Suite | What it measures |
| --- | --- |
| [Protocol and planning](benchmarks/protocol/README.md) | Execution-free protocol fixtures and real Spark analysis/configuration, with varied plans, types, sizes, concurrency, and sharing. |
| [Executed queries](benchmarks/README.md) | End-to-end JVM gRPC versus Rust/JNI metadata, queries, Arrow streaming, and concurrent configuration calls. |
| [RSI comparisons](benchmarks/protocol/RSI.md) | Frozen baseline/candidate builds, same-binary noise controls, alternating fresh JVMs, per-case guards, independent confirmation, and holdouts. |

## Protocol and planning coverage

The synthetic fixtures exercise all 12 RPC routes in the pinned protocol,
14 analysis variants, all seven configuration variants, artifacts, structured
errors, interruption, release, reattachment, and session isolation. They replay
prebuilt results and never evaluate plans. The real planning profile analyzes
plans and accesses configuration through the production paths; a Spark listener
requires zero job starts. Route coverage does not imply support for every protobuf
field or Spark operation.

Smoke, standard, and stress corpora contain 23, 48, and 60 plans. Shapes include
chains, wide projections, balanced joins, unions, aggregates, windows/sorts,
literals, and local Arrow relations. Six Arrow families cover primitives,
Unicode strings/binary, decimals/temporal values, nested values, maps, and nulls.
Concurrency, session sharing, and HTTP/2 channel sharing vary independently.

## Requirements

- Rust 1.88 or newer, Cargo, and `protoc`.
- Python 3.9 or newer for orchestration/reporting; protocol benchmarks use only
  the Python standard library and the Rust load generator.
- For JVM targets: JDK 21, a C compiler with JNI headers, and a matching Spark 4.2
  assembly with shaded Connect gRPC jars. Listener ownership inspection requires
  `lsof`; the protocol runner also supports Linux `/proc` as a fallback.
- Executed-query benchmarks and Python acceptance additionally need a compatible
  official PySpark Connect client, gRPC, and PyArrow.

The official Rust client/protocol is pinned to
[`9ed82b874357cdff887977864026d98f8b00f434`](https://github.com/apache/spark-connect-rust/commit/9ed82b874357cdff887977864026d98f8b00f434)
(Spark 4.3 protocol). The JVM adapters were developed against Spark
4.2.0-SNAPSHOT assembly revision `250ccca3f2b5f043e20b415adf17bda5d202c60f`,
Scala 2.13.17, and JDK 21.0.10. They use Spark internal APIs; another assembly may
require source changes. Spark 4.2 lacks GetStatus, which is explicitly recorded
as unsupported rather than counted as successful throughput.

## Build and run

Run commands from the repository root. A Rust-only fixture smoke run needs no JVM:

```sh
cargo build --locked --release --manifest-path benchmarks/protocol/Cargo.toml --target-dir target
python3 benchmarks/protocol/run.py --output results/rust-fixture-smoke \
  --targets rust-fixture --profile smoke --forks 1 \
  --concurrency 1,4 --requests 8 --warmups 4 --repetitions 1
python3 benchmarks/protocol/report.py results/rust-fixture-smoke
```

Build the JVM targets and Rust server adapter:

```sh
export JAVA_HOME=/path/to/jdk-21
export SPARK_HOME=/path/to/matching-spark-assembly
jvm/build.sh --tests
cargo build --locked --release --manifest-path jni/Cargo.toml --target-dir target
```

Then exercise all four targets, including the real Spark planning paths:

```sh
python3 benchmarks/protocol/run.py --output results/protocol-smoke \
  --profile smoke --forks 1 --concurrency 1,4 \
  --requests 8 --warmups 4 --repetitions 1 --max-prepared-mib 60
python3 benchmarks/protocol/report.py results/protocol-smoke
```

The native filename defaults to `libspark_connect_jni.dylib` on macOS and
`libspark_connect_jni.so` on Linux. `SPARK_HOME`/`JAVA_HOME` can also be supplied
with `--spark-home`/`--java-home`. Use a new output directory for every run.

## Interpretation and limits

The load generator uses bounded closed-loop concurrency. Plan construction and
validation are outside the primary timer; completed-round throughput also
includes preparation and cleanup. Reports preserve failed/unsupported/skipped
cases, raw logs, artifact hashes, host context, listener identity, and shutdown
evidence. Individual RPCs are not independent fresh-server samples.

These are local experimental servers with no ingress TLS/authentication. Bind to
trusted loopback endpoints. The native engine implements a subset of Spark; the
synthetic fixtures are protocol measurement tools rather than Spark engines.
JNI result buffers use Arrow C Data sharing, while gRPC still serializes Arrow
IPC and protobuf responses.

Repeated large local Arrow analysis can retain native resources in the tested
Spark assembly's driver-side Arrow reader. This affects both real planning
frontends. The 60 MiB prepared-cell budget skips large repeated payload cases but
does not fix the reader lifecycle or bound total JVM native memory. Start with
smoke and inspect skips/failures before increasing sizes.

A same-binary control must pass before interpreting candidate speedups. Local
controls have shown substantial variation even with longer sampling windows; this
repository does not claim a verified Rust/JVM performance improvement.

## Validation

```sh
python3 -m unittest discover -s benchmarks/protocol/tests
python3 -m unittest discover -s jvm -p 'test_*.py'
cargo test --locked --manifest-path benchmarks/protocol/Cargo.toml --target-dir target
cargo test --locked
cargo test --locked --manifest-path jni/Cargo.toml --target-dir target
cargo fmt --check
cargo fmt --manifest-path benchmarks/protocol/Cargo.toml --check
cargo fmt --manifest-path jni/Cargo.toml --check
JAVA_HOME="$JAVA_HOME" jni/test-c-export.sh --sanitize
```

JNI tests require `JAVA_HOME`. Real Spark acceptance and benchmark runs require
the assembly described above; Python acceptance needs its client dependencies.
See [executed-query instructions](benchmarks/README.md) for JNI checking and
cancellation/result validation.
