# Benchmark-driven optimization

Use the same frozen Rust benchmark to decide whether to keep one product change.
Passing a local performance gate is exploratory evidence; it also needs independent
correctness checks, confirmation, and broader coverage.

The `spark-connect-protocol-bench` executable owns `suite`, `report`, `compare`,
and `api-audit`; its public Rust library exposes the same implementation. Corpus
plans originate from the pinned Rust client's public APIs, with generated
protobuf/gRPC bindings used for route probes. Run `api-audit` to inspect unmodified
API output and keep active client gaps separate from resolved serialization
contracts.
The pin `87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce` includes
upstream array-literal fix SPARK-61119,
[`70688ec07ff87ea5cd4d685247074feeaa2c2b7f`](https://github.com/apache/spark-connect-rust/commit/70688ec07ff87ea5cd4d685247074feeaa2c2b7f),
and Arrow map-import normalization from the
[complete fork diff](https://github.com/dbtsai/spark-connect-rust/compare/925dc876f63f1f779fe176f62575a0aa96cc6886...87b2b1e817e7384ed63e8b2385f4b5e4a51ad7ce).
The map fix is proposed upstream in
[PR #396](https://github.com/apache/spark-connect-rust/pull/396)
([SPARK-61212](https://issues.apache.org/jira/browse/SPARK-61212)) and is not merged.
Typed literals use unmodified client output; map-import evidence comes from the
public session API's serialized IPC schema. Audit-only probes preserve timed
case counts and the 12-route inventory.
See [Rust API gaps](RUST_API_GAPS.md) for evidence and scope. This execution-free
workflow requires no Python; the optional [executed-query profile](../README.md)
and Python acceptance checks remain independent holdouts.

1. Save baseline source, dependency locks, build command/toolchain, artifact
   SHA256 values, and host context. Copy its jar and native library outside build
   output paths. Build the candidate with identical settings.
2. Freeze the harness, corpus, and acceptance policy before editing product code.
   The supplied [planning policy](policies/planning.json) declares ten target
   case/concurrency combinations and guards every measured cell. Its matrix must
   match the CLI; a case filter cannot silently narrow it.
3. Run an identical-binary noise control. If the frozen guards fail, preserve the
   run and investigate measurement stability. Do not claim an improvement or
   relax thresholds to accept the result. Any sampling/harness revision is a
   separate, recorded experiment with a new control.
4. Make one product change. Validate protocol/schema semantics, JNI ownership,
   cancellation, and session sharing independently of timing.
5. Compare baseline/candidate in alternating fresh JVMs. Stop builds, profiling,
   and unrelated heavy work during timing. Keep failed samples and raw logs.
6. Evaluate each case, including primary and completed-round rates. Count distinct
   case/concurrency targets; sharing topologies cannot inflate that count.
7. Repeat a promising result in a new output directory starting in the opposite
   order. Run [sharing](policies/sharing.json),
   [bounded standard](policies/standard.json), and
   executed-query/cancellation/Arrow holdouts before promotion.
8. Keep an experiment ledger with the hypothesis, exact patch, commands,
   correctness results, all timing samples, and keep/revert decision. Declare a
   finite experiment budget. An inconclusive result remains inconclusive.

## Same-binary control

After building the protocol binary and JNI jar/library as described in the root
README, save the baseline artifacts:

```sh
mkdir -p results/baseline
cp .build/jvm/spark-connect-native-bridge.jar results/baseline/bridge.jar
cp target/release/libspark_connect_jni.dylib results/baseline/
```

Use `.so` on Linux. Set `SPARK_HOME` and `JAVA_HOME`, then run:

```sh
target/release/spark-connect-protocol-bench compare --output results/noise-control \
  --baseline-jar results/baseline/bridge.jar \
  --baseline-library results/baseline/libspark_connect_jni.dylib \
  --candidate-jar results/baseline/bridge.jar \
  --candidate-library results/baseline/libspark_connect_jni.dylib \
  --policy benchmarks/protocol/policies/planning.json \
  --requests 1024 --warmups 512 --repetitions 3 --pairs 2 --control
```

Both variant labels must have identical jar and native-library hashes. The
coordinator retains the `rust-jvm-planning` target name for both; it does not label
a candidate as the upstream JVM server. It exports and freezes a corpus, or
accepts `--payload-dir` to reuse an existing export.

The coordinator freezes the Rust harness and load generator along with server
artifacts, policy, and corpus, and verifies source/snapshot hashes before and
after each child. It invokes the frozen executable for both `suite` and `report`.
Each child summary must identify those reporter bytes in
`analysis_reporter_sha256`; a summary from another reporter invalidates the
comparison. Individual suite output also retains `artifacts/harness` for
reproducible standalone reporting:

```sh
results/example-suite/artifacts/harness report --input results/example-suite
target/release/spark-connect-protocol-bench api-audit --strict --output results/rust-api-audit.json
cargo test --locked --manifest-path benchmarks/protocol/Cargo.toml --target-dir target --lib
```

The tests exercise orchestration, reporting, comparisons, and API contracts
without Spark servers. `api-audit --strict` rejects observed regressions in the
array-element or public Arrow map-import contracts; it is separate from benchmark
validity. Regenerate the frozen corpus and rerun controls after changing the
client revision. Historical results retain their original dependency and corpus
hashes.

## Candidate and confirmation

After a passing control, build and validate one candidate and save its matching
jar/library under `results/candidate/`. Run:

```sh
target/release/spark-connect-protocol-bench compare --output results/screen \
  --baseline-jar results/baseline/bridge.jar \
  --baseline-library results/baseline/libspark_connect_jni.dylib \
  --candidate-jar results/candidate/bridge.jar \
  --candidate-library results/candidate/libspark_connect_jni.dylib \
  --policy benchmarks/protocol/policies/planning.json \
  --requests 1024 --warmups 512 --repetitions 3 --pairs 3 --start-order AB
```

For confirmation, repeat into a new directory with `--start-order BA` and the
same frozen inputs. `compare.json` retains individual fresh-fork ratios;
`COMPARE.md` summarizes their geometric means. A failed child invalidates the
comparison and retains its log. A guard failure rejects the gate even if declared
targets improve. Exit status is zero for a valid passing gate (or a valid
descriptive run without a policy), and nonzero for rejected or invalid runs.

For holdouts, match each supplied policy's full matrix explicitly. The sharing
policy uses smoke, 64 requests, 32 warmups, one repetition, and all four
session/channel combinations. The standard policy uses standard, 64 requests,
32 warmups, one repetition, and shared sessions/channels. Both use concurrency
1/8/32 and a 60 MiB prepared-cell budget. Preserve all unsupported/skipped cases.

The original local same-binary controls failed stability guards, including a
larger-window control. This import includes no accepted optimization or claimed
speedup. Stabilize the control on the measurement host before drawing conclusions
from a new candidate.
