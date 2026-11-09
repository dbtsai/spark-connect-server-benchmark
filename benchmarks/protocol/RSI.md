# Benchmark-driven optimization

Use the same frozen benchmark to decide whether to keep one product change.
Passing a local performance gate is exploratory evidence; it also needs independent
correctness checks, confirmation, and broader coverage.

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
   order. Run [sharing](policies/sharing.json), [bounded standard](policies/standard.json),
   and executed-query/cancellation/Arrow holdouts before promotion.
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
python3 benchmarks/protocol/compare.py --output results/noise-control \
  --baseline-jar results/baseline/bridge.jar \
  --baseline-library results/baseline/libspark_connect_jni.dylib \
  --candidate-jar results/baseline/bridge.jar \
  --candidate-library results/baseline/libspark_connect_jni.dylib \
  --policy benchmarks/protocol/policies/planning.json \
  --requests 1024 --warmups 512 --repetitions 3 --pairs 2 --control
```

Both variant labels must have identical jar and native-library hashes. The
wrapper retains the `rust-jvm-planning` target name for both; it does not label a
candidate as the upstream JVM server. It exports and freezes a corpus, or accepts
`--payload-dir` to reuse an existing export.

## Candidate and confirmation

After a passing control, build and validate one candidate and save its matching
jar/library under `results/candidate/`. Run:

```sh
python3 benchmarks/protocol/compare.py --output results/screen \
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
