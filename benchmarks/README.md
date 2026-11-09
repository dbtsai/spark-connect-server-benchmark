# Executed-query comparison

`run.py` launches upstream JVM Spark Connect and the embedded Rust/JNI frontend
one at a time in alternating fresh-process pairs. The same official PySpark client
workloads measure configuration, analysis, small SQL, range/filter/aggregate,
join/aggregate, Arrow streaming, and concurrent configuration. These workloads
execute Spark jobs; use the [protocol suite](protocol/README.md) for execution-free
measurement.

Build the JNI native library and Java jar as described in the root README. Use a
Python environment with an official PySpark Connect client compatible with the
Spark assembly, gRPC, and PyArrow. A `pyspark-client` installation supplies the
client; confirm the resulting dependency versions match your server.

```sh
export JAVA_HOME=/path/to/jdk-21
export SPARK_HOME=/path/to/matching-spark-assembly
python3 benchmarks/run.py --output results/executed-queries --pairs 4 \
  --native-kind engine --python /absolute/path/to/client-python \
  --allow-completed-release-absence
python3 benchmarks/report.py results/executed-queries
```

The native engine does not retain already-completed operations, so the client
compatibility flag allows that explicit lifecycle difference. It does not skip
validation of returned values or active-operation cleanup.

For acceptance only, add `--xcheck-jni --engine-acceptance` and optionally
`--rust-example "$PWD/target/debug/examples/rust_client"` after building the Rust
example. JNI checking is excluded from performance runs. Keep acceptance outputs
separate from timing outputs.

The typed Spark engine passes relation plans and engine operations across JNI;
Rust owns Spark Connect decoding, session/operation tracking, and responses.
`--native-kind bridge` selects the older adapter that invokes Spark's RPC handlers
directly inside the JVM. It measures a different architecture and must be labeled
separately.

Both modes use internal Spark APIs pinned to the assembly in the root README.
Run on a quiet host, retain raw failures and artifact hashes, and interpret paired
fresh-process ratios rather than treating individual requests as independent
server replications. Optional `report.py --plot` needs Matplotlib.

The optional runner's option guards use only Python's standard library and do
not start Spark:

```sh
python3 -m unittest discover -s benchmarks/tests -p 'test_*.py'
```
