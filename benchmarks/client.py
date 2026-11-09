"""Identical official-PySpark workloads for JVM gRPC and the embedded Rust/JNI listener."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import os
from pathlib import Path
import platform
import statistics
import threading
import time
import traceback
import uuid


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", default=os.environ.get("SPARK_REMOTE"), required="SPARK_REMOTE" not in os.environ)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fork-id", default="0")
    parser.add_argument("--pair-id", default="0")
    parser.add_argument("--order-index", type=int, default=0)
    parser.add_argument("--metadata-warmups", type=int, default=200)
    parser.add_argument("--metadata-samples", type=int, default=500)
    parser.add_argument("--query-warmups", type=int, default=10)
    parser.add_argument("--query-samples", type=int, default=30)
    parser.add_argument("--arrow-warmups", type=int, default=3)
    parser.add_argument("--arrow-samples", type=int, default=6)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--samples", type=int)
    parser.add_argument("--range-rows", type=int, default=1_000_000)
    parser.add_argument("--join-rows", type=int, default=100_000)
    parser.add_argument("--stream-rows", type=int, default=4_000_000)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--config-calls", type=int, default=1000)
    parser.add_argument("--concurrency-warmups", type=int, default=1)
    parser.add_argument("--concurrency-samples", type=int, default=3)
    parser.add_argument("--allow-completed-release-absence", action="store_true",
                        help="allow non-retaining frontend to omit an already-completed operation ID")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--workloads", default="config_get,analyze_range_schema,tiny_sql,range_filter_aggregate,join_aggregate,arrow_stream,config_concurrent")
    args = parser.parse_args()
    if args.warmups is not None:
        args.metadata_warmups = args.query_warmups = args.arrow_warmups = args.concurrency_warmups = args.warmups
    if args.samples is not None:
        args.metadata_samples = args.query_samples = args.arrow_samples = args.concurrency_samples = args.samples
    for key in ("metadata_warmups", "query_warmups", "arrow_warmups", "concurrency_warmups"):
        if getattr(args, key) < 0:
            parser.error(key + " must be nonnegative")
    for key in ("metadata_samples", "query_samples", "arrow_samples", "concurrency_samples", "range_rows", "join_rows", "stream_rows", "concurrency", "config_calls"):
        if getattr(args, key) <= 0:
            parser.error(key + " must be positive")
    return args


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def percentile(values, fraction):
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = int(index)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def main():
    args = parse_args()
    import grpc
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyspark
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.connect.proto import base_pb2 as base
    from pyspark.sql.connect.proto import base_pb2_grpc as rpc
    from pyspark.sql.connect.proto import relations_pb2 as relations

    identity = {"mode": args.mode, "fork_id": args.fork_id, "pair_id": args.pair_id,
                "order_index": args.order_index}
    settings = {key: value for key, value in vars(args).items() if key not in {"output", "remote", *identity}}
    settings.update({"raw_execute_reattachable": True, "release_all_outside_timing": True,
                     "validation_outside_timing": True, "partitions": 2,
                     "concurrent_client_channels": args.concurrency})
    report = {"schema_version": 1, **identity, "remote": args.remote, "settings": settings,
              "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "client_versions": {"python": platform.python_version(), "pyspark": pyspark.__version__,
                                  "grpc": grpc.__version__, "pyarrow": pa.__version__},
              "samples": [], "workload_summaries": [], "cleanup": []}
    address = args.remote.removeprefix("sc://").split("/", 1)[0]
    channels = []
    spark = None
    raw_session = str(uuid.uuid4())
    user = base.UserContext(user_id="spark-connect-benchmark")
    raw_ready = False
    pool = None

    def channel():
        value = grpc.insecure_channel(address, options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)])
        channels.append(value)
        return value

    def timed(name, phase, iteration, run, validate, cleanup=None, context=None):
        record = {**identity, "workload": name, "phase": phase, "iteration": iteration,
                  "first_batch_ns": None, "rows": None, "arrow_ipc_bytes": None,
                  "response_count": None, "passed": False}
        result = None
        start = time.perf_counter_ns()
        cpu = time.process_time_ns()
        try:
            result = run(context, start)
        except Exception as error:
            record.update({"error": repr(error), "traceback": traceback.format_exc()})
        finally:
            record["elapsed_ns"] = time.perf_counter_ns() - start
            record["client_process_cpu_ns"] = time.process_time_ns() - cpu
        if "error" not in record:
            try:
                record.update(validate(result, context))
                record["passed"] = True
            except Exception as error:
                record.update({"error": repr(error), "traceback": traceback.format_exc()})
        if cleanup is not None:
            try:
                cleanup(context, result)
            except Exception as error:
                record.update({"cleanup_error": repr(error), "passed": False})
        report["samples"].append(record)
        return record

    def repeated(name, warmups, samples, run, validate, setup=lambda: None, cleanup=None):
        for phase, count in (("warmup", warmups), ("measured", samples)):
            for iteration in range(count):
                context = setup()
                timed(name, phase, iteration, run, validate, cleanup, context)
        print(json.dumps({"workload": name, "measured": samples,
                          "passed": all(s["passed"] for s in report["samples"] if s["workload"] == name)}), flush=True)

    try:
        stub = rpc.SparkConnectServiceStub(channel())
        initial = stub.Config(base.ConfigRequest(session_id=raw_session, user_context=user,
            operation=base.ConfigRequest.Operation(set=base.ConfigRequest.Set(
                pairs=[base.KeyValue(key="spark.sql.session.timeZone", value="UTC")]))), timeout=args.timeout)
        raw_ready = True
        server_id = initial.server_side_session_id
        fields = {"session_id": raw_session, "user_context": user,
                  "client_observed_server_side_session_id": server_id}
        config_request = base.ConfigRequest(**fields, operation=base.ConfigRequest.Operation(
            get=base.ConfigRequest.Get(keys=["spark.sql.session.timeZone"])))
        analyze_request = base.AnalyzePlanRequest(**fields, schema=base.AnalyzePlanRequest.Schema(
            plan=base.Plan(root=relations.Relation(range=relations.Range(
                start=0, end=args.range_rows, step=1, num_partitions=2)))))
        chosen = args.workloads.split(",")
        known = {"config_get", "analyze_range_schema", "tiny_sql", "range_filter_aggregate",
                 "join_aggregate", "arrow_stream", "config_concurrent"}
        require(set(chosen) <= known, "unknown workload: " + str(set(chosen) - known))

        def config_valid(response, context=None):
            pairs = [(pair.key, pair.value) for pair in response.pairs]
            require(pairs == [("spark.sql.session.timeZone", "UTC")], "incorrect Config result: " + str(pairs))
            require(response.session_id == raw_session and response.server_side_session_id == server_id,
                    "Config session identity changed")
            return {"response_count": 1, "protobuf_bytes": response.ByteSize(), "validation": {"time_zone": "UTC"}}

        if "config_get" in chosen:
            repeated("config_get", args.metadata_warmups, args.metadata_samples,
                     lambda _, start: stub.Config(config_request, timeout=args.timeout), config_valid)
        if "analyze_range_schema" in chosen:
            def analyze_valid(response, context):
                fields_out = response.schema.schema.struct.fields
                require(len(fields_out) == 1 and fields_out[0].name == "id"
                        and fields_out[0].data_type.WhichOneof("kind") == "long"
                        and not fields_out[0].nullable, "incorrect range schema")
                return {"response_count": 1, "protobuf_bytes": response.ByteSize(),
                        "validation": {"field": "id", "type": "long", "nullable": False}}
            repeated("analyze_range_schema", args.metadata_warmups, args.metadata_samples,
                     lambda _, start: stub.AnalyzePlan(analyze_request, timeout=args.timeout), analyze_valid)

        if any(name in chosen for name in ("tiny_sql", "range_filter_aggregate", "join_aggregate")):
            spark = SparkSession.builder.remote(args.remote).create()
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.conf.set("spark.sql.shuffle.partitions", "2")
        if "tiny_sql" in chosen:
            def tiny_valid(rows, context):
                require(len(rows) == 1 and rows[0][0] == 2, "incorrect tiny SQL result")
                return {"rows": 1, "validation": {"answer": 2}}
            repeated("tiny_sql", args.query_warmups, args.query_samples,
                     lambda _, start: spark.sql("SELECT 1 + 1 AS answer").collect(), tiny_valid)
        if "range_filter_aggregate" in chosen:
            frame = spark.range(0, args.range_rows, 1, 2).where("id % 7 = 0").agg(
                F.count("*").alias("n"), F.sum("id").alias("total"))
            last = (args.range_rows - 1) // 7
            expected = (last + 1, 7 * last * (last + 1) // 2)
            def range_valid(rows, context):
                require(len(rows) == 1 and tuple(rows[0]) == expected, "incorrect range aggregate")
                return {"rows": 1, "input_rows": args.range_rows,
                        "validation": {"count": expected[0], "sum": expected[1]}}
            repeated("range_filter_aggregate", args.query_warmups, args.query_samples,
                     lambda _, start: frame.collect(), range_valid)
        if "join_aggregate" in chosen:
            keys = min(1024, args.join_rows)
            left = spark.range(0, args.join_rows, 1, 2).select(
                F.col("id").alias("left_id"), (F.col("id") % keys).alias("key"))
            right = spark.range(0, keys, 1, 2).select(F.col("id").alias("key"),
                (F.col("id") % 5).alias("weight"))
            frame = left.join(F.broadcast(right), "key").agg(F.count("*").alias("n"),
                F.sum("left_id").alias("total"), F.sum("weight").alias("weighted"))
            weighted = sum((((args.join_rows - 1 - key) // keys) + 1) * (key % 5) for key in range(keys))
            expected = (args.join_rows, args.join_rows * (args.join_rows - 1) // 2, weighted)
            def join_valid(rows, context):
                require(len(rows) == 1 and tuple(rows[0]) == expected, "incorrect join aggregate")
                return {"rows": 1, "input_rows": args.join_rows,
                        "validation": {"count": expected[0], "sum": expected[1], "weighted_sum": expected[2]}}
            repeated("join_aggregate", args.query_warmups, args.query_samples,
                     lambda _, start: frame.collect(), join_valid)

        if "arrow_stream" in chosen:
            def arrow_setup():
                operation = str(uuid.uuid4())
                return {"operation": operation, "call": None,
                        "request": base.ExecutePlanRequest(**fields, operation_id=operation,
                            plan=base.Plan(root=relations.Relation(range=relations.Range(
                                start=0, end=args.stream_rows, step=1, num_partitions=2))),
                            request_options=[base.ExecutePlanRequest.RequestOption(
                                reattach_options=base.ReattachOptions(reattachable=True))])}
            def arrow_run(context, start):
                context["call"] = stub.ExecutePlan(context["request"], timeout=args.timeout)
                messages = []
                first = None
                for response in context["call"]:
                    if first is None and response.HasField("arrow_batch"):
                        first = time.perf_counter_ns() - start
                    messages.append(response)
                return messages, first
            def arrow_valid(result, context):
                messages, first = result
                require(messages and messages[-1].HasField("result_complete"), "Arrow result is incomplete")
                rows = declared = payload_bytes = checksum = protobuf_bytes = batches = 0
                minimum = maximum = None
                for response in messages:
                    require(response.session_id == raw_session and response.operation_id == context["operation"],
                            "Arrow response identity changed")
                    protobuf_bytes += response.ByteSize()
                    if not response.HasField("arrow_batch"):
                        continue
                    arrow = response.arrow_batch
                    declared += arrow.row_count
                    payload_bytes += len(arrow.data)
                    with pa.ipc.open_stream(arrow.data) as reader:
                        require(len(reader.schema) == 1 and reader.schema[0].name == "id"
                                and pa.types.is_int64(reader.schema[0].type), "incorrect Arrow schema")
                        for batch in reader:
                            batches += 1
                            rows += batch.num_rows
                            column = batch.column(0)
                            require(column.null_count == 0, "Arrow range contains nulls")
                            if batch.num_rows:
                                checksum += pc.sum(column).as_py()
                                lo, hi = pc.min(column).as_py(), pc.max(column).as_py()
                                minimum = lo if minimum is None else min(minimum, lo)
                                maximum = hi if maximum is None else max(maximum, hi)
                require(rows == declared == args.stream_rows, "incorrect Arrow row count")
                require(checksum == args.stream_rows * (args.stream_rows - 1) // 2
                        and minimum == 0 and maximum == args.stream_rows - 1, "incorrect Arrow range values")
                require(first is not None and payload_bytes >= args.stream_rows * 8, "missing Arrow payload")
                return {"rows": rows, "arrow_ipc_bytes": payload_bytes, "protobuf_bytes": protobuf_bytes,
                        "response_count": len(messages), "arrow_record_batches": batches,
                        "first_batch_ns": first, "validation": {"sum": checksum, "min": minimum, "max": maximum}}
            def arrow_cleanup(context, result):
                if context["call"] is not None:
                    context["call"].cancel()
                response = stub.ReleaseExecute(base.ReleaseExecuteRequest(**fields,
                    operation_id=context["operation"], release_all=base.ReleaseExecuteRequest.ReleaseAll()),
                    timeout=args.timeout)
                require(response.operation_id == context["operation"] or
                        (args.allow_completed_release_absence and not response.HasField("operation_id")),
                        "ReleaseAll did not release the measured operation")
            repeated("arrow_stream", args.arrow_warmups, args.arrow_samples, arrow_run, arrow_valid,
                     arrow_setup, arrow_cleanup)

        if "config_concurrent" in chosen:
            stubs = [rpc.SparkConnectServiceStub(channel()) for _ in range(args.concurrency)]
            pool = ThreadPoolExecutor(max_workers=args.concurrency)
            def concurrent_setup():
                barrier = threading.Barrier(args.concurrency + 1)
                def worker(index):
                    barrier.wait()
                    observations = []
                    for call_index in range(index, args.config_calls, args.concurrency):
                        started, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        try:
                            response = stubs[index].Config(config_request, timeout=args.timeout)
                            error = None
                        except Exception as failure:
                            response, error = None, repr(failure)
                        observations.append((call_index, time.perf_counter_ns() - started,
                                             time.thread_time_ns() - cpu, response, error))
                    return observations
                return barrier, [pool.submit(worker, index) for index in range(args.concurrency)]
            def concurrent_run(context, start):
                barrier, futures = context
                barrier.wait()
                return sorted((entry for future in futures for entry in future.result()), key=lambda value: value[0])
            def concurrent_valid(observations, context):
                require(len(observations) == args.config_calls, "concurrent call count changed")
                for _, elapsed, _, response, error in observations:
                    require(error is None and elapsed > 0, "concurrent Config failed: " + str(error))
                    config_valid(response)
                return {"calls": len(observations), "concurrency": args.concurrency,
                        "response_count": len(observations),
                        "call_latencies_ns": [entry[1] for entry in observations],
                        "call_thread_cpu_ns": [entry[2] for entry in observations]}
            repeated("config_concurrent", args.concurrency_warmups, args.concurrency_samples,
                     concurrent_run, concurrent_valid, concurrent_setup)
    except Exception as error:
        report.update({"error": repr(error), "traceback": traceback.format_exc()})
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        if spark is not None:
            try:
                spark.stop()
                report["cleanup"].append({"session": "pyspark", "passed": True})
            except Exception as error:
                report["cleanup"].append({"session": "pyspark", "passed": False, "error": repr(error)})
        if raw_ready:
            try:
                stub.ReleaseSession(base.ReleaseSessionRequest(session_id=raw_session, user_context=user), timeout=args.timeout)
                report["cleanup"].append({"session": "raw", "passed": True})
            except Exception as error:
                report["cleanup"].append({"session": "raw", "passed": False, "error": repr(error)})
        for value in channels:
            value.close()
        for name in dict.fromkeys(sample["workload"] for sample in report["samples"]):
            measured = [sample for sample in report["samples"] if sample["workload"] == name and sample["phase"] == "measured"]
            valid = [sample for sample in measured if sample["passed"]]
            summary = {"workload": name, "measured_samples": len(measured), "valid_samples": len(valid),
                       "passed": len(valid) == len(measured) and bool(measured)}
            if valid:
                times = [sample["elapsed_ns"] for sample in valid]
                summary.update({"median_ns": statistics.median(times), "p95_ns": percentile(times, .95),
                                "min_ns": min(times), "max_ns": max(times)})
            report["workload_summaries"].append(summary)
        report["passed"] = ("error" not in report and bool(report["samples"])
                            and all(sample["passed"] for sample in report["samples"])
                            and all(item["passed"] for item in report["cleanup"]))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"output": str(args.output), "passed": report["passed"]}), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
