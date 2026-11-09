"""Spark API and protocol acceptance checks for the embedded native server."""

import argparse
import datetime
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import time
import traceback
import uuid
import zlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def run_protocol(remote):
    import grpc
    from pyspark.sql.connect.proto import base_pb2 as base
    from pyspark.sql.connect.proto import base_pb2_grpc as rpc
    from pyspark.sql.connect.proto import relations_pb2 as relations

    address = remote.removeprefix("sc://").split("/", 1)[0]
    session_id = str(uuid.uuid4())
    operation_id = str(uuid.uuid4())
    user = base.UserContext(user_id="jvm-acceptance-protocol")
    results = {}
    with grpc.insecure_channel(address) as channel:
        stub = rpc.SparkConnectServiceStub(channel)
        try:
            request = base.ExecutePlanRequest(
                session_id=session_id,
                user_context=user,
                operation_id=operation_id,
                plan=base.Plan(
                    root=relations.Relation(
                        range=relations.Range(start=0, end=3, step=1, num_partitions=1)
                    )
                ),
                request_options=[
                    base.ExecutePlanRequest.RequestOption(
                        reattach_options=base.ReattachOptions(reattachable=True)
                    )
                ],
            )
            messages = list(stub.ExecutePlan(request, timeout=30))
            assert messages and messages[-1].HasField("result_complete"), messages
            assert all(message.session_id == session_id for message in messages)
            assert all(message.operation_id == operation_id for message in messages)
            response_ids = [message.response_id for message in messages]
            assert len(set(response_ids)) == len(response_ids), response_ids
            server_id = messages[0].server_side_session_id
            assert server_id and all(
                message.server_side_session_id == server_id for message in messages
            )
            replay = list(
                stub.ReattachExecute(
                    base.ReattachExecuteRequest(
                        session_id=session_id,
                        user_context=user,
                        operation_id=operation_id,
                        client_observed_server_side_session_id=server_id,
                        last_response_id=response_ids[0],
                    ),
                    timeout=30,
                )
            )
            assert [message.response_id for message in replay] == response_ids[1:]
            results["reattach"] = {
                "response_count": len(messages),
                "replayed_response_count": len(replay),
                "response_types": [message.WhichOneof("response_type") for message in messages],
                "preserved_response_ids": True,
            }
            released = stub.ReleaseExecute(
                base.ReleaseExecuteRequest(
                    session_id=session_id,
                    user_context=user,
                    operation_id=operation_id,
                    client_observed_server_side_session_id=server_id,
                    release_until=base.ReleaseExecuteRequest.ReleaseUntil(
                        response_id=response_ids[0]
                    ),
                ),
                timeout=30,
            )
            assert released.session_id == session_id
            assert released.server_side_session_id == server_id
            assert released.operation_id == operation_id
            released = stub.ReleaseExecute(
                base.ReleaseExecuteRequest(
                    session_id=session_id,
                    user_context=user,
                    operation_id=operation_id,
                    client_observed_server_side_session_id=server_id,
                    release_all=base.ReleaseExecuteRequest.ReleaseAll(),
                ),
                timeout=30,
            )
            assert released.operation_id == operation_id
            try:
                list(
                    stub.ReattachExecute(
                        base.ReattachExecuteRequest(
                            session_id=session_id,
                            user_context=user,
                            operation_id=operation_id,
                            client_observed_server_side_session_id=server_id,
                        ),
                        timeout=30,
                    )
                )
            except grpc.RpcError as error:
                assert error.code() in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.INTERNAL)
                assert "OPERATION_NOT_FOUND" in error.details() or "not found" in error.details().lower()
                results["release"] = {
                    "release_until": True,
                    "release_all": True,
                    "reattach_after_release_code": error.code().name,
                }
            else:
                raise AssertionError("released operation remained reattachable")

            payload = b"Spark Connect JVM artifact forwarding\n" * 512
            artifact_name = "cache/" + hashlib.sha256(payload).hexdigest()
            statuses = stub.ArtifactStatus(
                base.ArtifactStatusesRequest(
                    session_id=session_id,
                    user_context=user,
                    names=[artifact_name],
                    client_observed_server_side_session_id=server_id,
                ),
                timeout=30,
            )
            assert not statuses.statuses[artifact_name].exists
            midpoint = len(payload) // 2
            chunks = [payload[:midpoint], payload[midpoint:]]
            upload = [
                base.AddArtifactsRequest(
                    session_id=session_id,
                    user_context=user,
                    client_observed_server_side_session_id=server_id,
                    begin_chunk=base.AddArtifactsRequest.BeginChunkedArtifact(
                        name=artifact_name,
                        total_bytes=len(payload),
                        num_chunks=2,
                        initial_chunk=base.AddArtifactsRequest.ArtifactChunk(
                            data=chunks[0], crc=zlib.crc32(chunks[0])
                        ),
                    ),
                ),
                base.AddArtifactsRequest(
                    session_id=session_id,
                    user_context=user,
                    client_observed_server_side_session_id=server_id,
                    chunk=base.AddArtifactsRequest.ArtifactChunk(
                        data=chunks[1], crc=zlib.crc32(chunks[1])
                    ),
                ),
            ]
            uploaded = stub.AddArtifacts(iter(upload), timeout=30)
            assert uploaded.session_id == session_id
            assert uploaded.server_side_session_id == server_id
            assert len(uploaded.artifacts) == 1
            assert uploaded.artifacts[0].name == artifact_name
            assert uploaded.artifacts[0].is_crc_successful
            statuses = stub.ArtifactStatus(
                base.ArtifactStatusesRequest(
                    session_id=session_id,
                    user_context=user,
                    names=[artifact_name],
                    client_observed_server_side_session_id=server_id,
                ),
                timeout=30,
            )
            assert statuses.statuses[artifact_name].exists
            results["artifacts"] = {"chunks": 2, "bytes": len(payload), "cached": True}
        finally:
            stub.ReleaseSession(
                base.ReleaseSessionRequest(session_id=session_id, user_context=user), timeout=30
            )
    return results


def run_transport_errors(remote):
    import grpc
    from google.rpc import status_pb2
    from pyspark.sql.connect.proto import base_pb2 as base
    from pyspark.sql.connect.proto import base_pb2_grpc as rpc
    from pyspark.sql.connect.proto import relations_pb2 as relations

    address = remote.removeprefix("sc://").split("/", 1)[0]
    results = {}
    with grpc.insecure_channel(
        address, options=[("grpc.max_send_message_length", 65 * 1024 * 1024)]
    ) as channel:
        for name, method, payload, expected in [
            ("unknown_nonempty", "/spark.connect.SparkConnectService/UnknownMethod", b"nonempty",
             grpc.StatusCode.UNIMPLEMENTED),
            ("malformed_protobuf", "/spark.connect.SparkConnectService/Config", b"\xff",
             grpc.StatusCode.INTERNAL),
            ("oversized_declared_input", "/spark.connect.SparkConnectService/Config",
             bytes(64 * 1024 * 1024 + 1), grpc.StatusCode.OUT_OF_RANGE),
        ]:
            call = channel.unary_unary(method, request_serializer=lambda value: value,
                                      response_deserializer=lambda value: value)
            try:
                call(payload, timeout=20)
            except grpc.RpcError as error:
                assert error.code() == expected, (name, error.code(), error.details())
                results[name] = {"code": error.code().name, "expected": expected.name,
                                 "details": error.details()}
                if name == "oversized_declared_input":
                    assert "67108865" in error.details() and "67108864" in error.details(), error.details()
                    assert "message length too large" in error.details(), error.details()
            else:
                raise AssertionError(name + " unexpectedly succeeded")

        stub = rpc.SparkConnectServiceStub(channel)
        session_id = str(uuid.uuid4())
        user = base.UserContext(user_id="native-error-details")
        try:
            request = base.ExecutePlanRequest(
                session_id=session_id, user_context=user, operation_id=str(uuid.uuid4()),
                plan=base.Plan(root=relations.Relation(sql=relations.SQL(
                    query="SELECT * FROM missing_native_error_" + uuid.uuid4().hex
                ))),
            )
            try:
                list(stub.ExecutePlan(request, timeout=30))
            except grpc.RpcError as error:
                values = [value for key, value in error.trailing_metadata()
                          if key == "grpc-status-details-bin"]
                assert values and isinstance(values[0], bytes), error.trailing_metadata()
                details = status_pb2.Status.FromString(values[0])
                assert details.details, details
                assert "TABLE_OR_VIEW_NOT_FOUND" in str(details), details
                results["structured_spark_error"] = {
                    "code": error.code().name,
                    "detail_count": len(details.details),
                    "binary_status_bytes": len(values[0]),
                }
            else:
                raise AssertionError("missing table unexpectedly succeeded")
        finally:
            stub.ReleaseSession(base.ReleaseSessionRequest(session_id=session_id, user_context=user),
                                timeout=30)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=["official", "rust"], default="official")
    parser.add_argument("--protocol", action="store_true")
    args = parser.parse_args()
    remote = os.environ["SPARK_REMOTE"]
    # A missing-view error is expected below when proving session isolation.
    logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)
    results = []
    sessions = []

    def check(name, action):
        started = time.monotonic()
        try:
            value = action()
            results.append(
                {"name": name, "passed": True, "seconds": round(time.monotonic() - started, 3), "result": value}
            )
        except Exception as error:
            results.append(
                {"name": name, "passed": False, "seconds": round(time.monotonic() - started, 3), "error": repr(error), "traceback": traceback.format_exc()}
            )

    spark = SparkSession.builder.remote(remote).create()
    sessions.append(spark)
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

        def dataframe_join():
            left = spark.range(4).withColumnRenamed("id", "left_id")
            right = spark.range(2, 6).withColumnRenamed("id", "right_id")
            rows = (
                left.join(right, F.col("left_id") == F.col("right_id"), "inner")
                .select(F.col("left_id"), F.col("right_id"))
                .orderBy(F.col("left_id"))
                .collect()
            )
            assert [tuple(row) for row in rows] == [(2, 2), (3, 3)], rows
            return [list(row) for row in rows]

        check("dataframe_join", dataframe_join)

        def catalyst_and_types():
            row = spark.sql(
                "SELECT transform(array(1, 2, 3), x -> x * x) AS squares, "
                "CAST('123.45' AS DECIMAL(10, 2)) AS amount, "
                "TIMESTAMP '2024-01-02 03:04:05' AS at"
            ).collect()[0]
            assert list(row[0]) == [1, 4, 9], row
            assert row[1] == Decimal("123.45"), row
            timestamp_micros = (
                int(row[2].timestamp() * 1_000_000)
                if isinstance(row[2], datetime.datetime)
                else row[2]
            )
            assert timestamp_micros == 1_704_164_645_000_000, row
            return {"squares": list(row[0]), "amount": str(row[1]), "timestamp_micros": timestamp_micros, "timestamp_client_type": type(row[2]).__name__}

        check("catalyst_higher_order_decimal_timestamp", catalyst_and_types)

        def configuration_and_views():
            key = "spark.sql.shuffle.partitions"
            original = spark.conf.get(key)
            spark.conf.set(key, "7")
            assert spark.conf.get(key) == "7"
            other = SparkSession.builder.remote(remote).create()
            sessions.append(other)
            assert other.conf.get(key) == original
            other.conf.set(key, "11")
            assert other.conf.get(key) == "11"
            assert spark.conf.get(key) == "7"
            name = "jvm_acceptance_" + uuid.uuid4().hex
            spark.range(4).createOrReplaceTempView(name)
            assert spark.sql(f"SELECT SUM(id) AS total FROM {name}").collect()[0][0] == 6
            try:
                other.sql(f"SELECT * FROM {name}").collect()
            except Exception as error:
                assert "TABLE_OR_VIEW_NOT_FOUND" in str(error) or "not found" in str(error).lower()
            else:
                raise AssertionError("temporary view leaked to another session")
            spark.conf.set(key, original)
            return {"first_session_value": "7", "second_session_value": "11", "isolated_temp_view_sum": 6}

        check("configuration_and_temp_view_isolation", configuration_and_views)

        def python_udf():
            transform = F.udf(lambda value: value * 3 + 1, T.LongType())
            rows = spark.range(4).select(transform(F.col("id")).alias("value")).orderBy(F.col("value")).collect()
            assert [row[0] for row in rows] == [1, 4, 7, 10], rows
            return [row[0] for row in rows]

        check("python_udf", python_udf)

        def artifact_python_udf():
            directory = Path(__file__).resolve().parents[1] / ".build" / "jvm-native" / "artifacts"
            directory.mkdir(parents=True, exist_ok=True)
            module_name = "jvm_acceptance_helper_" + uuid.uuid4().hex
            artifact = directory / (module_name + ".py")
            artifact.write_text("def bump(value):\n    return value + 40\n")
            try:
                spark.addArtifact(str(artifact), pyfile=True)

                def apply_artifact(value):
                    import importlib
                    return importlib.import_module(module_name).bump(value)

                transform = F.udf(apply_artifact, T.LongType())
                rows = spark.range(3).select(transform(F.col("id")).alias("value")).orderBy(F.col("value")).collect()
                assert [row[0] for row in rows] == [40, 41, 42], rows
                return [row[0] for row in rows]
            finally:
                artifact.unlink(missing_ok=True)

        check("add_artifact_python_udf", artifact_python_udf)
        if args.protocol:
            check("explicit_grpc_reattach_release_chunked_artifacts", lambda: run_protocol(remote))
            check("native_transport_error_codes_and_details", lambda: run_transport_errors(remote))
    finally:
        for session in reversed(sessions):
            try:
                session.stop()
            except Exception as error:
                results.append({"name": "session_stop", "passed": False, "error": repr(error)})
    report = {"profile": args.profile, "remote": remote, "checks": results, "passed": all(result["passed"] for result in results)}
    print(json.dumps(report, indent=2), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
