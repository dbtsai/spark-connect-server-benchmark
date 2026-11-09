"""Launch the native server inside Spark and run reproducible lifecycle/API acceptance."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time


SHUTDOWN_COMPLETE = "Embedded Spark Connect shutdown complete"


def shutdown_error(content, exit_code):
    if re.search(r"shutdown failed|JNI stop is incomplete|native stop incomplete", content,
                 flags=re.IGNORECASE):
        return "embedded server reported failed or incomplete shutdown"
    if SHUTDOWN_COMPLETE not in content.splitlines():
        return "embedded server omitted successful cleanup marker"
    if exit_code not in (0, -signal.SIGTERM, 128 + signal.SIGTERM):
        return "embedded server exited abnormally"
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path, help="built JNI .dylib or .so")
    parser.add_argument("--spark-home", type=Path, default=os.environ.get("SPARK_HOME"))
    parser.add_argument("--python", default=sys.executable, help="official PySpark client/worker Python")
    parser.add_argument("--rust-python", help="optional Python with the Rust-backed PySpark client")
    parser.add_argument("--rust-example", type=Path, help="optional prebuilt official Rust client example")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.spark_home is None:
        parser.error("set SPARK_HOME or pass --spark-home")
    root = Path(__file__).resolve().parents[1]
    library = args.library.resolve(strict=True)
    spark_home = args.spark_home.resolve(strict=True)
    build = root / ".build" / "jvm-native"
    build.mkdir(parents=True, exist_ok=True)
    output = args.output or build / "acceptance.json"
    env = os.environ.copy()
    env.update({"SPARK_HOME": str(spark_home), "SPARK_LOCAL_IP": "127.0.0.1",
                "SPARK_CONNECT_MODE": "0", "PYSPARK_PYTHON": args.python, "TZ": "UTC"})
    common = [str(spark_home / "bin" / "spark-submit"), "--master", "local[2]",
              "--driver-memory", "1g", "--driver-java-options", "-Xcheck:jni",
              "--conf", "spark.ui.enabled=false", "--conf", "spark.driver.host=127.0.0.1",
              "--conf", "spark.driver.bindAddress=127.0.0.1", "--conf", "spark.sql.shuffle.partitions=2",
              "--conf", "spark.pyspark.python=" + args.python,
              "--conf", "spark.sql.warehouse.dir=" + str(build / "warehouse")]
    report = {"library": str(library), "spark_home": str(spark_home), "results": []}
    server = None
    log = None
    address = None

    def run(name, command, timeout=180):
        started = time.monotonic()
        completed = subprocess.run(command, cwd=root, env=env, capture_output=True,
                                   text=True, timeout=timeout)
        path = build / (name + ".log")
        path.write_text(completed.stdout + completed.stderr)
        result = {"name": name, "command": command, "exit_code": completed.returncode,
                  "seconds": round(time.monotonic() - started, 3), "log": str(path)}
        report["results"].append(result)
        print(json.dumps(result), flush=True)
        if completed.returncode != 0:
            raise RuntimeError(name + " failed; see " + str(path))
        if re.search(r"WARNING in native method|FATAL ERROR in native method", path.read_text()):
            raise RuntimeError(name + " triggered a JNI check warning")
        return completed

    try:
        lifecycle = run("lifecycle", common + ["--class",
            "org.apache.spark.sql.connect.nativebridge.NativeLifecycleTest",
            str(root / ".build" / "jvm" / "bridge-test.jar"), str(library)], timeout=90)
        if "NATIVE_LIFECYCLE_OK" not in lifecycle.stdout:
            raise RuntimeError("native lifecycle success marker missing")
        command = common + ["--class", "org.apache.spark.sql.connect.nativebridge.EmbeddedSparkConnect",
                            str(root / ".build" / "jvm" / "spark-connect-native-bridge.jar"),
                            str(library), "127.0.0.1:0"]
        log_path = build / "server.log"
        log = log_path.open("w")
        server = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        report.update({"server_command": command, "server_pid": server.pid, "server_log": str(log_path)})
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            content = log_path.read_text(errors="replace")
            listening = re.search(r"Embedded Spark Connect listening on (\S+)", content)
            identity = re.search(r"Embedded Spark Connect JVM PID (\d+); Java Connect listener absent", content)
            if listening and identity:
                address = listening.group(1)
                report["reported_jvm_pid"] = int(identity.group(1))
                if int(identity.group(1)) != server.pid:
                    raise RuntimeError("Spark launcher and JVM PID differ")
                break
            if server.poll() is not None:
                raise RuntimeError("embedded server exited during startup; see " + str(log_path))
            time.sleep(0.1)
        else:
            raise RuntimeError("embedded server startup timed out; see " + str(log_path))
        lsof = shutil.which("lsof") or "/usr/sbin/lsof"
        ownership = subprocess.run([lsof, "-nP", "-iTCP:" + address.rsplit(":", 1)[1],
                                    "-sTCP:LISTEN", "-Fp"], capture_output=True, text=True, check=True)
        owners = sorted({int(line[1:]) for line in ownership.stdout.splitlines() if line.startswith("p")})
        report["listener_owner_pids"] = owners
        if owners != [server.pid]:
            raise RuntimeError("native listener is not owned exclusively by the Spark JVM")
        env["SPARK_REMOTE"] = "sc://" + address
        report["remote"] = env["SPARK_REMOTE"]
        run("official-python", [args.python, "jvm/client_acceptance.py", "--profile", "official", "--protocol"])
        if args.rust_python:
            run("rust-python", [args.rust_python, "jvm/client_acceptance.py", "--profile", "rust"])
        if args.rust_example:
            run("official-rust", [str(args.rust_example.resolve(strict=True))])
    except Exception as error:
        report["error"] = repr(error)
    finally:
        if server is not None:
            if server.poll() is not None and "error" not in report:
                report["error"] = "embedded server exited before requested shutdown"
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                report["forced_kill"] = True
            report["server_exit_code"] = server.returncode
        if log is not None:
            log.close()
        if address is not None:
            try:
                import socket
                with socket.create_connection(("127.0.0.1", int(address.rsplit(":", 1)[1])), timeout=1):
                    report["listener_leaked"] = True
            except OSError:
                report["listener_closed"] = True
        if "server_log" in report:
            content = Path(report["server_log"]).read_text(errors="replace")
            report["jni_check_clean"] = not bool(re.search(
                r"WARNING in native method|FATAL ERROR in native method", content))
            if not report["jni_check_clean"]:
                report["error"] = "embedded server triggered a JNI check warning"
            report["shutdown_complete"] = SHUTDOWN_COMPLETE in content.splitlines()
            failure = shutdown_error(content, report["server_exit_code"])
            if failure is not None:
                report.setdefault("error", failure)
        report["passed"] = "error" not in report and not report.get("forced_kill") and not report.get("listener_leaked")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"output": str(output), "passed": report["passed"]}), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
