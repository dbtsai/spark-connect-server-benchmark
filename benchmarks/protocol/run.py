#!/usr/bin/env python3
"""Run local, sequential Spark Connect protocol/planning benchmark forks."""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TARGETS = ("rust-fixture", "jvm-fixture", "jvm-planning", "rust-jvm-planning")
FIXTURE_MARKER = "PROTOCOL_FIXTURE_LISTENING "


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def csv(value, choices=None, integer=False):
    values = [part.strip() for part in value.split(",")]
    if not values or any(not part for part in values) or len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("expected distinct comma-separated values")
    if choices and not set(values) <= set(choices):
        raise argparse.ArgumentTypeError("expected values from " + ",".join(choices))
    if integer:
        try:
            parsed = [int(value) for value in values]
        except ValueError as error:
            raise argparse.ArgumentTypeError("expected positive integers") from error
        if any(value < 1 or value > 512 for value in parsed):
            raise argparse.ArgumentTypeError("concurrency must be 1..512")
        if len(set(parsed)) != len(parsed):
            raise argparse.ArgumentTypeError("concurrency values must be distinct")
        values = list(map(str, parsed))
    return ",".join(values)


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def free_port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def connected(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def listener_owners(port):
    """Check identity so a startup race cannot target an unrelated listener."""
    lsof = shutil.which("lsof") or ("/usr/sbin/lsof" if Path("/usr/sbin/lsof").exists() else None)
    if lsof:
        result = subprocess.run([lsof, "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fp"],
                                capture_output=True, text=True, timeout=10)
        if result.returncode not in (0, 1):
            raise RuntimeError("listener ownership inspection failed: " + result.stderr.strip())
        return sorted({int(line[1:]) for line in result.stdout.splitlines() if line.startswith("p")})
    # Linux fallback maps listening socket inodes to their owning process FDs.
    inodes = set()
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        if path.exists():
            for line in path.read_text().splitlines()[1:]:
                fields = line.split()
                if fields[3] == "0A" and int(fields[1].split(":")[1], 16) == port:
                    inodes.add(fields[9])
    if not Path("/proc").is_dir():
        raise RuntimeError("listener inspection requires lsof or Linux /proc")
    owners = set()
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            for fd in (process / "fd").iterdir():
                target = os.readlink(fd)
                if target.startswith("socket:[") and target[8:-1] in inodes:
                    owners.add(int(process.name))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return sorted(owners)


def proc_stats(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "pid=,rss=,time=,pcpu="],
                            capture_output=True, text=True, timeout=10)
    return {"fields": ["pid", "rss_kib", "cpu_time", "cpu_percent"],
            "raw": result.stdout.strip()}


def command_output(command, env):
    try:
        result = subprocess.run(command, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, timeout=25)
        return {"command": command, "exit_code": result.returncode, "output": result.stdout}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "error": str(error)}


def environment(args, folder=None):
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", SPARK_CONNECT_MODE="0", RUST_LOG="warn")
    for variable, home in (("JAVA_HOME", args.java_home), ("SPARK_HOME", args.spark_home)):
        if home is None:
            env.pop(variable, None)
        else:
            env[variable] = str(home)
    for key in ("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "SPARK_SUBMIT_OPTS",
                "SPARK_REMOTE", "SPARK_API_MODE"):
        env.pop(key, None)
    if folder:
        env.update(SPARK_CONF_DIR=str(folder / "conf"), SPARK_LOG_DIR=str(folder / "logs"),
                   SPARK_LOCAL_DIRS=str(folder / "local"))
    return env


def freeze(args, output):
    artifacts = output / "artifacts"
    artifacts.mkdir()
    records = {}
    names = ["binary"]
    if any(target != "rust-fixture" for target in args.target_list):
        names.append("jar")
    if "rust-jvm-planning" in args.target_list:
        names.append("library")
    for name in names:
        source = getattr(args, name)
        destination = artifacts / source.name
        shutil.copy2(source, destination)
        records[name] = {"source": str(source), "snapshot": str(destination),
                         "sha256": sha(destination)}
        setattr(args, name, destination)
    (artifacts / "runner.py").write_bytes(Path(__file__).read_bytes())
    (artifacts / "reporter.py").write_bytes((HERE / "report.py").read_bytes())
    return records


def prepare_payloads(args, output, env):
    destination = output / "payloads"
    if args.payload_dir:
        source = args.payload_dir
        manifest_bytes = (source / "manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        destination.mkdir()
        for entry in manifest["payloads"] + manifest.get("cases", []):
            for field in ("schema_file", "arrow_file", "relation_file", "expected_schema_file"):
                if field not in entry:
                    continue
                name = Path(entry[field])
                if name.is_absolute() or any(part in (".", "..") for part in name.parts):
                    raise ValueError("payload manifest path escapes its directory")
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source / name, target)
        (destination / "manifest.json").write_bytes(manifest_bytes)
    else:
        command = [str(args.binary), "export", "--profile", args.profile,
                   "--output", str(destination)]
        with (output / "export.log").open("w") as log:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=120)
        if result.returncode:
            raise RuntimeError("corpus export failed; see export.log")
    hashes = {str(path.relative_to(destination)): sha(path)
              for path in sorted(destination.rglob("*")) if path.is_file()}
    return destination, hashes


def spark_jars(home):
    assembly = home / "assembly/target/scala-2.13/jars"
    result = assembly if assembly.is_dir() else home / "jars"
    if not result.is_dir():
        raise ValueError("Spark jar directory missing: " + str(result))
    return result


def server_command(args, target, folder):
    if target == "rust-fixture":
        return ([str(args.binary), "serve-fixture", "--payload-dir", str(args.payloads),
                 "--listen", "127.0.0.1:0", "--threads", str(args.server_threads)], None, {})
    if target == "jvm-fixture":
        return ([str(args.java_home / "bin/java"), "-Xms256m", "-Xmx1g", "-cp",
                 str(args.jar) + os.pathsep + str(spark_jars(args.spark_home) / "*"),
                 "org.apache.spark.sql.connect.nativebridge.benchmark.ProtocolFixtureServer",
                 str(args.payloads), "0", str(args.server_threads)], None, {})
    port = free_port()
    configs = {
        "spark.connect.grpc.binding.address": "127.0.0.1",
        "spark.connect.grpc.binding.port": str(port), "spark.connect.grpc.port.maxRetries": "0",
        "spark.driver.bindAddress": "127.0.0.1", "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "2",
        "spark.sql.session.timeZone": "UTC", "spark.sql.artifact.isolation.enabled": "true",
        "spark.sql.artifact.isolation.alwaysApplyClassloader": "true",
        "spark.extraListeners": "org.apache.spark.sql.connect.nativebridge.benchmark.NoExecutionListener",
        "spark.sql.warehouse.dir": str(folder / "warehouse"),
    }
    klass = ("org.apache.spark.sql.connect.service.SparkConnectServer" if target == "jvm-planning"
             else "org.apache.spark.sql.connect.nativebridge.engine.EmbeddedSparkEngine")
    command = [str(args.spark_home / "bin/spark-submit"), "--class", klass, "--master", "local[2]",
               "--driver-memory", "1g", "--name", folder.name]
    if target == "jvm-planning":
        command += ["--jars", str(args.jar)]
    for name, value in configs.items():
        command += ["--conf", name + "=" + value]
    command += (["spark-internal"] if target == "jvm-planning"
                else [str(args.jar), str(args.library), f"127.0.0.1:{port}"])
    return command, port, configs


def await_ready(process, log, target, selected_port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup ({process.returncode}): {log}")
        text = log.read_text(errors="replace")
        if target.endswith("fixture"):
            match = re.search(r"^PROTOCOL_FIXTURE_LISTENING 127\.0\.0\.1:(\d+)$", text, re.MULTILINE)
            if match:
                selected_port = int(match.group(1))
        if target == "rust-jvm-planning":
            identity = f"Embedded Spark Connect JVM PID {process.pid}; Java Connect listener absent"
            if identity not in text:
                time.sleep(0.05)
                continue
        if selected_port and connected(selected_port):
            owners = listener_owners(selected_port)
            if owners != [process.pid]:
                raise RuntimeError(f"listener belongs to {owners}, expected server PID {process.pid}")
            return selected_port, owners
        time.sleep(0.05)
    raise RuntimeError("server readiness timeout: " + str(log))


def last_json_marker(text, marker):
    matches = re.findall(r"^" + re.escape(marker) + r" (.+)$", text, re.MULTILINE)
    return json.loads(matches[-1]) if matches else None


def stop(process, log, target, selected_port):
    if process.poll() is not None:
        raise RuntimeError(f"server exited before controlled shutdown ({process.returncode})")
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired as error:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        raise RuntimeError("server did not finish SIGTERM shutdown") from error
    if process.returncode not in (0, -signal.SIGTERM, 128 + signal.SIGTERM):
        raise RuntimeError(f"unexpected server exit code {process.returncode}")
    deadline = time.monotonic() + 3
    while selected_port and connected(selected_port) and time.monotonic() < deadline:
        time.sleep(0.05)
    if selected_port and connected(selected_port):
        raise RuntimeError("server listener stayed open after shutdown")
    text = log.read_text(errors="replace")
    if re.search(r"WARNING in native method|FATAL ERROR in native method|Java engine handle cleanup failed:", text):
        raise RuntimeError("JNI cleanup/checking error in " + str(log))
    evidence = {"exit_code": process.returncode, "listener_closed": True}
    if target.endswith("fixture"):
        if "PROTOCOL_FIXTURE_SHUTDOWN" not in text:
            raise RuntimeError("fixture shutdown marker missing")
        stats = last_json_marker(text, "PROTOCOL_FIXTURE_STATS")
        if not isinstance(stats, dict) or stats.get("query_executions") != 0:
            raise RuntimeError("fixture execution-free audit missing or failed")
        evidence["fixture_stats"] = stats
    else:
        jobs = last_json_marker(text, "PROTOCOL_BENCH_SPARK_JOBS")
        if not isinstance(jobs, dict) or jobs.get("jobs_started") != 0:
            raise RuntimeError("planning-only Spark job audit missing or nonzero")
        evidence["spark_job_audit"] = jobs
        if target == "rust-jvm-planning" and "Embedded Spark Engine shutdown complete" not in text:
            raise RuntimeError("native engine shutdown did not complete")
    return evidence


def run_fork(args, target, fork, index):
    name = f"fork{fork}-{target}"
    folder = args.output / name
    folder.mkdir()
    for part in ("conf", "local", "logs", "warehouse"):
        (folder / part).mkdir()
    (folder / "conf/log4j2.properties").write_text("""status = error
name = ProtocolBenchmark
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %p %c{1}: %m%n
rootLogger.level = warn
rootLogger.appenderRef.console.ref = console
""")
    command, selected_port, configs = server_command(args, target, folder)
    env = environment(args, folder)
    record = {"fork_id": name, "fork": fork, "target": target, "order_index": index,
              "profile_kind": "fixture" if target.endswith("fixture") else "planning",
              "command": command, "spark_configs": configs, "client_json": f"{name}/client.json",
              "started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "load_before": os.getloadavg()}
    log = folder / "server.log"
    process = None
    failed = None
    try:
        with log.open("w") as stream:
            process = subprocess.Popen(command, cwd=folder, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            record["pid"] = process.pid
            selected_port, owners = await_ready(process, log, target, selected_port, args.startup_timeout)
            record.update(port=selected_port, listener_owner_pids=owners,
                          server_before=proc_stats(process.pid))
            client = [str(args.binary), "run", "--endpoint", f"http://127.0.0.1:{selected_port}",
                      "--mode", record["profile_kind"], "--profile", args.profile,
                      "--payload-dir", str(args.payloads), "--output", str(folder / "client.json"),
                      "--concurrency", args.concurrency, "--session-models", args.session_models,
                      "--channel-models", args.channel_models, "--requests", str(args.requests),
                      "--warmups", str(args.warmups), "--repetitions", str(args.repetitions),
                      "--timeout-seconds", str(args.timeout_seconds), "--client-threads", str(args.client_threads),
                      "--max-prepared-mib", str(args.max_prepared_mib)]
            if args.case:
                client += ["--case", args.case]
            if args.coverage_only:
                client.append("--coverage-only")
            record["client_command"] = client
            with (folder / "client.log").open("w") as client_log:
                result = subprocess.run(client, cwd=ROOT, env=env, stdout=client_log,
                                        stderr=subprocess.STDOUT, timeout=args.client_timeout)
            record.update(client_exit_code=result.returncode, server_after=proc_stats(process.pid),
                          load_after=os.getloadavg())
            if result.returncode:
                raise RuntimeError("client failed: " + str(folder / "client.log"))
            client_result = json.loads((folder / "client.json").read_text())
            if client_result.get("passed") is not True:
                raise RuntimeError("client validation failed: " + str(folder / "client.json"))
    except BaseException as error:
        failed = error
        record["error"] = repr(error)
    finally:
        if process:
            try:
                record["shutdown"] = stop(process, log, target, selected_port)
                record["clean_shutdown"] = True
            except Exception as error:
                record["clean_shutdown"] = False
                record["shutdown_error"] = str(error)
                if failed is None:
                    failed = error
            record["server_exit_code"] = process.returncode
        save(folder / "fork.json", record)
    if failed:
        raise failed
    return record


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--targets", type=lambda value: csv(value, TARGETS), default=",".join(TARGETS))
    parser.add_argument("--forks", type=positive, default=2,
                        help="fresh forks per target; alternating forward/reverse target order")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/spark-connect-protocol-bench")
    parser.add_argument("--jar", type=Path, default=ROOT / ".build/jvm/spark-connect-native-bridge.jar")
    native_suffix = ".dylib" if sys.platform == "darwin" else ".so"
    parser.add_argument("--library", type=Path, default=ROOT / ("target/release/libspark_connect_jni" + native_suffix))
    parser.add_argument("--spark-home", type=Path, default=os.environ.get("SPARK_HOME") or None,
                        help="Spark installation; defaults to SPARK_HOME")
    parser.add_argument("--java-home", type=Path, default=os.environ.get("JAVA_HOME") or None,
                        help="JDK installation; defaults to JAVA_HOME")
    parser.add_argument("--payload-dir", type=Path)
    parser.add_argument("--profile", choices=("smoke", "standard", "stress"), default="standard")
    parser.add_argument("--concurrency", type=lambda value: csv(value, integer=True), default="1,8,32")
    parser.add_argument("--session-models", type=lambda value: csv(value, ("shared", "isolated")), default="shared,isolated")
    parser.add_argument("--channel-models", type=lambda value: csv(value, ("shared", "isolated")), default="shared,isolated")
    parser.add_argument("--requests", type=positive, default=64)
    parser.add_argument("--warmups", type=int, default=16)
    parser.add_argument("--repetitions", type=positive, default=3)
    parser.add_argument("--timeout-seconds", type=positive, default=30)
    parser.add_argument("--server-threads", type=positive, default=4,
                        help="application workers for synthetic fixtures; real Spark retains its own pools")
    parser.add_argument("--client-threads", type=positive, default=4)
    parser.add_argument("--max-prepared-mib", type=positive, default=256,
                        help="per-cell prepared request/response budget; larger cells are explicitly skipped")
    parser.add_argument("--case", help="substring filter for timed corpus cases")
    parser.add_argument("--coverage-only", action="store_true")
    parser.add_argument("--startup-timeout", type=positive, default=90)
    parser.add_argument("--client-timeout", type=positive, default=3600)
    args = parser.parse_args(argv)
    args.target_list = args.targets.split(",")
    if any(target != "rust-fixture" for target in args.target_list):
        for name, variable in (("spark_home", "SPARK_HOME"), ("java_home", "JAVA_HOME")):
            if getattr(args, name) is None:
                parser.error("--" + name.replace("_", "-") + " or " + variable + " must be set")
    if args.warmups < 0 or args.requests < max(map(int, args.concurrency.split(","))):
        parser.error("warmups must be nonnegative and requests must cover maximum concurrency")
    if args.forks > 20 or args.server_threads > 128 or args.client_threads > 128:
        parser.error("forks <=20 and thread counts <=128 are required")
    if args.requests > 100000 or args.warmups > 100000 or args.repetitions > 100 or args.timeout_seconds > 600:
        parser.error("requests/warmups <=100000, repetitions <=100, timeout-seconds <=600 are required")
    for name in ("output", "binary", "jar", "library", "spark_home", "java_home", "payload_dir"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).expanduser().resolve())
    return args


def main():
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"schema_version": 1, "complete": False, "runs": [], "metadata": {
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "platform": platform.platform(),
        "machine": platform.machine(), "cpu_count": os.cpu_count(), "targets": args.target_list,
        "forks_per_target": args.forks, "profile": args.profile, "coverage_only": args.coverage_only,
        "concurrency": args.concurrency, "session_models": args.session_models,
        "channel_models": args.channel_models, "requests": args.requests, "warmups": args.warmups,
        "repetitions": args.repetitions, "server_threads": args.server_threads,
        "client_threads": args.client_threads, "max_prepared_mib": args.max_prepared_mib, "case_filter": args.case,
        "no_query_execution_requested": True, "runner_sha256": sha(Path(__file__)),
        "reporter_sha256": sha(HERE / "report.py")}}
    save(args.output / "manifest.json", manifest)
    try:
        manifest["metadata"]["artifacts"] = freeze(args, args.output)
        env = environment(args)
        if any(target != "rust-fixture" for target in args.target_list):
            manifest["metadata"]["java_version"] = command_output([str(args.java_home / "bin/java"), "-version"], env)
            manifest["metadata"]["spark_version"] = command_output([str(args.spark_home / "bin/spark-submit"), "--version"], env)
        args.payloads, hashes = prepare_payloads(args, args.output, env)
        manifest["metadata"]["payload_sha256"] = hashes
        manifest["metadata"]["payload_directory"] = str(args.payloads)
        save(args.output / "manifest.json", manifest)
        for fork in range(1, args.forks + 1):
            order = args.target_list if fork % 2 else list(reversed(args.target_list))
            for index, target in enumerate(order):
                print(f"Running fork {fork}: {target}", flush=True)
                try:
                    record = run_fork(args, target, fork, index)
                except BaseException:
                    record_path = args.output / f"fork{fork}-{target}/fork.json"
                    if record_path.exists():
                        manifest["runs"].append(json.loads(record_path.read_text()))
                    raise
                manifest["runs"].append(record)
                save(args.output / "manifest.json", manifest)
        current_hashes = {str(path.relative_to(args.payloads)): sha(path)
                          for path in sorted(args.payloads.rglob("*")) if path.is_file()}
        if hashes != current_hashes:
            raise RuntimeError("frozen payload files changed during the run")
        manifest["complete"] = True
    except BaseException as error:
        manifest["error"] = repr(error)
        raise
    finally:
        save(args.output / "manifest.json", manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
