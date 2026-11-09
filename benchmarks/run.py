#!/usr/bin/env python3
"""Run sequential paired JVM gRPC / embedded JNI benchmarks on one local host."""
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

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def proc_stats(pid):
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'pid=,rss=,time=,pcpu='], capture_output=True, text=True)
    return result.stdout.strip()


def listener_owners(selected_port):
    lsof = shutil.which('lsof') or '/usr/sbin/lsof'
    result = subprocess.run(
        [lsof, '-nP', f'-iTCP:{selected_port}', '-sTCP:LISTEN', '-Fp'],
        capture_output=True, text=True, timeout=10)
    if result.returncode not in (0, 1):
        raise RuntimeError(f'cannot inspect listener ownership: {result.stderr.strip()}')
    return sorted({int(line[1:]) for line in result.stdout.splitlines() if line.startswith('p')})


def stop(proc, log, native, selected_port, native_kind="bridge"):
    already_exited = proc.poll() is not None
    if not already_exited:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            already_exited = True
    try:
        proc.wait(timeout=25)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=10)
        raise RuntimeError(f'server {proc.pid} did not shut down after SIGTERM')
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', selected_port), timeout=.2):
                time.sleep(.1)
        except ConnectionRefusedError:
            break
    else:
        raise RuntimeError(f'server {proc.pid} left port {selected_port} open after shutdown')
    if already_exited:
        raise RuntimeError(f'server {proc.pid} exited before controlled shutdown ({proc.returncode})')
    if proc.returncode not in (0, 128 + signal.SIGTERM, -signal.SIGTERM):
        raise RuntimeError(f'server {proc.pid} exited unexpectedly after SIGTERM ({proc.returncode})')
    text = log.read_text(errors='replace')
    marker = 'Embedded Spark Engine shutdown complete' if native_kind == 'engine' else 'Embedded Spark Connect shutdown complete'
    if native and re.search(r'WARNING in native method|FATAL ERROR in native method', text):
        raise RuntimeError(f'JNI checking failed: {log}')
    if native and 'Java engine handle cleanup failed:' in text:
        raise RuntimeError(f'Java engine cleanup failed: {log}')
    if native and marker not in text:
        raise RuntimeError(f'native shutdown completion missing: {log}')


def run_fork(args, output, mode, pair, order, workload_args):
    name = f'pair{pair}-{mode}'
    folder = output / name
    folder.mkdir()
    for part in ('conf', 'local', 'logs', 'warehouse'):
        (folder / part).mkdir()
    # Fixed logging configuration keeps Spark task logs outside timed client output.
    (folder / 'conf/log4j2.properties').write_text('''status = error
name = Benchmark
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %p %c{1}: %m%n
rootLogger.level = warn
rootLogger.appenderRef.console.ref = console
''')
    selected_port = port()
    env = dict(os.environ, JAVA_HOME=str(args.java_home), SPARK_HOME=str(args.spark_home),
               SPARK_CONF_DIR=str(folder / 'conf'), SPARK_LOG_DIR=str(folder / 'logs'),
               SPARK_LOCAL_DIRS=str(folder / 'local'), SPARK_LOCAL_IP='127.0.0.1',
               SPARK_CONNECT_MODE='0', PYSPARK_PYTHON=str(args.python))
    # Do not inherit JVM debug flags such as -Xcheck:jni into timed runs.
    for key in ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS',
                'SPARK_SUBMIT_OPTS', 'SPARK_REMOTE', 'SPARK_API_MODE'):
        env.pop(key, None)
    klass = ('org.apache.spark.sql.connect.service.SparkConnectServer' if mode == 'jvm'
             else ('org.apache.spark.sql.connect.nativebridge.engine.EmbeddedSparkEngine'
                   if args.native_kind == 'engine' else 'org.apache.spark.sql.connect.nativebridge.EmbeddedSparkConnect'))
    configs = {
        'spark.connect.grpc.binding.address': '127.0.0.1',
        'spark.connect.grpc.binding.port': str(selected_port),
        'spark.connect.grpc.port.maxRetries': '0',
        'spark.driver.bindAddress': '127.0.0.1', 'spark.driver.host': '127.0.0.1',
        'spark.ui.enabled': 'false', 'spark.sql.shuffle.partitions': '2',
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
        'spark.connect.grpc.arrow.maxBatchSize': '4m',
        'spark.sql.artifact.isolation.enabled': 'true',
        'spark.sql.artifact.isolation.alwaysApplyClassloader': 'true',
        'spark.sql.warehouse.dir': str(folder / 'warehouse'),
        'spark.pyspark.python': str(args.python),
    }
    cmd = [str(args.spark_home / 'bin/spark-submit'), '--class', klass, '--master', 'local[2]',
           '--driver-memory', '1g', '--name', name]
    if args.xcheck_jni:
        cmd += ['--driver-java-options', '-Xcheck:jni']
    for key, value in configs.items():
        cmd += ['--conf', f'{key}={value}']
    cmd += (['spark-internal'] if mode == 'jvm' else
            [str(args.jar), str(args.library), f'127.0.0.1:{selected_port}'])
    record = {'mode': mode, 'fork_id': name, 'pair_id': f'pair{pair}', 'order': order,
              'client_json': f'{name}/client.json', 'command': cmd, 'spark_configs': configs,
              'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'load_before': os.getloadavg()}
    log = folder / 'server.log'
    with log.open('w') as out:
        proc = subprocess.Popen(cmd, cwd=folder, env=env, stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True)
        record['pid'] = proc.pid
        failure = None
        try:
            ready = False
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f'server exited ({proc.returncode}): {log}')
                log_text = log.read_text(errors='replace')
                if mode != 'jvm':
                    listening = f'Embedded Spark Connect listening on 127.0.0.1:{selected_port}'
                    identity = f'Embedded Spark Connect JVM PID {proc.pid}; Java Connect listener absent'
                    if listening not in log_text or identity not in log_text:
                        time.sleep(.1)
                        continue
                try:
                    with socket.create_connection(('127.0.0.1', selected_port), timeout=.2):
                        ready = True
                        break
                except OSError:
                    time.sleep(.1)
            if not ready:
                raise RuntimeError(f'server readiness timeout: {log}')
            owners = listener_owners(selected_port)
            record['listener_owner_pids'] = owners
            if owners != [proc.pid]:
                raise RuntimeError(f'listener owner differs from Spark launcher PID: {owners} != {[proc.pid]}')
            if mode != 'jvm':
                identity = re.search(r'Embedded Spark Connect JVM PID (\d+); Java Connect listener absent',
                                     log.read_text(errors='replace'))
                if identity is None or int(identity.group(1)) != proc.pid:
                    raise RuntimeError('native listener did not confirm JVM identity and absent Java listener')
                record['reported_jvm_pid'] = int(identity.group(1))
            record['server_before'] = proc_stats(proc.pid)
            client_cmd = [str(args.python), str(args.client_snapshot), '--remote',
                          f'sc://127.0.0.1:{selected_port}', '--mode',
                          'jvm-grpc' if mode == 'jvm' else 'rust-jni', '--output',
                          str(folder / 'client.json'), '--fork-id', name, '--pair-id', f'pair{pair}',
                          '--order-index', str(order)] + workload_args
            record['client_command'] = client_cmd
            with (folder / 'client.log').open('w') as client_log:
                result = subprocess.run(client_cmd, cwd=ROOT, env=env, stdout=client_log,
                                        stderr=subprocess.STDOUT, timeout=args.client_timeout)
            if args.engine_acceptance and mode == 'jni' and result.returncode == 0:
                acceptance_cmd = [str(args.python), str(ROOT / 'jvm/engine_acceptance.py'),
                                  '--remote', f'sc://127.0.0.1:{selected_port}',
                                  '--output', str(folder / 'engine-acceptance.json')]
                with (folder / 'engine-acceptance.log').open('w') as acceptance_log:
                    acceptance = subprocess.run(acceptance_cmd, cwd=ROOT, env=env,
                                                stdout=acceptance_log, stderr=subprocess.STDOUT, timeout=240)
                record['engine_acceptance_exit_code'] = acceptance.returncode
                if acceptance.returncode:
                    raise RuntimeError(f'engine acceptance failed: {folder / "engine-acceptance.log"}')
            if args.rust_example and mode == 'jni' and result.returncode == 0:
                rust_env = dict(env, SPARK_REMOTE=f'sc://127.0.0.1:{selected_port}')
                with (folder / 'rust-client.log').open('w') as rust_log:
                    rust = subprocess.run([str(args.rust_example)], cwd=ROOT, env=rust_env,
                                          stdout=rust_log, stderr=subprocess.STDOUT, timeout=120)
                record['rust_client_exit_code'] = rust.returncode
                if rust.returncode:
                    raise RuntimeError(f'official Rust client failed: {folder / "rust-client.log"}')
            record['client_exit_code'] = result.returncode
            record['server_after'] = proc_stats(proc.pid)
            record['load_after'] = os.getloadavg()
            if result.returncode:
                raise RuntimeError(f'client failed ({result.returncode}): {folder / "client.log"}')
            if proc.poll() is not None:
                raise RuntimeError(f'server exited during client measurements ({proc.returncode})')
        except BaseException as error:
            failure = error
            record['error'] = repr(error)
            raise
        finally:
            try:
                stop(proc, log, mode != 'jvm', selected_port, args.native_kind)
                record['clean_shutdown'] = True
                record['listener_closed'] = True
            except Exception as error:
                record['clean_shutdown'] = False
                record['shutdown_error'] = str(error)
                if failure is None:
                    raise
            finally:
                record['server_exit_code'] = proc.returncode
                save(folder / 'fork.json', record)
    return record


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--spark-home', type=Path, default=os.environ.get('SPARK_HOME') or None,
                        help='Spark installation; defaults to SPARK_HOME')
    parser.add_argument('--java-home', type=Path, default=os.environ.get('JAVA_HOME') or None,
                        help='JDK installation; defaults to JAVA_HOME')
    parser.add_argument('--python', type=Path, default=Path(sys.executable))
    native_suffix = '.dylib' if sys.platform == 'darwin' else '.so'
    parser.add_argument('--library', type=Path, default=ROOT / ('target/release/libspark_connect_jni' + native_suffix))
    parser.add_argument('--jar', type=Path, default=ROOT / '.build/jvm/spark-connect-native-bridge.jar')
    parser.add_argument('--engine-acceptance', action='store_true')
    parser.add_argument('--rust-example', type=Path)
    parser.add_argument('--native-kind', choices=['bridge', 'engine'], default='bridge')
    parser.add_argument('--xcheck-jni', action='store_true', help='acceptance only; exclude from performance measurements')
    parser.add_argument('--pairs', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--client-timeout', type=int, default=1200)
    args, workload_args = parser.parse_known_args(argv)
    for name, variable in (('spark_home', 'SPARK_HOME'), ('java_home', 'JAVA_HOME')):
        if getattr(args, name) is None:
            parser.error('--' + name.replace('_', '-') + ' or ' + variable + ' must be set')
    # Resolving a venv's python symlink selects the base interpreter and loses
    # the environment containing grpc/PySpark. Preserve the supplied entry point.
    args.python = args.python.expanduser().absolute()
    if args.rust_example:
        args.rust_example = args.rust_example.expanduser().resolve()
    for name in ('output', 'spark_home', 'java_home', 'library', 'jar'):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    return args, workload_args


def main():
    args, workload_args = parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.client_snapshot = args.output / 'client.py'
    args.client_snapshot.write_bytes((ROOT / 'benchmarks/client.py').read_bytes())
    (args.output / 'runner.py').write_bytes(Path(__file__).read_bytes())
    manifest = {'metadata': {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                            'platform': platform.platform(), 'machine': platform.machine(),
                            'cpu_count': os.cpu_count(), 'library': str(args.library),
                            'library_sha256': sha(args.library), 'jar_sha256': sha(args.jar),
                            'spark_home': str(args.spark_home), 'java_home': str(args.java_home),
                            'client_python': str(args.python), 'workload_args': workload_args,
                            'pair_orders': ['AB', 'BA', 'BA', 'AB'][:args.pairs],
                            'A': 'jvm', 'B': 'jni', 'native_architecture': ('typed Engine/Catalyst' if args.native_kind == 'engine' else 'RPC handler bridge'),
                            'xcheck_jni': args.xcheck_jni, 'client_sha256': sha(args.client_snapshot),
                            'runner_sha256': sha(Path(__file__))}, 'runs': []}
    save(args.output / 'manifest.json', manifest)
    try:
        pairs = [('jvm', 'jni'), ('jni', 'jvm'), ('jni', 'jvm'), ('jvm', 'jni')]
        for pair, modes in enumerate(pairs[:args.pairs], 1):
            for order, mode in enumerate(modes):
                print(f'Running pair {pair} {mode}', flush=True)
                try:
                    record = run_fork(args, args.output, mode, pair, order, workload_args)
                except BaseException as error:
                    fork_path = args.output / f'pair{pair}-{mode}' / 'fork.json'
                    record = json.loads(fork_path.read_text()) if fork_path.exists() else {
                        'mode': mode, 'fork_id': f'pair{pair}-{mode}', 'pair_id': f'pair{pair}',
                        'order': order, 'client_json': f'pair{pair}-{mode}/client.json',
                        'error': repr(error), 'clean_shutdown': False}
                    manifest['runs'].append(record)
                    raise
                manifest['runs'].append(record)
                save(args.output / 'manifest.json', manifest)
        manifest['complete'] = True
    except BaseException as error:
        manifest['complete'] = False
        manifest['error'] = str(error)
        raise
    finally:
        save(args.output / 'manifest.json', manifest)


if __name__ == '__main__':
    main()
