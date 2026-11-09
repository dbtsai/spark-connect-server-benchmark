#!/usr/bin/env python3
"""Compare frozen Rust/JVM engine builds using the unchanged protocol benchmark."""

import argparse
from collections import defaultdict
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
KEY = ("case", "workload", "session_model", "channel_model", "concurrency")
METRICS = {
    "median_latency": "median_ms", "p95_latency": "p95_ms", "p99_latency": "p99_ms",
    "throughput": "requests_per_s", "completed_round_throughput": "completed_round_requests_per_s",
}


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def read_json(path):
    value = json.loads(Path(path).read_text())
    json.dumps(value, allow_nan=False)
    return value


def positive(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(label + " must be positive and finite")
    return value


def order(pair, start):
    first = ("baseline", "candidate") if start == "AB" else ("candidate", "baseline")
    return first if pair % 2 else tuple(reversed(first))


def verify(inputs):
    for entry in inputs:
        for name in ("source", "snapshot"):
            if sha(entry[name]) != entry["sha256"]:
                raise ValueError("frozen input changed: " + entry[name])


def freeze(source, destination, inputs):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    entry = {"source": str(source), "snapshot": str(destination), "sha256": sha(destination)}
    if sha(source) != entry["sha256"]:
        raise ValueError("input changed while freezing: " + str(source))
    inputs.append(entry)
    return destination


def aggregate(samples, pairs):
    """Keep fresh forks as independent paired units; never pool variant RPCs."""
    if isinstance(pairs, bool) or not isinstance(pairs, int) or pairs < 1:
        raise ValueError("pairs must be a positive integer")
    indexed = {}
    for sample in samples:
        identity = (sample["pair"], sample["variant"])
        if isinstance(identity[0], bool) or not isinstance(identity[0], int):
            raise ValueError("pair identity must be integral")
        if identity in indexed or identity[1] not in ("baseline", "candidate"):
            raise ValueError("duplicate or unknown variant identity")
        summary = sample["summary"]
        if summary.get("valid") is not True or len(summary.get("forks", [])) != 1:
            raise ValueError("child summary is invalid or does not contain one fresh fork")
        fork = summary["forks"][0]
        if fork.get("target") != "rust-jvm-planning" or fork.get("profile_kind") != "planning":
            raise ValueError("child is not the Rust/JVM planning target")
        if fork.get("non_success_cells"):
            raise ValueError("child has unsupported or failed measured cells")
        indexed[identity] = fork
    expected = {(pair, variant) for pair in range(1, pairs + 1) for variant in ("baseline", "candidate")}
    if set(indexed) != expected:
        raise ValueError("missing or unexpected fresh fork identities")
    grouped = defaultdict(list)
    fixed_settings = None
    fixed_grid = None
    fixed_skips = None
    for pair in range(1, pairs + 1):
        variants = [indexed[pair, variant] for variant in ("baseline", "candidate")]
        maps = []
        for fork in variants:
            settings = {k: v for k, v in fork["settings"].items() if k not in ("endpoint", "output", "started_at")}
            if fixed_settings is None:
                fixed_settings = settings
            if settings != fixed_settings:
                raise ValueError("measurement settings differ across variants or pairs")
            skips = sorted(json.dumps(row, sort_keys=True) for row in fork.get("skipped_cells", []))
            if fixed_skips is None:
                fixed_skips = skips
            if skips != fixed_skips:
                raise ValueError("skipped cell grids or reasons differ")
            mapped = {tuple(group[field] for field in KEY): group for group in fork["groups"]}
            if len(mapped) != len(fork["groups"]) or not mapped:
                raise ValueError("empty or duplicated child cell grid")
            if fixed_grid is None:
                fixed_grid = set(mapped)
            if set(mapped) != fixed_grid:
                raise ValueError("successful cell grids differ across variants or pairs")
            maps.append(mapped)
        for identity in sorted(fixed_grid):
            baseline, candidate = (mapped[identity] for mapped in maps)
            for field in ("requests", "repetitions", "latency_window", "throughput_window", "byte_accounting"):
                if baseline[field] != candidate[field]:
                    raise ValueError("cell counts or timing windows differ")
            rounds = lambda group: [(row["repetition"], row["requests"]) for row in group["rounds"]]
            if rounds(baseline) != rounds(candidate):
                raise ValueError("repetition identities or counts differ")
            values = {}
            for variant, group in (("baseline", baseline), ("candidate", candidate)):
                values[variant] = {metric: positive(group[field], field) for metric, field in METRICS.items()}
                values[variant]["client_cpu_per_request"] = positive(group["client_cpu_ns"], "client_cpu_ns") / positive(group["requests"], "requests")
            ratios = {metric: values["candidate"][metric] / value for metric, value in values["baseline"].items()}
            grouped[identity].append({"pair": pair, **values, "ratios": ratios})
    return [{**dict(zip(KEY, identity)), "pairs": values, "ratios": {
        metric: {"geometric_mean": math.exp(statistics.mean(math.log(pair["ratios"][metric]) for pair in values)),
                 "min": min(pair["ratios"][metric] for pair in values),
                 "max": max(pair["ratios"][metric] for pair in values)}
        for metric in values[0]["ratios"]}} for identity, values in sorted(grouped.items())]


def gate(cells, policy, control=False):
    if not cells:
        raise ValueError("no measured cells to evaluate")
    if policy is None:
        return {"status": "descriptive", "passed": None, "failures": []}
    bounds = {
        "median_latency": ("max_median_latency_ratio", False),
        "throughput": ("min_throughput_ratio", True),
        "p95_latency": ("max_p95_latency_ratio", False),
        "client_cpu_per_request": ("max_client_cpu_per_request_ratio", False),
    }
    if "min_completed_round_throughput_ratio" in policy:
        bounds["completed_round_throughput"] = ("min_completed_round_throughput_ratio", True)
    for key, _ in bounds.values():
        positive(policy[key], key)
    threshold = positive(policy["min_target_throughput_ratio"], "min_target_throughput_ratio")
    minimum = positive(policy["min_improved_targets"], "min_improved_targets")
    if int(minimum) != minimum or threshold <= 1:
        raise ValueError("target improvement count must be integral and throughput ratio must exceed one")
    targets = policy.get("targets")
    if not isinstance(targets, list) or not targets:
        raise ValueError("policy requires declared targets")
    required = set()
    for target in targets:
        if not isinstance(target.get("case"), str) or not isinstance(target.get("concurrency"), list) or not target["concurrency"]:
            raise ValueError("target requires case and concurrency list")
        for workers in target["concurrency"]:
            if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
                raise ValueError("target concurrency must be a positive integer")
            required.add((target["case"], workers))
    available = {(cell["case"], cell["concurrency"]) for cell in cells}
    if not required <= available:
        raise ValueError("policy target is missing from the measured grid")
    all_pairs = policy.get("require_all_pairs_target_improve", False)
    if not isinstance(all_pairs, bool):
        raise ValueError("require_all_pairs_target_improve must be boolean")
    failures, improved_cells, target_results = [], [], defaultdict(list)
    for cell in cells:
        label = "/".join(str(cell[field]) for field in KEY)
        ratios = {metric: value["geometric_mean"] for metric, value in cell["ratios"].items()}
        for metric, (bound, lower) in bounds.items():
            if (ratios[metric] < policy[bound]) if lower else (ratios[metric] > policy[bound]):
                failures.append({"cell": label, "metric": metric, "ratio": ratios[metric], "bound": policy[bound]})
        if (cell["case"], cell["concurrency"]) in required:
            consistent = not all_pairs or all(pair["ratios"]["throughput"] > 1 and pair["ratios"]["median_latency"] <= 1 for pair in cell["pairs"])
            qualifies = ratios["throughput"] >= threshold and ratios["median_latency"] <= 1 and consistent
            target_results[cell["case"], cell["concurrency"]].append(qualifies)
            if qualifies:
                improved_cells.append(label)
    improved = [{"case": case, "concurrency": workers} for (case, workers), values in sorted(target_results.items()) if all(values)]
    if not control and len(improved) < minimum:
        failures.append({"reason": "too few improved target cells", "required": minimum, "actual": len(improved)})
    passed = not failures
    return {"status": ("noise_control_pass" if control else "performance_candidate") if passed else "rejected",
            "passed": passed, "improved_targets": improved, "improved_target_cells": improved_cells, "failures": failures}


def effective_matrix(args):
    matrix = {key: getattr(args, key) for key in ("profile", "requests", "warmups", "repetitions", "max_prepared_mib")}
    matrix["concurrency"] = [int(value.strip()) for value in args.concurrency.split(",")]
    for key in ("session_models", "channel_models"):
        matrix[key] = [value.strip() for value in getattr(args, key).split(",")]
    matrix["case_filter"] = args.case
    return matrix


def validate_matrix(policy, matrix):
    if policy is None or "matrix" not in policy:
        return
    expected = policy["matrix"]
    if not isinstance(expected, dict) or not expected:
        raise ValueError("policy matrix must be a nonempty object")
    if any(key not in matrix or value != matrix[key] for key, value in expected.items()):
        raise ValueError("effective CLI matrix differs from frozen policy")
    if matrix["case_filter"] != expected.get("case_filter"):
        raise ValueError("case filter differs from frozen policy")


def markdown(result):
    lines = ["# Rust/JVM engine build comparison", "", "Status: **" + result["status"] + "**. Ratios are Candidate/Baseline; lower latency/CPU and higher rates favor Candidate.", "",
             "The load generator, protocol runner, reporter, corpus and policy are frozen. Each pair uses separate fresh JVMs in alternating order. Individual pairs remain in `compare.json`; no RPC samples are treated as independent server forks.", ""]
    lines += ["- " + str(error) for error in result["errors"]]
    lines += ["", "| Case | Concurrency | Sessions/channels | Median latency | p95 latency | Primary rate | Completed-round rate | Client CPU/request |",
              "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for cell in result["cells"]:
        values = [f"{cell['ratios'][metric]['geometric_mean']:.3f}" for metric in ("median_latency", "p95_latency", "throughput", "completed_round_throughput", "client_cpu_per_request")]
        lines.append(f"| {cell['case']} | {cell['concurrency']} | {cell['session_model']}/{cell['channel_model']} | " + " | ".join(values) + " |")
    lines += ["", "Guard and target decisions, skipped cells and raw failures are retained. A control skips the improvement requirement and checks guard bounds. A performance_candidate is an exploratory local gate; it still requires independent confirmation and broader correctness/holdout checks.", ""]
    return "\n".join(lines)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runner", type=Path, default=HERE / "run.py")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/spark-connect-protocol-bench")
    for variant in ("baseline", "candidate"):
        parser.add_argument("--" + variant + "-jar", type=Path, required=True)
        parser.add_argument("--" + variant + "-library", type=Path, required=True)
    parser.add_argument("--spark-home", type=Path, default=os.environ.get("SPARK_HOME") or None,
                        help="Spark installation; defaults to SPARK_HOME")
    parser.add_argument("--java-home", type=Path, default=os.environ.get("JAVA_HOME") or None,
                        help="JDK installation; defaults to JAVA_HOME")
    parser.add_argument("--payload-dir", type=Path)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--start-order", choices=("AB", "BA"), default="AB")
    parser.add_argument("--control", action="store_true")
    parser.add_argument("--profile", choices=("smoke", "standard", "stress"), default="smoke")
    for key, default in (("concurrency", "1,8,32"), ("session-models", "shared"), ("channel-models", "shared")):
        parser.add_argument("--" + key, default=default)
    for key, default in (("requests", 128), ("warmups", 64), ("repetitions", 2), ("max-prepared-mib", 60), ("client-threads", 4), ("timeout-seconds", 30), ("client-timeout", 3600), ("startup-timeout", 90)):
        parser.add_argument("--" + key, type=int, default=default)
    parser.add_argument("--case")
    args = parser.parse_args(argv)
    for name, variable in (("spark_home", "SPARK_HOME"), ("java_home", "JAVA_HOME")):
        if getattr(args, name) is None:
            parser.error("--" + name.replace("_", "-") + " or " + variable + " must be set")
    if not 1 <= args.pairs <= 20:
        parser.error("pairs must be 1..20")
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.expanduser().resolve())
    return args


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    result = {"schema_version": 1, "valid": False, "status": "invalid", "control": args.control,
              "pairs_requested": args.pairs, "start_order": args.start_order, "inputs": [], "runs": [], "cells": [], "errors": []}
    samples = []
    try:
        harness = args.output / "harness/benchmarks/protocol"
        runner = freeze(args.runner, harness / "run.py", result["inputs"])
        reporter = freeze(args.runner.parent / "report.py", harness / "report.py", result["inputs"])
        freeze(Path(__file__).resolve(), harness / "compare.py", result["inputs"])
        binary = freeze(args.binary, args.output / "artifacts/loadgen" / args.binary.name, result["inputs"])
        artifacts = {}
        for variant in ("baseline", "candidate"):
            artifacts[variant] = {kind: freeze(getattr(args, variant + "_" + kind), args.output / "artifacts" / variant / getattr(args, variant + "_" + kind).name, result["inputs"]) for kind in ("jar", "library")}
        if args.control and any(sha(artifacts["baseline"][kind]) != sha(artifacts["candidate"][kind]) for kind in ("jar", "library")):
            raise ValueError("noise control requires identical jar and native library bytes")
        policy = None
        if args.policy:
            policy = read_json(freeze(args.policy, args.output / "artifacts/policy.json", result["inputs"]))
        result["policy"] = policy
        result["matrix"] = effective_matrix(args)
        validate_matrix(policy, result["matrix"])
        result["parameters"] = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
        spec = importlib.util.spec_from_file_location("frozen_protocol_runner", runner)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        args.binary = binary
        payloads, hashes = module.prepare_payloads(args, args.output, module.environment(args))
        for name, digest in hashes.items():
            entry = {"source": str(payloads / name), "snapshot": str(payloads / name), "sha256": digest}
            if args.payload_dir:
                entry["source"] = str(args.payload_dir / name)
            result["inputs"].append(entry)
        for pair in range(1, args.pairs + 1):
            for variant in order(pair, args.start_order):
                verify(result["inputs"])
                folder = args.output / f"pair{pair}-{variant}"
                command = [sys.executable, str(runner), "--output", str(folder), "--targets", "rust-jvm-planning", "--forks", "1",
                           "--binary", str(binary), "--jar", str(artifacts[variant]["jar"]), "--library", str(artifacts[variant]["library"]),
                           "--payload-dir", str(payloads), "--spark-home", str(args.spark_home), "--java-home", str(args.java_home)]
                for key in ("profile", "concurrency", "requests", "warmups", "repetitions", "session_models", "channel_models", "max_prepared_mib", "client_threads", "timeout_seconds", "client_timeout", "startup_timeout"):
                    command += ["--" + key.replace("_", "-"), str(getattr(args, key))]
                if args.case:
                    command += ["--case", args.case]
                record = {"pair": pair, "variant": variant, "directory": str(folder), "command": command}
                result["runs"].append(record)
                save(args.output / "compare.json", result)
                print(f"Pair {pair}: {variant}", flush=True)
                with (args.output / f"pair{pair}-{variant}.log").open("w") as log:
                    record["run_exit_code"] = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT).returncode
                    record["report_exit_code"] = subprocess.run([sys.executable, str(reporter), str(folder)], stdout=log, stderr=subprocess.STDOUT).returncode
                verify(result["inputs"])
                if record["run_exit_code"] or record["report_exit_code"]:
                    raise ValueError(f"invalid child {pair}/{variant}; inspect {folder} and its log")
                samples.append({"pair": pair, "variant": variant, "summary": read_json(folder / "summary.json")})
        result["cells"] = aggregate(samples, args.pairs)
        result["decision"] = gate(result["cells"], policy, args.control)
        result.update(valid=True, status=result["decision"]["status"])
    except Exception as error:
        result["errors"].append(repr(error))
    finally:
        try:
            verify(result["inputs"])
        except Exception as error:
            result.update(valid=False, status="invalid")
            result["errors"].append(repr(error))
        save(args.output / "compare.json", result)
        (args.output / "COMPARE.md").write_text(markdown(result))
    return 0 if result["valid"] and result["status"] != "rejected" else 1


if __name__ == "__main__":
    sys.exit(run(parse_args()))
