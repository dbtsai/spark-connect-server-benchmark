#!/usr/bin/env python3
"""Summarize matched Spark JVM gRPC / Rust JNI benchmark forks.

Usage: python3 benchmarks/report.py RUN_DIRECTORY [--plot]

The run directory contains manifest.json and the client JSON files named by its
runs. Standard-library reporting is always available; --plot uses matplotlib
when installed. Invalid or incomplete comparisons produce reports and exit 1.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics


MODE_NAMES = {"jvm": "jvm", "jvm-grpc": "jvm", "jni": "jni", "rust-jni": "jni"}


def number(value, label, *, positive=False, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number")
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f"{label} must be finite and {'positive' if positive else 'nonnegative'}")
    if integer and int(value) != value:
        raise ValueError(f"{label} must be an integer")
    return value


def nearest_rank(values, quantile):
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def ratio_summary(pairs, field):
    values = [pair[field] for pair in pairs]
    if any(value is None for value in values):
        return None
    return {
        "ratios": [{"pair_id": pair["pair_id"], "jni_over_jvm": pair[field]} for pair in pairs],
        "geometric_mean": math.exp(statistics.mean(math.log(value) for value in values)),
        "min": min(values),
        "max": max(values),
    }


def summarize_workload(samples):
    elapsed = [sample["elapsed_ns"] for sample in samples]
    latencies = []
    for sample in samples:
        # Concurrent rounds have one elapsed interval but multiple RPC latencies.
        latencies.extend(sample.get("call_latencies_ns") or [sample["elapsed_ns"]])
    total_ns = sum(elapsed)
    cpu = [sample.get("client_process_cpu_ns") for sample in samples]
    arrow = [sample.get("arrow_ipc_bytes") for sample in samples]
    rows = [sample.get("rows") for sample in samples]
    messages = [sample.get("response_count") for sample in samples]
    first_batch = [sample["first_batch_ns"] for sample in samples if sample.get("first_batch_ns") is not None]
    total_cpu = sum(cpu) if all(value is not None for value in cpu) else None
    total_arrow = sum(arrow) if all(value is not None for value in arrow) else None
    total_rows = sum(rows) if all(value is not None for value in rows) else None
    total_calls = sum(sample.get("calls", 1) for sample in samples)
    seconds = total_ns / 1e9
    halfway = len(latencies) // 2
    drift = None
    if len(latencies) >= 4:
        drift = statistics.median(latencies[halfway:]) / statistics.median(latencies[:halfway])
    return {
        "measured_sample_count": len(samples),
        "latency_observation_count": len(latencies),
        "calls": total_calls,
        "latency_basis": "per_call" if any(sample.get("call_latencies_ns") for sample in samples) else "per_sample",
        "median_ms": statistics.median(latencies) / 1e6,
        "p95_ms": nearest_rank(latencies, 0.95) / 1e6,
        "min_ms": min(latencies) / 1e6,
        "max_ms": max(latencies) / 1e6,
        "total_elapsed_s": seconds,
        "operations_per_s": total_calls / seconds,
        "arrow_ipc_bytes": total_arrow,
        "arrow_mib_per_s": total_arrow / (1024 * 1024) / seconds if total_arrow else None,
        "rows": total_rows,
        "rows_per_s": total_rows / seconds if total_rows is not None else None,
        "response_count": sum(messages) if all(value is not None for value in messages) else None,
        "client_cpu_s": total_cpu / 1e9 if total_cpu is not None else None,
        "client_cpu_wall_ratio": total_cpu / total_ns if total_cpu is not None else None,
        "first_batch_median_ms": statistics.median(first_batch) / 1e6 if first_batch else None,
        "last_half_over_first_half_median": drift,
    }


def validate_sample(sample, label):
    if not isinstance(sample, dict):
        raise ValueError(f"{label} must be an object")
    if not isinstance(sample.get("workload"), str) or not sample["workload"]:
        raise ValueError(f"{label} has no workload name")
    if sample.get("phase") not in ("warmup", "measured"):
        raise ValueError(f"{label} has an unknown phase")
    if sample.get("passed") is not True:
        raise ValueError(f"{label} failed: {sample.get('error', 'passed is not true')}")
    number(sample.get("elapsed_ns"), f"{label}.elapsed_ns", positive=True, integer=True)
    for field in ("client_process_cpu_ns", "first_batch_ns", "rows", "arrow_ipc_bytes", "response_count"):
        if sample.get(field) is not None:
            number(sample[field], f"{label}.{field}", integer=True)
    calls = number(sample.get("calls", 1), f"{label}.calls", positive=True, integer=True)
    if sample.get("call_latencies_ns") is not None:
        values = sample["call_latencies_ns"]
        if not isinstance(values, list) or len(values) != calls:
            raise ValueError(f"{label}.call_latencies_ns must contain one duration per call")
        for value in values:
            number(value, f"{label}.call_latencies_ns", positive=True, integer=True)
    elif calls != 1:
        raise ValueError(f"{label} has multiple calls without per-call latencies")


def load_fork(directory, record, index):
    label = f"run {index}"
    if not isinstance(record, dict):
        raise ValueError(f"{label} must be an object")
    for key in ("fork_id", "pair_id", "client_json"):
        if not isinstance(record.get(key), str) or not record[key]:
            raise ValueError(f"{label} has no {key}")
    label = record["fork_id"]
    mode = MODE_NAMES.get(record.get("mode"))
    if mode is None:
        raise ValueError(f"{label} has an unknown mode")
    if record.get("client_exit_code", 0) != 0:
        raise ValueError(f"{label} client failed with exit code {record['client_exit_code']}")
    if record.get("clean_shutdown") is False:
        raise ValueError(f"{label} server did not shut down cleanly")
    path = directory / record["client_json"]
    raw = path.read_bytes()
    client = json.loads(raw)
    if not isinstance(client, dict):
        raise ValueError(f"{label} client JSON must be an object")
    if client.get("passed") is not True:
        details = client.get("error") or client.get("errors")
        if not details:
            details = [sample.get("error") or sample.get("cleanup_error") or "failed sample"
                       for sample in client.get("samples", [])
                       if isinstance(sample, dict) and sample.get("passed") is False]
        if not details and isinstance(client.get("cleanup"), list):
            details = [item for item in client["cleanup"]
                       if isinstance(item, dict) and item.get("passed") is False]
        raise ValueError(f"{label} client run failed: {details or 'passed is not true'}")
    for key in ("fork_id", "pair_id"):
        if key in client and client[key] != record[key]:
            raise ValueError(f"{label} client {key} differs from manifest")
    if "mode" in client and MODE_NAMES.get(client["mode"]) != mode:
        raise ValueError(f"{label} client mode differs from manifest")
    if "order_index" in client and "order" in record and client["order_index"] != record["order"]:
        raise ValueError(f"{label} client order differs from manifest")
    cleanup = client.get("cleanup")
    if cleanup is False or (isinstance(cleanup, dict) and (cleanup.get("passed") is False or cleanup.get("error"))):
        raise ValueError(f"{label} client cleanup failed: {cleanup}")
    samples = client.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError(f"{label} has no samples")
    measured = defaultdict(list)
    warmups = defaultdict(int)
    for sample_index, sample in enumerate(samples):
        validate_sample(sample, f"{label} sample {sample_index}")
        if sample["phase"] == "measured":
            measured[sample["workload"]].append(sample)
        else:
            warmups[sample["workload"]] += 1
    if not measured:
        raise ValueError(f"{label} has no measured samples")
    missing = set(warmups) - set(measured)
    if missing:
        raise ValueError(f"{label} has warmups without measurements: {sorted(missing)}")
    workloads = {name: summarize_workload(values) for name, values in sorted(measured.items())}
    for name, value in workloads.items():
        value["warmup_sample_count"] = warmups[name]
    return {
        "fork_id": record["fork_id"],
        "pair_id": record["pair_id"],
        "mode": mode,
        "order": record.get("order"),
        "client_json": record["client_json"],
        "client_json_sha256": hashlib.sha256(raw).hexdigest(),
        "settings": client.get("settings"),
        "client_versions": client.get("client_versions"),
        "workloads": workloads,
    }


def build_summary(directory):
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict) or not isinstance(manifest.get("runs"), list):
        raise ValueError("manifest must contain a runs array")
    summary = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "metadata": manifest.get("metadata", {}),
        "methodology": {
            "independent_unit": "matched pair of fresh JVM forks",
            "ratio": "JNI / JVM; lower favors JNI for latency, higher favors JNI for rates",
            "latency": "median of measured workload invocations, or concurrent per-RPC latencies, within each fork",
            "p95": "nearest-rank percentile within each fork; descriptive only",
            "arrow_throughput": "total measured Arrow IPC bytes / total measured elapsed seconds / 2^20",
            "cross_pair_aggregation": "geometric mean and range of paired fork-summary ratios",
            "significance": "No pooled RPC inference, significance test, or confidence interval",
        },
        "forks": [],
        "comparisons": [],
        "errors": [],
        "warnings": [],
    }
    errors, warnings = summary["errors"], summary["warnings"]
    if manifest.get("complete") is False:
        errors.append(f"manifest is incomplete: {manifest.get('error', 'no completion recorded')}")
    elif manifest.get("complete") is not True:
        warnings.append("Manifest has no explicit complete=true marker.")
    if not manifest["runs"]:
        errors.append("manifest contains no runs")
    seen_forks = set()
    pairs = defaultdict(dict)
    expected_workloads = None
    settings = None
    versions = None
    for index, record in enumerate(manifest["runs"]):
        try:
            fork = load_fork(directory, record, index)
        except (OSError, ValueError, TypeError) as error:
            errors.append(str(error))
            continue
        summary["forks"].append(fork)
        if fork["fork_id"] in seen_forks:
            errors.append(f"duplicate fork ID: {fork['fork_id']}")
        seen_forks.add(fork["fork_id"])
        if fork["mode"] in pairs[fork["pair_id"]]:
            errors.append(f"duplicate {fork['mode']} fork in {fork['pair_id']}")
        pairs[fork["pair_id"]][fork["mode"]] = fork
        names = set(fork["workloads"])
        if expected_workloads is None:
            expected_workloads = names
            settings, versions = fork["settings"], fork["client_versions"]
        else:
            if names != expected_workloads:
                errors.append(f"{fork['fork_id']} workload set differs from other forks")
            if fork["settings"] != settings:
                errors.append(f"{fork['fork_id']} client settings differ from other forks")
            if fork["client_versions"] != versions:
                errors.append(f"{fork['fork_id']} client versions differ from other forks")
    expected_pairs = summary["metadata"].get("pair_orders") if isinstance(summary["metadata"], dict) else None
    if isinstance(expected_pairs, list) and len(pairs) != len(expected_pairs):
        errors.append(f"expected {len(expected_pairs)} pairs, found {len(pairs)}")
    for pair_id, modes in pairs.items():
        if set(modes) != {"jvm", "jni"}:
            errors.append(f"{pair_id} does not have exactly one JVM and one JNI fork")
            continue
        jvm, jni = modes["jvm"], modes["jni"]
        if jvm["order"] is not None and jni["order"] is not None and {jvm["order"], jni["order"]} != {0, 1}:
            errors.append(f"{pair_id} fork order must contain 0 and 1")
        for name in set(jvm["workloads"]) & set(jni["workloads"]):
            a, b = jvm["workloads"][name], jni["workloads"][name]
            for field in (
                "measured_sample_count", "warmup_sample_count", "calls",
                "latency_observation_count", "rows", "latency_basis",
            ):
                if a[field] != b[field]:
                    errors.append(f"{pair_id}/{name} {field} mismatch: JVM={a[field]}, JNI={b[field]}")
            if a["arrow_ipc_bytes"] != b["arrow_ipc_bytes"]:
                warnings.append(f"{pair_id}/{name} Arrow IPC byte counts differ; rates use each fork's actual bytes.")
    summary["pair_count"] = len(pairs)
    if len(pairs) < 4:
        warnings.append(f"Only {len(pairs)} matched pairs were supplied; treat results as exploratory.")
    warnings.append(
        "Sequential RPCs within a JVM are correlated; per-fork p95 values are "
        "descriptive, not independent replications.")
    warnings.append(
        "Arrow output rates include Spark execution and client consumption, "
        "not isolated transport bandwidth.")
    if any(value["client_cpu_wall_ratio"] is not None and value["client_cpu_wall_ratio"] >= 0.8
           for fork in summary["forks"] for value in fork["workloads"].values()):
        warnings.append(
            "Some client CPU/wall ratios are at least 0.8; client work may hide "
            "server differences. This ratio alone does not establish a client bottleneck.")
    if not errors:
        for name in sorted(expected_workloads or []):
            matched = []
            for pair_id, modes in pairs.items():
                a = modes["jvm"]["workloads"][name]
                b = modes["jni"]["workloads"][name]
                matched.append({
                    "pair_id": pair_id,
                    "jvm_fork_id": modes["jvm"]["fork_id"],
                    "jni_fork_id": modes["jni"]["fork_id"],
                    "jvm": a,
                    "jni": b,
                    "latency_ratio": b["median_ms"] / a["median_ms"],
                    "operation_rate_ratio": b["operations_per_s"] / a["operations_per_s"],
                    "arrow_rate_ratio": (
                        b["arrow_mib_per_s"] / a["arrow_mib_per_s"]
                        if a["arrow_mib_per_s"] and b["arrow_mib_per_s"] else None),
                })
            summary["comparisons"].append({
                "workload": name,
                "pair_count": len(matched),
                "pairs": matched,
                "latency_ratio": ratio_summary(matched, "latency_ratio"),
                "operation_rate_ratio": ratio_summary(matched, "operation_rate_ratio"),
                "arrow_rate_ratio": ratio_summary(matched, "arrow_rate_ratio"),
            })
    summary["valid"] = not errors
    return summary


def formatted(value, digits=3):
    return "—" if value is None else f"{value:.{digits}f}"


def escaped(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def ratio_text(value):
    if value is None:
        return "—"
    return f"{value['geometric_mean']:.3f} [{value['min']:.3f}, {value['max']:.3f}]"


def markdown(summary):
    architecture = summary.get("metadata", {}).get("native_architecture", "unspecified JNI backend")
    lines = ["# JVM gRPC and Rust JNI benchmark", "", f"Native architecture: **{escaped(architecture)}**. Reference: standard JVM Spark Connect gRPC.", ""]
    if not summary["valid"]:
        lines += ["**Invalid comparison.** Fix the input errors before interpreting performance ratios.", ""]
        lines += [f"- {escaped(error)}" for error in summary["errors"]]
        lines.append("")
        if summary["forks"]:
            lines += ["## Loaded observations", "",
                      "These counts are diagnostic only; comparative ratios are withheld because validation failed.", "",
                      "| Fork | Pair | Mode | Workload | Measured rounds | Calls | Median / p95 ms | Rows | Arrow bytes |",
                      "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |"]
            for fork in summary["forks"]:
                for name, value in fork["workloads"].items():
                    lines.append(f"| {escaped(fork['fork_id'])} | {escaped(fork['pair_id'])} | {fork['mode']} | {escaped(name)} | {value['measured_sample_count']} | {value['calls']} | {value['median_ms']:.3f} / {value['p95_ms']:.3f} | {value['rows']} | {value['arrow_ipc_bytes']} |")
            lines.append("")
    else:
        lines += [f"{summary['pair_count']} matched pairs of fresh JVM forks. All ratios are **JNI / JVM**: lower favors JNI for latency; higher favors JNI for rates.", "",
                  "Per-fork latency medians are compared within each pair. The table reports the geometric mean and range of those paired ratios; individual RPCs are not pooled across forks.", "",
                  "| Workload | Pairs | Median latency ratio GM [range] | Arrow MiB/s ratio GM [range] | Operations/s ratio GM [range] |",
                  "| --- | ---: | ---: | ---: | ---: |"]
        for comparison in summary["comparisons"]:
            lines.append(f"| {escaped(comparison['workload'])} | {comparison['pair_count']} | {ratio_text(comparison['latency_ratio'])} | {ratio_text(comparison['arrow_rate_ratio'])} | {ratio_text(comparison['operation_rate_ratio'])} |")
        lines += ["", "## Matched pairs", "",
                  "`n` is the number of measured rounds. `calls` counts Config RPCs for concurrent rounds and workload invocations otherwise; a PySpark query invocation may issue several RPCs. Concurrent rounds contribute individual call latencies to that fork's median and p95. p95 uses nearest rank and is descriptive, especially for small samples.", "",
                  "| Workload | Pair | JVM n / calls | JNI n / calls | JVM median / p95 ms | JNI median / p95 ms | Latency ratio | JVM / JNI Arrow MiB/s | JVM / JNI client CPU:wall |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for comparison in summary["comparisons"]:
            for pair in comparison["pairs"]:
                a, b = pair["jvm"], pair["jni"]
                lines.append(f"| {escaped(comparison['workload'])} | {escaped(pair['pair_id'])} | {a['measured_sample_count']} / {a['calls']} | {b['measured_sample_count']} / {b['calls']} | {a['median_ms']:.3f} / {a['p95_ms']:.3f} | {b['median_ms']:.3f} / {b['p95_ms']:.3f} | {pair['latency_ratio']:.3f} | {formatted(a['arrow_mib_per_s'])} / {formatted(b['arrow_mib_per_s'])} | {formatted(a['client_cpu_wall_ratio'])} / {formatted(b['client_cpu_wall_ratio'])} |")
    lines += ["", "## Scope and limitations", ""]
    lines += [f"- {escaped(warning)}" for warning in summary["warnings"]]
    lines += [f"- {escaped(note)}" for note in summary.get("metadata", {}).get("benchmark_notes", [])]
    lines += ["- These are measurements of this host, build, workload, and client. They do not establish a universal speedup or full Spark compatibility.",
              "- No significance test or confidence interval is computed from pooled RPC samples.", "",
              "## Reproducibility", "", "The manifest and client JSON files remain the raw evidence. `summary.json` retains per-fork summaries, sample counts, paired ratios, source hashes, and time-order drift summaries.", "",
              "```json", json.dumps(summary.get("metadata", {}), indent=2), "```", ""]
    if summary.get("plot"):
        lines += [f"![Paired fork ratios]({summary['plot']})", ""]
    return "\n".join(lines)


def plot(summary, directory):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        summary["warnings"].append("matplotlib is unavailable; optional plot skipped.")
        return
    metrics = [("latency_ratio", "JNI / JVM median latency\nLower favors JNI")]
    if any(value["arrow_rate_ratio"] is not None for value in summary["comparisons"]):
        metrics.append(("arrow_rate_ratio", "JNI / JVM Arrow IPC MiB/s\nHigher favors JNI"))
    figure, axes = plt.subplots(1, len(metrics), figsize=(6 * len(metrics), max(4, 0.55 * len(summary["comparisons"]))), squeeze=False)
    for axis, (field, title) in zip(axes[0], metrics):
        comparisons = [value for value in summary["comparisons"] if value[field] is not None]
        for index, value in enumerate(comparisons):
            ratios = value[field]["ratios"]
            offsets = [(position - (len(ratios) - 1) / 2) * 0.055 for position in range(len(ratios))]
            axis.scatter([item["jni_over_jvm"] for item in ratios], [index + offset for offset in offsets], color="tab:blue", alpha=0.7)
            axis.scatter(value[field]["geometric_mean"], index, marker="D", color="black", s=35)
        axis.axvline(1, color="gray", linestyle="--", linewidth=1)
        axis.set_xscale("log")
        axis.set_yticks(range(len(comparisons)), [value["workload"] for value in comparisons])
        axis.invert_yaxis()
        axis.set_title(title)
        axis.grid(axis="x", alpha=0.2)
    figure.suptitle("Each dot is one matched fork pair; diamond is geometric mean")
    figure.tight_layout()
    path = directory / "paired-ratios.png"
    figure.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(figure)
    summary["plot"] = path.name


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("--plot", action="store_true", help="also plot paired ratios if matplotlib is installed")
    args = parser.parse_args()
    directory = args.run_directory.resolve(strict=True)
    try:
        summary = build_summary(directory)
    except (OSError, ValueError, TypeError) as error:
        summary = {"schema_version": 1, "valid": False, "errors": [str(error)], "warnings": [], "forks": [], "comparisons": []}
    if args.plot and summary["valid"] and summary["comparisons"]:
        plot(summary, directory)
    write_json(directory / "summary.json", summary)
    (directory / "REPORT.md").write_text(markdown(summary))
    print(json.dumps({"valid": summary["valid"], "summary": str(directory / "summary.json"), "report": str(directory / "REPORT.md"), "errors": summary["errors"]}))
    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
