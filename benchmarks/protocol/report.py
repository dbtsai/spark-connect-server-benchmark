#!/usr/bin/env python3
"""Validate and summarize execution-free Spark Connect benchmark forks."""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

PAIR_TARGETS = (("rust-fixture", "jvm-fixture"), ("rust-jvm-planning", "jvm-planning"))
KEY_FIELDS = ("case", "workload", "session_model", "channel_model", "concurrency")
ROUTES = {"ExecutePlan", "AnalyzePlan", "Config", "AddArtifacts", "ArtifactStatus", "Interrupt",
          "ReattachExecute", "ReleaseExecute", "ReleaseSession", "FetchErrorDetails", "CloneSession", "GetStatus"}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def number(value, name, positive=False, integer=True):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(name + " must be numeric")
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(name + " must be finite and nonnegative" if not positive else name + " must be positive")
    if integer and int(value) != value:
        raise ValueError(name + " must be an integer")
    return value


def nearest_rank(values, quantile):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def validate_cell(cell, label):
    if not isinstance(cell, dict):
        raise ValueError(label + " is not an object")
    for key in KEY_FIELDS[:4]:
        if not isinstance(cell.get(key), str) or not cell[key]:
            raise ValueError(label + " is missing " + key)
    for key in ("concurrency", "requests", "elapsed_ns"):
        number(cell.get(key), label + "." + key, positive=True)
    for key in ("repetition", "successes", "failures", "unsupported", "request_bytes", "response_bytes", "client_cpu_ns"):
        number(cell.get(key), label + "." + key)
    if cell["concurrency"] > cell["requests"]:
        raise ValueError(label + " has fewer requests than workers")
    if cell["successes"] + cell["failures"] + cell["unsupported"] != cell["requests"]:
        raise ValueError(label + " outcome counts do not sum to requests")
    for key in ("latencies_ns", "first_message_latencies_ns"):
        if not isinstance(cell.get(key), list):
            raise ValueError(label + " is missing " + key)
        for value in cell[key]:
            number(value, label + "." + key, positive=True)
    if "first_arrow_latencies_ns" in cell:
        if not isinstance(cell["first_arrow_latencies_ns"], list):
            raise ValueError(label + ".first_arrow_latencies_ns is not an array")
        for value in cell["first_arrow_latencies_ns"]:
            number(value, label + ".first_arrow_latencies_ns", positive=True)
        if len(cell["first_arrow_latencies_ns"]) > cell["requests"]:
            raise ValueError(label + " has more first-Arrow samples than requests")
    if not isinstance(cell.get("status_counts"), dict):
        raise ValueError(label + " status_counts is missing")
    for value in cell["status_counts"].values():
        number(value, label + ".status_counts")
    if sum(cell["status_counts"].values()) != cell["requests"]:
        raise ValueError(label + " status counts do not sum to requests")
    for key in ("validation_errors", "cleanup_errors"):
        if not isinstance(cell.get(key), list):
            raise ValueError(label + " is missing " + key)
    for key in ("latency_window", "throughput_window"):
        if not isinstance(cell.get(key), str) or not cell[key]:
            raise ValueError(label + " is missing its timing-window definition")
    if len(cell["latencies_ns"]) != cell["requests"]:
        raise ValueError(label + " requests do not have one latency each")
    if len(cell["first_message_latencies_ns"]) > cell["requests"]:
        raise ValueError(label + " has more first-message samples than requests")
    for key in ("warmup_validation_errors", "warmup_cleanup_errors"):
        if key in cell and not isinstance(cell[key], list):
            raise ValueError(label + "." + key + " is not an array")
    if "validation_cleanup_ns" in cell:
        number(cell["validation_cleanup_ns"], label + ".validation_cleanup_ns")
    for key in ("preparation_ns", "full_round_elapsed_ns"):
        if key in cell:
            number(cell[key], label + "." + key)
    if "full_round_elapsed_ns" in cell:
        if cell["full_round_elapsed_ns"] < cell["elapsed_ns"]:
            raise ValueError(label + " full round is shorter than its primary window")
        if all(key in cell for key in ("preparation_ns", "validation_cleanup_ns")):
            if cell["full_round_elapsed_ns"] != cell["preparation_ns"] + cell["elapsed_ns"] + cell["validation_cleanup_ns"]:
                raise ValueError(label + " full-round component times do not sum")


def summarize_cells(cells):
    latency = [value for cell in cells for value in cell["latencies_ns"]]
    first = [value for cell in cells for value in cell["first_message_latencies_ns"]]
    first_arrow = [value for cell in cells for value in cell.get("first_arrow_latencies_ns", [])]
    elapsed = sum(cell["elapsed_ns"] for cell in cells)
    requests = sum(cell["requests"] for cell in cells)
    response_bytes = sum(cell["response_bytes"] for cell in cells)
    cpu = sum(cell["client_cpu_ns"] for cell in cells)
    full_elapsed = sum(cell["full_round_elapsed_ns"] for cell in cells) if all("full_round_elapsed_ns" in cell for cell in cells) else None
    return {
        "repetitions": len(cells), "requests": requests, "elapsed_ns": elapsed,
        "median_ms": statistics.median(latency) / 1e6,
        "p95_ms": nearest_rank(latency, 0.95) / 1e6,
        "p99_ms": nearest_rank(latency, 0.99) / 1e6,
        "first_message_median_ms": statistics.median(first) / 1e6 if first else None,
        "first_arrow_median_ms": statistics.median(first_arrow) / 1e6 if first_arrow else None,
        "requests_per_s": requests * 1e9 / elapsed,
        "completed_round_requests_per_s": requests * 1e9 / full_elapsed if full_elapsed is not None else None,
        "full_round_elapsed_ns": full_elapsed,
        "preparation_ns": sum(cell["preparation_ns"] for cell in cells) if all("preparation_ns" in cell for cell in cells) else None,
        "validation_cleanup_ns": sum(cell["validation_cleanup_ns"] for cell in cells) if all("validation_cleanup_ns" in cell for cell in cells) else None,
        "response_mib_per_s": response_bytes * 1e9 / elapsed / 2**20 if response_bytes else None,
        "request_bytes": sum(cell["request_bytes"] for cell in cells),
        "response_bytes": response_bytes, "client_cpu_ns": cpu,
        "client_cpu_wall_ratio": cpu / elapsed,
        "latency_window": cells[0]["latency_window"],
        "throughput_window": cells[0]["throughput_window"],
        "byte_accounting": cells[0].get("byte_accounting", "protobuf encoded bytes, excluding gRPC/HTTP2 framing"),
        "rounds": [{"repetition": cell["repetition"], "requests": cell["requests"],
                    "elapsed_ns": cell["elapsed_ns"], "median_ms": statistics.median(cell["latencies_ns"]) / 1e6}
                   for cell in sorted(cells, key=lambda value: value["repetition"])],
    }


def coverage_records(value):
    if isinstance(value, dict):
        return value.get("records", value.get("checks", []))
    return value if isinstance(value, list) else []


def normalized_settings(settings):
    return {key: value for key, value in settings.items()
            if key not in ("endpoint", "output", "remote", "mode", "started_at")}


def validate_requested_grid(client, label):
    settings = client["settings"]
    coverage_only = settings.get("coverage_only")
    if not isinstance(coverage_only, bool):
        raise ValueError(label + " lacks explicit coverage_only settings")
    capabilities = client.get("capabilities")
    if not isinstance(capabilities, list):
        raise ValueError(label + " capabilities is not an array")
    cells, skipped = client.get("cells", []), client.get("skipped_cells", [])
    if coverage_only:
        if cells or skipped or capabilities:
            raise ValueError(label + " coverage-only run contains timed work or capabilities")
        return
    if not capabilities:
        raise ValueError(label + " timed run has no capability records")
    repetitions = int(number(settings.get("repetitions"), label + ".settings.repetitions", positive=True))
    requests = number(settings.get("requests"), label + ".settings.requests", positive=True)
    concurrency = settings.get("concurrency")
    if not isinstance(concurrency, list) or not concurrency:
        raise ValueError(label + " settings.concurrency is empty or not an array")
    for value in concurrency:
        number(value, label + ".settings.concurrency", positive=True)
        if value > requests:
            raise ValueError(label + " requested concurrency exceeds requests")
    if len(set(concurrency)) != len(concurrency):
        raise ValueError(label + " settings.concurrency contains duplicates")
    models = []
    for key in ("session_models", "channel_models"):
        values = settings.get(key)
        if not isinstance(values, list) or not values or any(value not in ("shared", "isolated") for value in values):
            raise ValueError(label + ".settings." + key + " is invalid")
        if len(set(values)) != len(values):
            raise ValueError(label + ".settings." + key + " contains duplicates")
        models.append(values)
    cases = {}
    for capability in capabilities:
        if not isinstance(capability, dict):
            raise ValueError(label + " capability is not an object")
        case, rpc = capability.get("case"), capability.get("rpc")
        if not isinstance(case, str) or not case or not isinstance(rpc, str) or not rpc:
            raise ValueError(label + " capability lacks case/rpc identity")
        if case in cases:
            raise ValueError(label + " contains duplicate capability cases")
        if not isinstance(capability.get("supported"), bool):
            raise ValueError(label + " capability lacks explicit support status")
        cases[case] = capability
    expected = {
        (case, capability["rpc"], session, channel, workers, repetition)
        for case, capability in cases.items() if capability["supported"]
        for workers in concurrency for session in models[0] for channel in models[1]
        for repetition in range(repetitions)
    }
    actual = set()
    for cell in cells:
        identity = tuple(cell[field] for field in KEY_FIELDS) + (cell["repetition"],)
        if cell["requests"] != requests:
            raise ValueError(label + " cell requests differ from requested settings")
        actual.add(identity)
    for skip in skipped:
        if not isinstance(skip, dict) or not isinstance(skip.get("reason"), str) or not skip["reason"].strip():
            raise ValueError(label + " skipped cell lacks an explicit reason")
        capability = cases.get(skip.get("case"))
        if capability is None:
            raise ValueError(label + " skipped cell has an unknown capability case")
        for key in ("session_model", "channel_model"):
            if not isinstance(skip.get(key), str):
                raise ValueError(label + " skipped cell lacks " + key)
        number(skip.get("concurrency"), label + ".skipped.concurrency", positive=True)
        number(skip.get("repetition"), label + ".skipped.repetition")
        identity = (skip["case"], capability["rpc"], skip["session_model"], skip["channel_model"],
                    skip["concurrency"], skip["repetition"])
        if identity in actual:
            raise ValueError(label + " contains duplicate or measured-and-skipped cell identities")
        actual.add(identity)
    missing, unexpected = expected - actual, actual - expected
    if missing or unexpected:
        raise ValueError(label + f" requested cell grid differs: {len(missing)} missing, {len(unexpected)} unexpected")


def validate_manifest_settings(settings, metadata, label):
    for key in ("profile", "coverage_only", "requests", "warmups", "repetitions", "client_threads", "case_filter"):
        if key in metadata and settings.get(key) != metadata[key]:
            raise ValueError(label + " client settings differ from manifest request: " + key)
    for key in ("concurrency", "session_models", "channel_models"):
        if key not in metadata:
            continue
        expected = metadata[key].split(",") if isinstance(metadata[key], str) else metadata[key]
        if key == "concurrency":
            expected = [int(value) for value in expected]
        if settings.get(key) != expected:
            raise ValueError(label + " client settings differ from manifest request: " + key)
    if "max_prepared_mib" in metadata and settings.get("max_prepared_bytes") != metadata["max_prepared_mib"] * 2**20:
        raise ValueError(label + " client settings differ from manifest request: max_prepared_bytes")


def load_fork(directory, record):
    if not isinstance(record, dict):
        raise ValueError("fork record is not an object")
    label = record.get("fork_id", "unnamed fork")
    if record.get("clean_shutdown") is not True or record.get("client_exit_code") != 0:
        raise ValueError(label + " failed or did not shut down cleanly; inspect raw " + str(record.get("client_json", "client.json")))
    if record.get("error"):
        raise ValueError(label + " failed: " + record["error"] + "; inspect raw " + str(record.get("client_json", "client.json")))
    relative = Path(record["client_json"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(label + " client path escapes the run directory")
    path = directory / relative
    client = json.loads(path.read_text())
    if client.get("passed") is not True:
        raise ValueError(label + " client validation did not pass; inspect raw " + str(relative))
    if client.get("incomplete") or client.get("in_progress") or client.get("error") or client.get("final_cleanup_errors"):
        raise ValueError(label + " has incomplete work or final cleanup errors")
    settings = client.get("settings")
    if not isinstance(settings, dict) or settings.get("no_execution") is not True:
        raise ValueError(label + " lacks explicit execution-free settings")
    if settings.get("retries") != 0:
        raise ValueError(label + " does not explicitly disable retries")
    if record.get("profile_kind") not in ("fixture", "planning") or client.get("mode") != record["profile_kind"]:
        raise ValueError(label + " client/server profile differs")
    coverage = client.get("coverage", {})
    if isinstance(coverage, dict) and coverage.get("passed") is not True:
        raise ValueError(label + " coverage validation did not pass")
    records = coverage_records(coverage)
    if not records or any(item.get("passed") is not True for item in records):
        raise ValueError(label + " has missing or failing coverage checks")
    tested = {item.get("route", item.get("rpc")) for item in records if item.get("tested", True) is True}
    expected = ROUTES if record["profile_kind"] == "fixture" else {"AnalyzePlan", "Config", "ReleaseSession"}
    if not expected <= tested:
        raise ValueError(label + " lacks required route probes: " + ", ".join(sorted(expected - tested)))
    if record["profile_kind"] == "planning" and tested - expected:
        raise ValueError(label + " planning profile probed a disallowed route")
    shutdown = record.get("shutdown", {})
    if shutdown.get("listener_closed") is not True:
        raise ValueError(label + " lacks closed-listener shutdown evidence")
    if record["profile_kind"] == "fixture":
        if shutdown.get("fixture_stats", {}).get("query_executions") != 0:
            raise ValueError(label + " lacks its no-query fixture audit")
    elif shutdown.get("spark_job_audit", {}).get("jobs_started") != 0:
        raise ValueError(label + " lacks its zero-Spark-jobs audit")
    grouped = defaultdict(list)
    outcomes = []
    seen = set()
    cells = client.get("cells", [])
    if not isinstance(cells, list):
        raise ValueError(label + " cells is not an array")
    for index, cell in enumerate(cells):
        validate_cell(cell, f"{label} cell {index}")
        key = tuple(cell[field] for field in KEY_FIELDS)
        identity = key + (cell["repetition"],)
        if identity in seen:
            raise ValueError(label + " contains duplicate cell/repetition identities")
        seen.add(identity)
        if any(cell.get(key) for key in ("failures", "unsupported", "validation_errors", "cleanup_errors",
                                         "warmup_validation_errors", "warmup_cleanup_errors")):
            outcomes.append({field: cell[field] for field in (*KEY_FIELDS, "repetition", "requests", "successes", "failures",
                            "unsupported", "status_counts", "validation_errors", "cleanup_errors")})
            outcomes[-1].update({field: cell.get(field, []) for field in ("warmup_validation_errors", "warmup_cleanup_errors")})
        else:
            grouped[key].append(cell)
    groups = []
    for key, values in sorted(grouped.items()):
        for field in ("latency_window", "throughput_window", "byte_accounting"):
            if len({cell.get(field) for cell in values}) != 1:
                raise ValueError(label + " mixes timing windows within one cell group")
        if len({"full_round_elapsed_ns" in cell for cell in values}) != 1:
            raise ValueError(label + " mixes available and missing completed-round counters")
        groups.append({**dict(zip(KEY_FIELDS, key)), **summarize_cells(values)})
    skipped = client.get("skipped_cells", [])
    if not isinstance(skipped, list):
        raise ValueError(label + " skipped_cells is not an array")
    validate_requested_grid(client, label)
    return {"fork_id": label, "fork": record["fork"], "target": record["target"],
            "profile_kind": record["profile_kind"], "client_sha256": sha(path),
            "settings": settings, "coverage": coverage, "groups": groups,
            "non_success_cells": outcomes, "skipped_cells": skipped,
            "client_metadata": {key: value for key, value in client.items()
                                if key not in ("cells", "coverage", "settings", "skipped_cells")}}


def build_summary(directory):
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    summary = {"schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
               "manifest_sha256": sha(manifest_path), "analysis_reporter_sha256": sha(Path(__file__)),
               "metadata": manifest.get("metadata", {}),
               "valid": False, "errors": [], "warnings": [], "forks": [], "comparisons": [],
               "methodology": {"independent_unit": "fresh server fork, matched within fork index",
                               "latency": "descriptive per-fork pooled RPC latencies across repetitions of the same cell",
                               "throughput": "successful requests / summed recorded primary-round elapsed time",
                               "completed_round_throughput": "successful requests / summed preparation + primary RPC round + validation/cleanup time; excludes server startup, channel/session establishment, and warmups",
                               "cross_fork_aggregation": "geometric mean and range of matched per-fork ratios",
                               "unsupported": "never pooled with successful throughput or latency",
                               "no_significance_claim": True}}
    errors, warnings = summary["errors"], summary["warnings"]
    if manifest.get("complete") is not True:
        errors.append("run manifest is incomplete: " + str(manifest.get("error", "no complete marker")))
    if not isinstance(manifest.get("runs"), list) or not manifest["runs"]:
        errors.append("manifest contains no forks")
    for artifact in manifest.get("metadata", {}).get("artifacts", {}).values():
        path = directory / "artifacts" / Path(artifact["snapshot"]).name
        if not path.is_file() or sha(path) != artifact["sha256"]:
            errors.append("frozen artifact hash differs: " + path.name)
    for key, name in (("runner_sha256", "runner.py"), ("reporter_sha256", "reporter.py")):
        expected = manifest.get("metadata", {}).get(key)
        if expected:
            path = directory / "artifacts" / name
            if not path.is_file() or sha(path) != expected:
                errors.append("frozen orchestration hash differs: " + name)
    for name, expected in manifest.get("metadata", {}).get("payload_sha256", {}).items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            errors.append("payload hash path escapes the run directory")
            continue
        path = directory / "payloads" / relative
        if not path.is_file() or sha(path) != expected:
            errors.append("frozen corpus hash differs: " + name)
    corpus_path = directory / "payloads/manifest.json"
    if corpus_path.is_file():
        summary["corpus"] = json.loads(corpus_path.read_text())
    seen = set()
    seen_targets = set()
    for record in manifest.get("runs", []):
        try:
            fork = load_fork(directory, record)
            validate_manifest_settings(fork["settings"], manifest.get("metadata", {}), fork["fork_id"])
            if fork["fork_id"] in seen:
                raise ValueError("duplicate fork: " + fork["fork_id"])
            identity = (fork["fork"], fork["target"])
            if identity in seen_targets:
                raise ValueError("duplicate fork/target identity: " + str(identity))
            seen.add(fork["fork_id"])
            seen_targets.add(identity)
            summary["forks"].append(fork)
            bad = [cell for cell in fork["non_success_cells"]
                   if any(cell[key] for key in ("failures", "validation_errors", "cleanup_errors",
                                               "warmup_validation_errors", "warmup_cleanup_errors"))]
            if bad:
                errors.append(fork["fork_id"] + " contains failed cells; excluded from successful comparisons")
            if fork["skipped_cells"]:
                warnings.append(fork["fork_id"] + f" skipped {len(fork['skipped_cells'])} cells; inspect reasons")
        except (OSError, ValueError, KeyError, TypeError) as error:
            message = str(error)
            client_path = record.get("client_json") if isinstance(record, dict) else None
            if client_path and str(client_path) not in message:
                message += "; inspect raw " + str(client_path)
            errors.append(message)
    metadata = manifest.get("metadata", {})
    if isinstance(metadata.get("forks_per_target"), int) and isinstance(metadata.get("targets"), list):
        expected = {(index, target) for index in range(1, metadata["forks_per_target"] + 1)
                    for target in metadata["targets"]}
        actual = {(fork["fork"], fork["target"]) for fork in summary["forks"]}
        if expected != actual:
            errors.append("successful fork identities do not match requested targets/fork count")
    forks = {(fork["fork"], fork["target"]): fork for fork in summary["forks"]}
    comparisons = defaultdict(list)
    for native, reference in PAIR_TARGETS:
        for index in sorted({fork["fork"] for fork in summary["forks"]}):
            left, right = forks.get((index, native)), forks.get((index, reference))
            if left is None or right is None:
                continue
            if normalized_settings(left["settings"]) != normalized_settings(right["settings"]):
                errors.append(f"fork {index} {native}/{reference}: client settings differ")
                continue
            lmap = {tuple(group[field] for field in KEY_FIELDS): group for group in left["groups"]}
            rmap = {tuple(group[field] for field in KEY_FIELDS): group for group in right["groups"]}
            if set(lmap) != set(rmap):
                warnings.append(f"fork {index} {native}/{reference}: {len(set(lmap) ^ set(rmap))} cell groups lack a successful counterpart; no ratios computed for them")
            for key in sorted(set(lmap) & set(rmap)):
                lgroup, rgroup = lmap[key], rmap[key]
                if any(lgroup[field] != rgroup[field] for field in ("requests", "repetitions", "latency_window", "throughput_window", "byte_accounting")):
                    errors.append(f"fork {index} {native}/{reference} {key}: sample counts/windows differ")
                    continue
                if [(value["repetition"], value["requests"]) for value in lgroup["rounds"]] != [(value["repetition"], value["requests"]) for value in rgroup["rounds"]]:
                    errors.append(f"fork {index} {native}/{reference} {key}: repetition identities/counts differ")
                    continue
                pair = {
                    "fork": index, "native_fork": left["fork_id"], "reference_fork": right["fork_id"],
                    "latency_ratio": lgroup["median_ms"] / rgroup["median_ms"],
                    "throughput_ratio": lgroup["requests_per_s"] / rgroup["requests_per_s"],
                    "native_median_ms": lgroup["median_ms"], "reference_median_ms": rgroup["median_ms"],
                    "native_requests_per_s": lgroup["requests_per_s"], "reference_requests_per_s": rgroup["requests_per_s"],
                }
                if lgroup["completed_round_requests_per_s"] is not None and rgroup["completed_round_requests_per_s"] is not None:
                    pair["completed_round_throughput_ratio"] = lgroup["completed_round_requests_per_s"] / rgroup["completed_round_requests_per_s"]
                    pair["native_completed_round_requests_per_s"] = lgroup["completed_round_requests_per_s"]
                    pair["reference_completed_round_requests_per_s"] = rgroup["completed_round_requests_per_s"]
                comparisons[(native, reference, key)].append(pair)
    for (native, reference, key), values in sorted(comparisons.items()):
        item = {"native": native, "reference": reference, **dict(zip(KEY_FIELDS, key)), "pairs": values}
        for field in ("latency_ratio", "throughput_ratio", "completed_round_throughput_ratio"):
            ratios = [value[field] for value in values if field in value]
            if not ratios:
                continue
            item[field] = {"geometric_mean": math.exp(statistics.mean(math.log(value) for value in ratios)),
                           "min": min(ratios), "max": max(ratios), "pairs": len(ratios)}
        summary["comparisons"].append(item)
    if len(summary["forks"]) < 4:
        warnings.append("Few fresh forks: results are smoke/descriptive evidence, not a stable performance estimate.")
    if any(group["client_cpu_wall_ratio"] >= 0.8 for fork in summary["forks"] for group in fork["groups"]):
        warnings.append("Some client CPU/wall ratios are >=0.8; measure client saturation before attributing a server ceiling.")
    if not summary["comparisons"] and any(fork["groups"] for fork in summary["forks"]):
        warnings.append("No matching target pairs: per-fork measurements are available without cross-target ratios.")
    summary["valid"] = not errors
    return summary


def esc(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def ratio(value):
    return f"{value['geometric_mean']:.3f} [{value['min']:.3f}, {value['max']:.3f}]"


def markdown(summary):
    lines = ["# Execution-free Spark Connect concurrency benchmark", "",
             "This suite separates synthetic protocol handling from real-server planning. "
             "It uses the official Rust client's generated Spark Connect protocol; no throughput ratio crosses those two profiles.", "",
             "Status: **" + ("valid" if summary["valid"] else "INVALID / incomplete") + "**. "
             f"Fresh server forks: {len(summary['forks'])}.", ""]
    if summary["errors"]:
        lines += ["## Validation failures", ""] + ["- " + esc(error) for error in summary["errors"]] + [""]
    lines += ["## Matched concurrency cells", "",
              "Ratios are Rust/JVM within the same profile and cell: lower favors Rust for latency, higher favors Rust for request rate. "
              "Each pair uses separate fresh servers. The table shows geometric means and ranges across matched fork pairs; individual RPCs are not independent replications.", "",
              "| Profile comparison | Case / primary RPC | Concurrency | Sessions / channels | Pairs | Median latency ratio GM [range] | Primary request-rate ratio GM [range] | Completed-round rate ratio GM [range] |",
              "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |"]
    for item in summary["comparisons"][:200]:
        lines.append(f"| {esc(item['native'])} / {esc(item['reference'])} | {esc(item['case'])} / {esc(item['workload'])} | "
                     f"{item['concurrency']} | {item['session_model']} / {item['channel_model']} | {len(item['pairs'])} | "
                     f"{ratio(item['latency_ratio'])} | {ratio(item['throughput_ratio'])} | "
                     + (ratio(item["completed_round_throughput_ratio"]) if "completed_round_throughput_ratio" in item else "—") + " |")
    if not summary["comparisons"]:
        lines.append("| No matched successful cells | — | — | — | — | — | — | — |")
    if len(summary["comparisons"]) > 200:
        lines += ["", f"Showing 200 of {len(summary['comparisons'])} matched cell groups; `comparisons.csv` and `summary.json` contain every group."]
    lines += ["", "## Per-fork measurements", "",
              "Only cells with every request successful, correct, and cleaned up appear here. p95/p99 are descriptive within each server fork. "
              "Primary rate uses the client's recorded primary-round window. Completed-round rate additionally includes request/lifecycle preparation and validation/cleanup; it excludes startup, channel/session establishment, and warmups. "
              "The report preserves each exact timing-window definition in `summary.json`.", "",
              "| Fork | Case / RPC | Concurrency | Sessions / channels | Reps / requests | Median / p95 / p99 ms | Primary requests/s | Completed-round requests/s | Response MiB/s | Client CPU:wall |",
              "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    shown = 0
    total_groups = sum(len(fork["groups"]) for fork in summary["forks"])
    for fork in summary["forks"]:
        for group in fork["groups"]:
            if shown >= 200:
                break
            rate = f"{group['response_mib_per_s']:.2f}" if group["response_mib_per_s"] is not None else "—"
            full_rate = f"{group['completed_round_requests_per_s']:.1f}" if group["completed_round_requests_per_s"] is not None else "—"
            lines.append(f"| {fork['fork_id']} | {esc(group['case'])} / {esc(group['workload'])} | {group['concurrency']} | "
                         f"{group['session_model']} / {group['channel_model']} | {group['repetitions']} / {group['requests']} | "
                         f"{group['median_ms']:.3f} / {group['p95_ms']:.3f} / {group['p99_ms']:.3f} | "
                         f"{group['requests_per_s']:.1f} | {full_rate} | {rate} | {group['client_cpu_wall_ratio']:.3f} |")
            shown += 1
    if total_groups > 200:
        lines += ["", f"Showing 200 of {total_groups} per-fork groups; `cells.csv` and `summary.json` contain every group."]
    lines += ["", "## Protocol coverage and unsupported outcomes", "",
              "Expected rejections and unavailable protocol versions are shown explicitly. "
              "A successful `UNIMPLEMENTED` capability probe verifies the rejection path; it does not mean that feature is implemented.", "",
              "| Fork | Route | Probe records | Statuses | Supported / tested |",
              "| --- | --- | ---: | --- | --- |"]
    for fork in summary["forks"]:
        records = coverage_records(fork["coverage"])
        grouped = defaultdict(list)
        for record in records:
            grouped[record.get("route", record.get("rpc", "unknown"))].append(record)
        for route, values in sorted(grouped.items()):
            statuses = Counter(value.get("status", value.get("outcome", "unknown")) for value in values)
            supported = sum(value.get("supported") is True for value in values)
            tested = sum(value.get("tested", True) is True for value in values)
            labels = ", ".join(f"{esc(status)}: {count}" for status, count in sorted(statuses.items()))
            lines.append(f"| {fork['fork_id']} | {esc(route)} | {len(values)} | {labels} | {supported} / {tested} |")
        for item in fork["coverage"].get("inventory", []) if isinstance(fork["coverage"], dict) else []:
            if item.get("tested") is False:
                lines.append(f"| {fork['fork_id']} | {esc(item.get('route', 'unknown'))} | 0 | "
                             f"Not tested: {esc(item.get('not_tested_reason', 'profile exclusion'))} | 0 / 0 |")
    rejected = [(fork, cell) for fork in summary["forks"] for cell in fork["non_success_cells"]]
    lines += ["", f"Accepted forks contain **{len(rejected)}** timed cells excluded for unsupported/failing outcomes. "
              "Their counts, statuses, and validation/cleanup errors remain in `summary.json`. "
              "Invalid forks are excluded entirely; validation failures name their raw `client.json` files for failure diagnostics.", ""]
    for fork in summary["forks"]:
        if fork["skipped_cells"]:
            lines.append(f"- {fork['fork_id']}: {len(fork['skipped_cells'])} skipped cells; inspect `skipped_cells` for resource-budget/filter reasons.")
    lines += ["", "## Scope and reproducibility", "",
              "Synthetic fixtures decode requests and return prebuilt Arrow responses without executing their plans. "
              "They measure those fixture implementations and runtime stacks, not the complete production frontends. "
              "Real-server planning calls avoid ExecutePlan and artifact upload; a Spark listener independently records zero job starts. "
              "These measurements exclude Spark task execution and worker-to-driver result production.", ""]
    lines += ["- " + esc(warning) for warning in summary["warnings"]]
    lines += ["- Concurrency is closed-loop with bounded in-flight requests. This is not an open-loop arrival-rate/SLO test.",
              "- Changing session and channel sharing independently separates session serialization from HTTP/2 multiplexing effects.",
              "- Startup, corpus generation, plan construction, validation, and lifecycle cleanup are outside the primary timing window; no application retries are enabled.",
              "- Host load and per-process snapshots are retained. A shared desktop and small fork count do not establish a universal speedup.",
              "- The manifest retains binary/jar/library/corpus hashes, JVM versions, commands, listener identity, shutdown, and no-execution audits.",
              "", "```json", json.dumps(summary["metadata"], indent=2), "```", ""]
    return "\n".join(lines)


def write_csvs(directory, summary):
    fields = ["fork_id", "target", *KEY_FIELDS, "repetitions", "requests", "median_ms", "p95_ms", "p99_ms",
              "first_message_median_ms", "first_arrow_median_ms", "requests_per_s", "completed_round_requests_per_s", "response_mib_per_s", "request_bytes",
              "response_bytes", "client_cpu_wall_ratio", "preparation_ns", "validation_cleanup_ns", "full_round_elapsed_ns", "latency_window", "throughput_window", "byte_accounting"]
    with (directory / "cells.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for fork in summary["forks"]:
            for group in fork["groups"]:
                writer.writerow({**group, "fork_id": fork["fork_id"], "target": fork["target"]})
    fields = ["native", "reference", *KEY_FIELDS, "pairs", "latency_ratio_gm", "latency_ratio_min",
              "latency_ratio_max", "throughput_ratio_gm", "throughput_ratio_min", "throughput_ratio_max",
              "completed_round_throughput_ratio_gm", "completed_round_throughput_ratio_min", "completed_round_throughput_ratio_max"]
    with (directory / "comparisons.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for group in summary["comparisons"]:
            row = {**group, "pairs": len(group["pairs"])}
            for key in ("latency_ratio", "throughput_ratio", "completed_round_throughput_ratio"):
                if key not in group:
                    continue
                for suffix, source in (("gm", "geometric_mean"), ("min", "min"), ("max", "max")):
                    row[key + "_" + suffix] = group[key][source]
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    try:
        summary = build_summary(directory)
    except (OSError, ValueError, TypeError, KeyError) as error:
        summary = {"schema_version": 1, "valid": False, "errors": [str(error)],
                   "warnings": [], "metadata": {}, "forks": [], "comparisons": []}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "REPORT.md").write_text(markdown(summary))
    write_csvs(directory, summary)
    print(json.dumps({"valid": summary["valid"], "report": str(directory / "REPORT.md"),
                      "errors": summary["errors"]}))
    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())
