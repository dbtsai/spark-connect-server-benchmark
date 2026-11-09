"""Pure regression tests: no Spark processes, builds, or network listeners."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("compare", Path(__file__).resolve().parents[1] / "compare.py")
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


def group(case="config/get", workers=8, scale=1):
    return {"case": case, "workload": "Config", "session_model": "shared", "channel_model": "shared", "concurrency": workers,
            "requests": 256, "repetitions": 2, "latency_window": "rpc", "throughput_window": "primary", "byte_accounting": "protobuf",
            "rounds": [{"repetition": 0, "requests": 128}, {"repetition": 1, "requests": 128}],
            "median_ms": 2 / scale, "p95_ms": 3 / scale, "p99_ms": 4 / scale, "requests_per_s": 100 * scale,
            "completed_round_requests_per_s": 80 * scale, "client_cpu_ns": 25600 / scale}


def sample(pair=1, variant="baseline", scale=1):
    return {"pair": pair, "variant": variant, "summary": {"valid": True, "forks": [{
        "target": "rust-jvm-planning", "profile_kind": "planning", "settings": {"endpoint": variant, "requests": 128, "repetitions": 2},
        "groups": [group(scale=scale)], "skipped_cells": [], "non_success_cells": []}]}}


def policy():
    return {"targets": [{"case": "config/get", "concurrency": [8]}], "min_target_throughput_ratio": 1.15,
            "min_improved_targets": 1, "max_median_latency_ratio": 1.2, "min_throughput_ratio": 0.8,
            "max_p95_latency_ratio": 1.5, "max_client_cpu_per_request_ratio": 1.25,
            "min_completed_round_throughput_ratio": 0.8, "require_all_pairs_target_improve": False}


class ComparisonTests(unittest.TestCase):
    def test_orders_alternate_and_can_start_ba(self):
        self.assertEqual([compare.order(i, "AB") for i in (1, 2, 3)], [("baseline", "candidate"), ("candidate", "baseline"), ("baseline", "candidate")])
        self.assertEqual(compare.order(1, "BA"), ("candidate", "baseline"))
        self.assertEqual(compare.order(2, "BA"), ("baseline", "candidate"))

    def test_fresh_pair_ratios_use_geometric_mean_and_keep_every_pair(self):
        cells = compare.aggregate([sample(1), sample(1, "candidate", 2), sample(2), sample(2, "candidate", 0.5)], 2)
        self.assertEqual(len(cells), 1)
        self.assertEqual(cells[0]["ratios"]["throughput"], {"geometric_mean": 1, "min": 0.5, "max": 2})
        self.assertEqual([p["ratios"]["throughput"] for p in cells[0]["pairs"]], [2, 0.5])
        self.assertEqual(cells[0]["pairs"][0]["baseline"]["client_cpu_per_request"], 100)
        self.assertEqual(cells[0]["ratios"]["client_cpu_per_request"]["geometric_mean"], 1)

    def test_bad_fork_identities_or_incomplete_pairs_rejected(self):
        for rows, pairs in [([sample()], 1), ([sample(), sample()], 1), ([sample(0), sample(0, "candidate")], 1), ([sample(True), sample(1, "candidate")], 1), ([], 0)]:
            with self.subTest(rows=rows, pairs=pairs), self.assertRaises(ValueError):
                compare.aggregate(rows, pairs)

    def test_invalid_child_failed_cells_or_wrong_engine_rejected(self):
        mutations = [lambda s: s["summary"].update(valid=False),
                     lambda s: s["summary"]["forks"].append(copy.deepcopy(s["summary"]["forks"][0])),
                     lambda s: s["summary"]["forks"][0].update(target="jvm-planning"),
                     lambda s: s["summary"]["forks"][0].update(non_success_cells=[{"failures": 1}])]
        for mutation in mutations:
            candidate = sample(1, "candidate"); mutation(candidate)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                compare.aggregate([sample(), candidate], 1)

    def test_settings_grids_skips_and_windows_must_match(self):
        mutations = [lambda f: f["settings"].update(requests=64), lambda f: f["groups"].clear(),
                     lambda f: f["groups"].append(copy.deepcopy(f["groups"][0])),
                     lambda f: f["groups"][0].update(case="different"), lambda f: f.update(skipped_cells=[{"case": "missing", "reason": "budget"}]),
                     lambda f: f["groups"][0].update(requests=128), lambda f: f["groups"][0].update(latency_window="different"),
                     lambda f: f["groups"][0]["rounds"][1].update(repetition=2), lambda f: f["groups"][0]["rounds"][1].update(requests=64)]
        for mutation in mutations:
            candidate = sample(1, "candidate"); mutation(candidate["summary"]["forks"][0])
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                compare.aggregate([sample(), candidate], 1)

    def test_nonfinite_zero_missing_metrics_rejected(self):
        for key, value in [("requests_per_s", 0), ("p95_ms", float("nan")), ("median_ms", float("inf")), ("client_cpu_ns", 0), ("completed_round_requests_per_s", None)]:
            candidate = sample(1, "candidate"); candidate["summary"]["forks"][0]["groups"][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                compare.aggregate([sample(), candidate], 1)

    def test_successful_candidate_guard_failure_and_missing_improvement(self):
        cells = compare.aggregate([sample(), sample(1, "candidate", 1.25)], 1)
        self.assertEqual(compare.gate(cells, policy())["status"], "performance_candidate")
        neutral = compare.aggregate([sample(), sample(1, "candidate")], 1)
        self.assertEqual(compare.gate(neutral, policy())["status"], "rejected")
        for metric, value in [("median_latency", 1.3), ("throughput", 0.7), ("p95_latency", 1.6), ("client_cpu_per_request", 1.3), ("completed_round_throughput", 0.7)]:
            bad = copy.deepcopy(cells); bad[0]["ratios"][metric]["geometric_mean"] = value
            with self.subTest(metric=metric):
                self.assertFalse(compare.gate(bad, policy())["passed"])

    def test_noise_control_checks_guards_without_demanding_improvement(self):
        cells = compare.aggregate([sample(), sample(1, "candidate")], 1)
        self.assertEqual(compare.gate(cells, policy(), True)["status"], "noise_control_pass")
        cells[0]["ratios"]["completed_round_throughput"]["geometric_mean"] = 0.7
        self.assertFalse(compare.gate(cells, policy(), True)["passed"])

    def test_all_pairs_target_improvement_optional(self):
        cells = compare.aggregate([sample(), sample(1, "candidate", 2), sample(2), sample(2, "candidate", 0.9)], 2)
        self.assertTrue(compare.gate(cells, policy())["passed"])
        strict = policy(); strict["require_all_pairs_target_improve"] = True
        self.assertFalse(compare.gate(cells, strict)["passed"])

    def test_topology_copies_do_not_satisfy_distinct_target_minimum(self):
        cells = compare.aggregate([sample(), sample(1, "candidate", 1.25)], 1)
        duplicate = copy.deepcopy(cells[0]); duplicate["session_model"] = "isolated"; cells.append(duplicate)
        p = policy(); p["min_improved_targets"] = 2
        self.assertFalse(compare.gate(cells, p)["passed"])
        self.assertEqual(len(compare.gate(cells, p)["improved_targets"]), 1)
        duplicate["ratios"]["throughput"]["geometric_mean"] = 1
        self.assertEqual(compare.gate(cells, policy())["improved_targets"], [])

    def test_missing_policy_targets_and_invalid_thresholds_rejected(self):
        cells = compare.aggregate([sample(), sample(1, "candidate", 1.25)], 1)
        for key, value in [("targets", []), ("targets", [{"case": "absent", "concurrency": [8]}]), ("min_improved_targets", 1.5), ("min_target_throughput_ratio", 1), ("max_p95_latency_ratio", float("inf")), ("require_all_pairs_target_improve", "false")]:
            p = policy(); p[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                compare.gate(cells, p)
        self.assertEqual(compare.gate(cells, None)["status"], "descriptive")
        with self.assertRaises(ValueError):
            compare.gate([], None)

    def test_policy_matrix_rejects_narrowed_settings_and_hidden_filter(self):
        matrix = {"profile": "smoke", "concurrency": [1, 8, 32], "case_filter": None}
        compare.validate_matrix({"matrix": {"profile": "smoke", "concurrency": [1, 8, 32]}}, matrix)
        for expected in ({"profile": "standard"}, {"concurrency": [1]}, {"unknown": 1}):
            with self.subTest(expected=expected), self.assertRaises(ValueError):
                compare.validate_matrix({"matrix": expected}, matrix)
        matrix["case_filter"] = "range"
        with self.assertRaises(ValueError):
            compare.validate_matrix({"matrix": {"profile": "smoke"}}, matrix)

    def test_frozen_hashes_detect_source_or_snapshot_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); source = root / "source"; source.write_text("original"); entries = []
            snapshot = compare.freeze(source, root / "frozen/item", entries)
            compare.verify(entries)
            source.write_text("changed")
            with self.assertRaises(ValueError): compare.verify(entries)
            source.write_text("original"); snapshot.write_text("changed")
            with self.assertRaises(ValueError): compare.verify(entries)

    def test_json_nonfinite_policy_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "policy.json"; p.write_text('{"invalid": NaN}')
            with self.assertRaises(ValueError): compare.read_json(p)

    def args(self, root, control=False):
        runner = root / "source/benchmarks/protocol/run.py"; runner.parent.mkdir(parents=True)
        runner.write_text('''import hashlib

def environment(args): return {}

def prepare_payloads(args, output, env):
    payloads = output / "payloads"
    payloads.mkdir()
    (payloads / "manifest.json").write_text("{}")
    return payloads, {"manifest.json": hashlib.sha256(b"{}").hexdigest()}
''')
        (runner.parent / "report.py").write_text("# frozen reporter")
        artifact = root / "input"; artifact.write_text("identical")
        p = root / "policy.json"; p.write_text(json.dumps(policy()))
        return types.SimpleNamespace(output=root / "result", runner=runner, binary=artifact,
            baseline_jar=artifact, baseline_library=artifact, candidate_jar=artifact, candidate_library=artifact,
            spark_home=root, java_home=root, payload_dir=None, policy=p, pairs=2, start_order="AB", control=control,
            profile="smoke", concurrency="1,8,32", requests=128, warmups=64, repetitions=2, session_models="shared", channel_models="shared",
            max_prepared_mib=60, client_threads=4, timeout_seconds=30, client_timeout=3600, startup_timeout=90, case=None)

    def test_failed_child_reported_and_no_following_pair_runs(self):
        with tempfile.TemporaryDirectory() as folder:
            args = self.args(Path(folder))
            with patch.object(compare.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)) as call:
                self.assertEqual(compare.run(args), 1)
            result = json.loads((args.output / "compare.json").read_text())
            self.assertFalse(result["valid"]); self.assertEqual(len(result["runs"]), 1)
            self.assertEqual(call.call_count, 2); self.assertTrue((args.output / "COMPARE.md").exists())
            self.assertEqual(result["runs"][0]["report_exit_code"], 1)

    def test_policy_matrix_failure_saves_outputs_without_starting_children(self):
        with tempfile.TemporaryDirectory() as folder:
            args = self.args(Path(folder)); p = policy(); p["matrix"] = {"requests": 256}; args.policy.write_text(json.dumps(p))
            with patch.object(compare.subprocess, "run") as call:
                self.assertEqual(compare.run(args), 1)
            call.assert_not_called(); self.assertTrue((args.output / "COMPARE.md").exists())

    def test_control_rejects_different_artifact_bytes_before_children(self):
        with tempfile.TemporaryDirectory() as folder:
            args = self.args(Path(folder), True); different = Path(folder) / "different"; different.write_text("different")
            args.candidate_library = different
            with patch.object(compare.subprocess, "run") as call:
                self.assertEqual(compare.run(args), 1)
            call.assert_not_called()
            self.assertIn("identical", (args.output / "compare.json").read_text())

    def test_mocked_full_control_uses_frozen_layout_and_alternating_children(self):
        with tempfile.TemporaryDirectory() as folder:
            args = self.args(Path(folder), True); labels = []
            def subprocess_run(command, **unused):
                if "--output" in command:
                    child = Path(command[command.index("--output") + 1]); child.mkdir()
                    labels.append(child.name)
                    self.assertEqual(Path(command[1]).parts[-3:], ("benchmarks", "protocol", "run.py"))
                    self.assertIn("rust-jvm-planning", command)
                else:
                    child = Path(command[-1]); variant = child.name.split("-", 1)[1]
                    (child / "summary.json").write_text(json.dumps(sample(variant=variant)["summary"]))
                return subprocess.CompletedProcess(command, 0)
            with patch.object(compare.subprocess, "run", side_effect=subprocess_run):
                self.assertEqual(compare.run(args), 0)
            self.assertEqual(labels, ["pair1-baseline", "pair1-candidate", "pair2-candidate", "pair2-baseline"])
            result = json.loads((args.output / "compare.json").read_text())
            self.assertTrue(result["valid"]); self.assertEqual(result["status"], "noise_control_pass")
            self.assertEqual(len(result["cells"][0]["pairs"]), 2)
            compare.verify(result["inputs"])


if __name__ == "__main__":
    unittest.main()
