import copy
import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("protocol_report", ROOT / "benchmarks/protocol/report.py")
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def cell(case="range", latency=100, elapsed=1000, unsupported=0):
    return {"case": case, "workload": "AnalyzePlan", "session_model": "shared",
            "channel_model": "shared", "concurrency": 2, "repetition": 0,
            "requests": 4, "successes": 4 - unsupported, "failures": 0,
            "unsupported": unsupported, "elapsed_ns": elapsed, "latencies_ns": [latency] * 4,
            "first_message_latencies_ns": [], "request_bytes": 32, "response_bytes": 64,
            "status_counts": {"Unimplemented": 4} if unsupported else {"Ok": 4},
            "validation_errors": [], "cleanup_errors": [], "client_cpu_ns": 200,
            "warmup_validation_errors": [], "warmup_cleanup_errors": [],
            "latency_window": "primary_rpc_drain_including_client_encode",
            "throughput_window": "primary_rpc_round_excluding_preparation_validation_cleanup"}


def skip(case="range", repetition=0):
    return {"case": case, "session_model": "shared", "channel_model": "shared", "concurrency": 2,
            "repetition": repetition, "reason": "prepared request/response memory budget"}


class ReporterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="protocol-report-test-")
        self.directory = Path(self.tmp.name)
        (self.directory / "payloads").mkdir()
        (self.directory / "payloads/manifest.json").write_text('{"version":1,"cases":[],"payloads":[]}')
        (self.directory / "artifacts").mkdir()
        (self.directory / "artifacts/binary").write_bytes(b"frozen binary")
        self.clients = {}
        runs = []
        for target, latency, elapsed in (("rust-fixture", 100, 1000), ("jvm-fixture", 200, 2000)):
            name = "fork1-" + target
            (self.directory / name).mkdir()
            self.clients[name] = {"schema_version": 1, "passed": True, "mode": "fixture",
                "settings": {"no_execution": True, "retries": 0, "requests": 4, "coverage_only": False,
                             "repetitions": 1, "concurrency": [2], "session_models": ["shared"],
                             "channel_models": ["shared"],
                             "endpoint": "http://127.0.0.1:" + str(1234 + latency)},
                "coverage": {"passed": True, "records": [{"route": route, "status": "OK",
                    "passed": True, "supported": True, "tested": True} for route in sorted(REPORT.ROUTES)]},
                "capabilities": [{"case": "range", "rpc": "AnalyzePlan", "supported": True}],
                "cells": [cell(latency=latency, elapsed=elapsed)], "skipped_cells": []}
            runs.append({"fork_id": name, "fork": 1, "target": target, "profile_kind": "fixture",
                         "client_json": name + "/client.json", "clean_shutdown": True, "client_exit_code": 0,
                         "shutdown": {"listener_closed": True, "fixture_stats": {"query_executions": 0}}})
        self.manifest = {"schema_version": 1, "complete": True, "runs": runs, "metadata": {
            "forks_per_target": 1, "targets": ["rust-fixture", "jvm-fixture"],
            "artifacts": {"binary": {"snapshot": "/old/run/artifacts/binary",
                                     "sha256": REPORT.sha(self.directory / "artifacts/binary")}},
            "payload_sha256": {"manifest.json": REPORT.sha(self.directory / "payloads/manifest.json")}}}

    def tearDown(self):
        self.tmp.cleanup()

    def summary(self):
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest))
        for name, client in self.clients.items():
            (self.directory / name / "client.json").write_text(json.dumps(client))
        return REPORT.build_summary(self.directory)

    def test_matched_ratios_and_csv(self):
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(len(result["comparisons"]), 1)
        self.assertAlmostEqual(result["comparisons"][0]["latency_ratio"]["geometric_mean"], 0.5)
        self.assertAlmostEqual(result["comparisons"][0]["throughput_ratio"]["geometric_mean"], 2)
        self.assertEqual(result["analysis_reporter_sha256"], REPORT.sha(ROOT / "benchmarks/protocol/report.py"))
        REPORT.write_csvs(self.directory, result)
        with (self.directory / "cells.csv").open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 2)
        with (self.directory / "comparisons.csv").open() as stream:
            row, = csv.DictReader(stream)
            self.assertEqual(float(row["latency_ratio_gm"]), 0.5)
        self.assertIn("valid", REPORT.markdown(result))

    def test_unsupported_excluded_and_preserved(self):
        for client in self.clients.values():
            client["capabilities"].append({"case": "get_status", "rpc": "AnalyzePlan", "supported": True})
        self.clients["fork1-rust-fixture"]["cells"].append(cell("get_status"))
        self.clients["fork1-jvm-fixture"]["cells"].append(cell("get_status", unsupported=4))
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(len(result["comparisons"]), 1)
        self.assertEqual(len(result["forks"][0]["groups"]), 2)
        self.assertEqual(len(result["forks"][1]["groups"]), 1)
        self.assertEqual(result["forks"][1]["non_success_cells"][0]["unsupported"], 4)

    def test_completed_round_rate_is_separate(self):
        for name, client in self.clients.items():
            value = client["cells"][0]
            value.update(preparation_ns=1000, validation_cleanup_ns=2000)
            value["full_round_elapsed_ns"] = value["elapsed_ns"] + 3000
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        group = result["forks"][0]["groups"][0]
        self.assertEqual(group["requests_per_s"], 4000000)
        self.assertEqual(group["completed_round_requests_per_s"], 1000000)
        self.assertAlmostEqual(result["comparisons"][0]["throughput_ratio"]["geometric_mean"], 2)
        self.assertAlmostEqual(result["comparisons"][0]["completed_round_throughput_ratio"]["geometric_mean"], 1.25)
        REPORT.write_csvs(self.directory, result)
        with (self.directory / "cells.csv").open() as stream:
            row = next(csv.DictReader(stream))
            self.assertEqual(float(row["completed_round_requests_per_s"]), 1000000)
        self.assertIn("Completed-round", REPORT.markdown(result))

    def test_bad_completed_round_components(self):
        self.clients["fork1-rust-fixture"]["cells"][0].update(
            preparation_ns=100, validation_cleanup_ns=200, full_round_elapsed_ns=1001)
        self.assertFalse(self.summary()["valid"])

    def test_first_arrow_latency_is_distinct(self):
        value = self.clients["fork1-rust-fixture"]["cells"][0]
        value["first_message_latencies_ns"] = [10] * 4
        value["first_arrow_latencies_ns"] = [20] * 4
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        group = result["forks"][0]["groups"][0]
        self.assertAlmostEqual(group["first_message_median_ms"], 0.00001)
        self.assertAlmostEqual(group["first_arrow_median_ms"], 0.00002)

    def test_excess_first_arrow_samples_rejected(self):
        self.clients["fork1-rust-fixture"]["cells"][0]["first_arrow_latencies_ns"] = [20] * 5
        self.assertFalse(self.summary()["valid"])

    def test_mixed_completed_round_counters(self):
        client = self.clients["fork1-rust-fixture"]
        repeated = copy.deepcopy(client["cells"][0])
        repeated.update(repetition=1, full_round_elapsed_ns=5000)
        client["cells"].append(repeated)
        self.assertFalse(self.summary()["valid"])

    def test_bad_counts(self):
        self.clients["fork1-rust-fixture"]["cells"][0]["successes"] = 3
        self.assertFalse(self.summary()["valid"])

    def test_bad_status_counts(self):
        self.clients["fork1-rust-fixture"]["cells"][0]["status_counts"] = {"Ok": 3}
        self.assertFalse(self.summary()["valid"])

    def test_bad_latency_count(self):
        self.clients["fork1-rust-fixture"]["cells"][0]["latencies_ns"] = [100]
        self.assertFalse(self.summary()["valid"])

    def test_duplicate_cell(self):
        client = self.clients["fork1-rust-fixture"]
        client["cells"].append(copy.deepcopy(client["cells"][0]))
        self.assertFalse(self.summary()["valid"])

    def test_settings_mismatch(self):
        self.clients["fork1-rust-fixture"]["settings"]["requests"] = 8
        result = self.summary()
        self.assertFalse(result["valid"])
        self.assertEqual(result["comparisons"], [])

    def test_repetition_identity_mismatch(self):
        self.clients["fork1-rust-fixture"]["cells"][0]["repetition"] = 1
        result = self.summary()
        self.assertFalse(result["valid"])
        self.assertEqual(result["comparisons"], [])

    def test_missing_fork(self):
        self.manifest["metadata"]["forks_per_target"] = 2
        self.assertFalse(self.summary()["valid"])

    def test_corrupt_artifact_hash(self):
        (self.directory / "artifacts/binary").write_bytes(b"different")
        self.assertFalse(self.summary()["valid"])

    def test_corrupt_corpus_hash(self):
        (self.directory / "payloads/manifest.json").write_text('{}')
        self.assertFalse(self.summary()["valid"])

    def test_incomplete_run(self):
        self.manifest["complete"] = False
        self.assertFalse(self.summary()["valid"])

    def test_failed_client(self):
        self.clients["fork1-rust-fixture"]["passed"] = False
        result = self.summary()
        self.assertFalse(result["valid"])
        self.assertTrue(any("fork1-rust-fixture/client.json" in error for error in result["errors"]))
        self.assertIn("raw `client.json`", REPORT.markdown(result))

    def test_missing_route(self):
        self.clients["fork1-rust-fixture"]["coverage"]["records"].pop()
        self.assertFalse(self.summary()["valid"])

    def test_nonzero_query_audit(self):
        self.manifest["runs"][0]["shutdown"]["fixture_stats"]["query_executions"] = 1
        self.assertFalse(self.summary()["valid"])

    def test_missing_shutdown_audit(self):
        self.manifest["runs"][0]["shutdown"]["listener_closed"] = False
        self.assertFalse(self.summary()["valid"])

    def test_nonempty_final_cleanup_errors(self):
        self.clients["fork1-rust-fixture"]["final_cleanup_errors"] = ["release failed"]
        self.assertFalse(self.summary()["valid"])

    def test_incomplete_client(self):
        self.clients["fork1-rust-fixture"]["incomplete"] = True
        self.assertFalse(self.summary()["valid"])

    def test_in_progress_client(self):
        self.clients["fork1-rust-fixture"]["in_progress"] = {"case": "range", "phase": "measuring"}
        self.assertFalse(self.summary()["valid"])

    def test_skipped_budget_cells_preserved(self):
        client = self.clients["fork1-rust-fixture"]
        client["capabilities"].append({"case": "huge", "rpc": "AnalyzePlan", "supported": True})
        skipped = skip("huge")
        client["skipped_cells"].append(skipped)
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["forks"][0]["skipped_cells"], [skipped])

    def test_symmetric_missing_repetitions_rejected(self):
        for client in self.clients.values():
            client["settings"]["repetitions"] = 2
        result = self.summary()
        self.assertFalse(result["valid"])
        self.assertEqual(result["comparisons"], [])
        self.assertTrue(any("1 missing" in error for error in result["errors"]))
        self.assertTrue(any("fork1-rust-fixture/client.json" in error for error in result["errors"]))

    def test_symmetric_settings_narrower_than_manifest_rejected(self):
        self.manifest["metadata"]["repetitions"] = 2
        self.assertFalse(self.summary()["valid"])

    def test_manifest_csv_grid_settings_match_client_arrays(self):
        self.manifest["metadata"].update(concurrency="2", session_models="shared", channel_models="shared")
        self.assertTrue(self.summary()["valid"])
        self.manifest["metadata"]["concurrency"] = "1,2"
        self.assertFalse(self.summary()["valid"])

    def test_symmetric_missing_topologies_rejected(self):
        for client in self.clients.values():
            client["settings"]["session_models"] = ["shared", "isolated"]
        self.assertFalse(self.summary()["valid"])

    def test_symmetric_missing_concurrency_rejected(self):
        for client in self.clients.values():
            client["settings"]["concurrency"] = [1, 2]
        self.assertFalse(self.summary()["valid"])

    def test_symmetric_out_of_range_repetitions_rejected(self):
        for client in self.clients.values():
            client["cells"][0]["repetition"] = 1
        self.assertFalse(self.summary()["valid"])

    def test_symmetric_wrong_request_count_rejected(self):
        for client in self.clients.values():
            client["settings"]["requests"] = 8
        self.assertFalse(self.summary()["valid"])

    def test_empty_timed_grid_rejected(self):
        for client in self.clients.values():
            client["cells"] = []
        self.assertFalse(self.summary()["valid"])

    def test_missing_capabilities_rejected(self):
        self.clients["fork1-rust-fixture"]["capabilities"] = []
        self.assertFalse(self.summary()["valid"])

    def test_duplicate_capabilities_rejected(self):
        client = self.clients["fork1-rust-fixture"]
        client["capabilities"].append(copy.deepcopy(client["capabilities"][0]))
        self.assertFalse(self.summary()["valid"])

    def test_unsupported_capability_requires_no_timed_cells(self):
        for client in self.clients.values():
            client["capabilities"].append({"case": "unavailable", "rpc": "GetStatus", "supported": False})
        self.assertTrue(self.summary()["valid"])
        self.clients["fork1-rust-fixture"]["cells"].append(cell("unavailable"))
        self.assertFalse(self.summary()["valid"])

    def test_explicit_skip_completes_requested_grid(self):
        for client in self.clients.values():
            client["settings"]["repetitions"] = 2
            client["skipped_cells"].append(skip(repetition=1))
        self.assertTrue(self.summary()["valid"])

    def test_all_cells_explicitly_skipped_is_valid(self):
        for client in self.clients.values():
            client["cells"] = []
            client["skipped_cells"].append(skip())
        self.assertTrue(self.summary()["valid"])

    def test_duplicate_skips_rejected(self):
        client = self.clients["fork1-rust-fixture"]
        client["cells"] = []
        client["skipped_cells"] = [skip(), skip()]
        self.assertFalse(self.summary()["valid"])

    def test_measured_and_skipped_cell_rejected(self):
        self.clients["fork1-rust-fixture"]["skipped_cells"].append(skip())
        self.assertFalse(self.summary()["valid"])

    def test_skip_without_reason_rejected(self):
        client = self.clients["fork1-rust-fixture"]
        client["cells"] = []
        client["skipped_cells"] = [skip()]
        client["skipped_cells"][0]["reason"] = ""
        self.assertFalse(self.summary()["valid"])

    def test_explicit_coverage_only_is_valid(self):
        for client in self.clients.values():
            client["settings"]["coverage_only"] = True
            client["cells"] = []
            client["capabilities"] = []
        result = self.summary()
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["comparisons"], [])

    def test_coverage_only_with_timed_work_rejected(self):
        for client in self.clients.values():
            client["settings"]["coverage_only"] = True
        self.assertFalse(self.summary()["valid"])

    def test_coverage_only_with_skipped_work_rejected(self):
        client = self.clients["fork1-rust-fixture"]
        client["settings"]["coverage_only"] = True
        client["cells"] = []
        client["capabilities"] = []
        client["skipped_cells"] = [skip()]
        self.assertFalse(self.summary()["valid"])

    def test_missing_coverage_only_setting_rejected(self):
        del self.clients["fork1-rust-fixture"]["settings"]["coverage_only"]
        self.assertFalse(self.summary()["valid"])


if __name__ == "__main__":
    unittest.main()
