"""Portable runner option resolution without builds, servers, or network access."""

import contextlib
import importlib.util
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXECUTED = load("executed_options", "benchmarks/run.py")
PROTOCOL = load("protocol_options", "benchmarks/protocol/run.py")
COMPARE = load("compare_options", "benchmarks/protocol/compare.py")
RUNNERS = (EXECUTED, PROTOCOL, COMPARE)


def parse(module, extra=()):
    argv = ["--output", "unused-test-output"]
    if module is COMPARE:
        argv += ["--baseline-jar", "baseline.jar", "--baseline-library", "baseline.so",
                 "--candidate-jar", "candidate.jar", "--candidate-library", "candidate.so"]
    result = module.parse_args(argv + list(extra))
    return result[0] if module is EXECUTED else result


class PortableOptionsTests(unittest.TestCase):
    def test_installations_default_to_environment(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark installation", "JAVA_HOME": "jdk installation"}, clear=True):
            for module in RUNNERS:
                with self.subTest(runner=module.__name__):
                    args = parse(module)
                    self.assertEqual(args.spark_home, Path("spark installation").resolve())
                    self.assertEqual(args.java_home, Path("jdk installation").resolve())

    def test_explicit_installations_override_environment(self):
        with patch.dict(os.environ, {"SPARK_HOME": "environment-spark", "JAVA_HOME": "environment-jdk"}, clear=True):
            for module in RUNNERS:
                with self.subTest(runner=module.__name__):
                    args = parse(module, ["--spark-home", "selected spark", "--java-home", "selected jdk"])
                    self.assertEqual(args.spark_home, Path("selected spark").resolve())
                    self.assertEqual(args.java_home, Path("selected jdk").resolve())

    def test_explicit_installations_work_without_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            for module in RUNNERS:
                with self.subTest(runner=module.__name__):
                    args = parse(module, ["--spark-home", "spark", "--java-home", "jdk"])
                    self.assertEqual(args.spark_home, Path("spark").resolve())
                    self.assertEqual(args.java_home, Path("jdk").resolve())

    def test_missing_or_empty_installation_has_clear_parser_error(self):
        for variable, option, present in (("SPARK_HOME", "--spark-home", {"JAVA_HOME": "jdk"}),
                                           ("JAVA_HOME", "--java-home", {"SPARK_HOME": "spark"})):
            for missing in ({}, {variable: ""}):
                for module in RUNNERS:
                    with self.subTest(runner=module.__name__, variable=variable, missing=missing):
                        error = io.StringIO()
                        with patch.dict(os.environ, dict(present, **missing), clear=True), contextlib.redirect_stderr(error):
                            with self.assertRaises(SystemExit) as raised:
                                parse(module)
                        self.assertEqual(raised.exception.code, 2)
                        self.assertIn(option + " or " + variable + " must be set", error.getvalue())

    def test_default_artifacts_follow_build_output_and_platform(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
            for module in (EXECUTED, PROTOCOL):
                for platform_name, suffix in (("darwin", ".dylib"), ("linux", ".so")):
                    with self.subTest(runner=module.__name__, platform=platform_name), patch.object(sys, "platform", platform_name):
                        args = parse(module)
                        self.assertEqual(args.library, ROOT / ("target/release/libspark_connect_jni" + suffix))
                        self.assertEqual(args.jar, ROOT / ".build/jvm/spark-connect-native-bridge.jar")

    def test_executed_python_default_and_override_preserve_virtualenv_entry_point(self):
        with tempfile.TemporaryDirectory() as folder:
            entry = Path(folder) / "venv-python"
            entry.symlink_to(sys.executable)
            with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
                with patch.object(sys, "executable", str(entry)):
                    self.assertEqual(parse(EXECUTED).python, entry)
                self.assertEqual(parse(EXECUTED, ["--python", str(entry)]).python, entry)

    def test_executed_workload_options_are_forwarded_unchanged(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
            _, workload = EXECUTED.parse_args(["--output", "unused-test-output", "--stream-rows", "1024"])
        self.assertEqual(workload, ["--stream-rows", "1024"])

    def test_rust_fixture_runs_without_spark_or_jdk_environment(self):
        with patch.dict(os.environ, {"SPARK_HOME": "", "JAVA_HOME": ""}, clear=True):
            args = parse(PROTOCOL, ["--targets", "rust-fixture"])
            self.assertIsNone(args.spark_home)
            self.assertIsNone(args.java_home)
        with patch.dict(os.environ, {"SPARK_HOME": "inherited", "JAVA_HOME": "inherited"}, clear=True):
            env = PROTOCOL.environment(args)
        self.assertNotIn("SPARK_HOME", env)
        self.assertNotIn("JAVA_HOME", env)

    def test_jvm_targets_require_installations_even_when_mixed_with_rust_fixture(self):
        for targets in ("jvm-fixture", "jvm-planning", "rust-jvm-planning", "rust-fixture,jvm-fixture"):
            with self.subTest(targets=targets), patch.dict(os.environ, {}, clear=True), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    parse(PROTOCOL, ["--targets", targets])
                self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
