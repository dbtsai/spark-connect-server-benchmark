"""Optional executed-query runner option guards; no Spark, builds, or network."""

import contextlib
import importlib.util
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("executed_options", ROOT / "benchmarks/run.py")
EXECUTED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXECUTED)


def parse(extra=()):
    args, _ = EXECUTED.parse_args(["--output", "unused-test-output"] + list(extra))
    return args


class ExecutedOptionsTests(unittest.TestCase):
    def test_installations_default_to_environment(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark installation", "JAVA_HOME": "jdk installation"}, clear=True):
            args = parse()
            self.assertEqual(args.spark_home, Path("spark installation").resolve())
            self.assertEqual(args.java_home, Path("jdk installation").resolve())

    def test_explicit_installations_override_environment(self):
        with patch.dict(os.environ, {"SPARK_HOME": "environment-spark", "JAVA_HOME": "environment-jdk"}, clear=True):
            args = parse(["--spark-home", "selected spark", "--java-home", "selected jdk"])
            self.assertEqual(args.spark_home, Path("selected spark").resolve())
            self.assertEqual(args.java_home, Path("selected jdk").resolve())

    def test_explicit_installations_work_without_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            args = parse(["--spark-home", "spark", "--java-home", "jdk"])
            self.assertEqual(args.spark_home, Path("spark").resolve())
            self.assertEqual(args.java_home, Path("jdk").resolve())

    def test_missing_or_empty_installation_has_clear_parser_error(self):
        for variable, option, present in (("SPARK_HOME", "--spark-home", {"JAVA_HOME": "jdk"}),
                                           ("JAVA_HOME", "--java-home", {"SPARK_HOME": "spark"})):
            for missing in ({}, {variable: ""}):
                with self.subTest(variable=variable, missing=missing):
                    error = io.StringIO()
                    with patch.dict(os.environ, dict(present, **missing), clear=True), contextlib.redirect_stderr(error):
                        with self.assertRaises(SystemExit) as raised:
                            parse()
                    self.assertEqual(raised.exception.code, 2)
                    self.assertIn(option + " or " + variable + " must be set", error.getvalue())

    def test_default_artifacts_follow_build_output_and_platform(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
            for platform_name, suffix in (("darwin", ".dylib"), ("linux", ".so")):
                with self.subTest(platform=platform_name), patch.object(sys, "platform", platform_name):
                    args = parse()
                    self.assertEqual(args.library, ROOT / ("target/release/libspark_connect_jni" + suffix))
                    self.assertEqual(args.jar, ROOT / ".build/jvm/spark-connect-native-bridge.jar")

    def test_python_default_and_override_preserve_virtualenv_entry_point(self):
        with tempfile.TemporaryDirectory() as folder:
            entry = Path(folder) / "venv-python"
            entry.symlink_to(sys.executable)
            with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
                with patch.object(sys, "executable", str(entry)):
                    self.assertEqual(parse().python, entry)
                self.assertEqual(parse(["--python", str(entry)]).python, entry)

    def test_workload_options_are_forwarded_unchanged(self):
        with patch.dict(os.environ, {"SPARK_HOME": "spark", "JAVA_HOME": "jdk"}, clear=True):
            _, workload = EXECUTED.parse_args(["--output", "unused-test-output", "--stream-rows", "1024"])
        self.assertEqual(workload, ["--stream-rows", "1024"])


if __name__ == "__main__":
    unittest.main()
