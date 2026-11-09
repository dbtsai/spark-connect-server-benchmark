"""Negative checks for acceptance's shutdown evidence requirements; no Spark needed."""

import signal
import unittest

from run_acceptance import SHUTDOWN_COMPLETE, shutdown_error


class ShutdownEvidenceTest(unittest.TestCase):
    def test_completed_cleanup_with_sigterm_is_accepted(self):
        self.assertIsNone(shutdown_error(SHUTDOWN_COMPLETE + "\n", 128 + signal.SIGTERM))

    def test_startup_cleanup_is_not_normal_shutdown(self):
        content = "Embedded Spark Connect startup failed; cleanup complete\n"
        self.assertIsNotNone(shutdown_error(content, 0))

    def test_missing_cleanup_marker_is_rejected(self):
        self.assertIsNotNone(shutdown_error("SparkContext stopped\n", 128 + signal.SIGTERM))

    def test_native_stop_failure_is_rejected_even_with_marker(self):
        content = "JNI stop is incomplete: active calls remain\n" + SHUTDOWN_COMPLETE + "\n"
        self.assertIsNotNone(shutdown_error(content, 128 + signal.SIGTERM))

    def test_launcher_cleanup_failure_is_rejected(self):
        content = "Embedded Spark Connect shutdown failed: native stop incomplete\n"
        self.assertIsNotNone(shutdown_error(content, 128 + signal.SIGTERM))

    def test_abnormal_exit_is_rejected_even_with_marker(self):
        self.assertIsNotNone(shutdown_error(SHUTDOWN_COMPLETE + "\n", 1))

    def test_marker_requires_a_complete_line(self):
        self.assertIsNotNone(shutdown_error(SHUTDOWN_COMPLETE + " (not really)\n", 0))


if __name__ == "__main__":
    unittest.main()
