#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_PYTHON="${NATIVE_TEST_PYTHON:-python3}"
"$TEST_PYTHON" -B "$PROJECT_DIR/jvm/test_acceptance_guards.py"
"$PROJECT_DIR/jvm/build.sh" --tests
exec "$TEST_PYTHON" -B "$PROJECT_DIR/jvm/run_acceptance.py" "$@"
