#!/bin/sh
set -eu

case "${1:-}" in
  '') sanitize=0 ;;
  --sanitize) sanitize=1 ;;
  *) echo "Usage: $0 [--sanitize]" >&2; exit 2 ;;
esac
if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [--sanitize]" >&2
  exit 2
fi

: "${JAVA_HOME:?Set JAVA_HOME to a JDK containing JNI headers}"
case "$(uname -s)" in
  Darwin) jni_platform=darwin ;;
  Linux) jni_platform=linux ;;
  *) echo 'The C ownership test requires macOS or Linux with pthreads' >&2; exit 2 ;;
esac

task_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output_dir="$task_root/.build/c-export"
mkdir -p "$output_dir"
set -- -std=c11 -Wall -Wextra -Werror -pthread
test_binary="$output_dir/ownership"
if [ "$sanitize" -eq 1 ]; then
  set -- "$@" -fsanitize=address,undefined -fno-omit-frame-pointer -g
  test_binary="$output_dir/ownership-sanitized"
fi
"${CC:-cc}" "$@" \
  -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/$jni_platform" \
  "$task_root/jni/tests/c_export_ownership.c" -o "$test_binary"
"$test_binary"
