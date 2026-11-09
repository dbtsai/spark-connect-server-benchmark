#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--tests" ) ]]; then
  echo "usage: $0 [--tests]" >&2
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${SPARK_HOME:?Set SPARK_HOME to an existing Spark 4.2 assembly with shaded Connect gRPC jars}"
SPARK_JARS="$SPARK_HOME/jars"
if [[ -d "$SPARK_HOME/assembly/target/scala-2.13/jars" ]]; then
  SPARK_JARS="$SPARK_HOME/assembly/target/scala-2.13/jars"
fi
if [[ ! -d "$SPARK_JARS" ]]; then
  echo "Spark jar directory does not exist: $SPARK_JARS" >&2
  exit 1
fi
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
JAR="${JAVA_HOME:+$JAVA_HOME/bin/}jar"
BUILD_DIR="$PROJECT_DIR/.build/jvm"
rm -rf -- "$BUILD_DIR/classes" "$BUILD_DIR/test-classes"
mkdir -p "$BUILD_DIR/classes"
MAIN_SOURCES=()
while IFS= read -r -d '' source; do
  MAIN_SOURCES+=("$source")
done < <(find "$PROJECT_DIR/jvm/src/main/java" -name '*.java' -type f -print0)
"$JAVAC" -proc:none -cp "$SPARK_JARS/*" -d "$BUILD_DIR/classes" \
  "${MAIN_SOURCES[@]}"
"$JAR" --create --file "$BUILD_DIR/spark-connect-native-bridge.jar" -C "$BUILD_DIR/classes" .
echo "$BUILD_DIR/spark-connect-native-bridge.jar"
if [[ "${1:-}" == "--tests" ]]; then
  mkdir -p "$BUILD_DIR/test-classes"
  TEST_SOURCES=()
  while IFS= read -r -d '' source; do
    TEST_SOURCES+=("$source")
  done < <(find "$PROJECT_DIR/jvm/src/test/java" -name '*.java' -type f -print0)
  "$JAVAC" -proc:none -cp "$SPARK_JARS/*:$BUILD_DIR/classes" -d "$BUILD_DIR/test-classes" \
    "${TEST_SOURCES[@]}"
  "$JAR" --create --file "$BUILD_DIR/bridge-test.jar" \
    -C "$BUILD_DIR/classes" . -C "$BUILD_DIR/test-classes" .
  echo "$BUILD_DIR/bridge-test.jar"
fi
