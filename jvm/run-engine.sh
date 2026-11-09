#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || "$1" != /* ]]; then
  echo "usage: $0 <absolute-native-library> [listen-address]" >&2
  exit 1
fi
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$PROJECT_DIR/jvm/build.sh"
export SPARK_CONNECT_MODE=0
exec "$SPARK_HOME/bin/spark-submit" \
  --class org.apache.spark.sql.connect.nativebridge.engine.EmbeddedSparkEngine \
  --master "${SPARK_MASTER:-local[2]}" \
  "$PROJECT_DIR/.build/jvm/spark-connect-native-bridge.jar" "$1" "${2:-127.0.0.1:15002}"
