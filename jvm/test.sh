#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$PROJECT_DIR/jvm/build.sh" --tests
BUILD_DIR="$PROJECT_DIR/.build/jvm"
mkdir -p "$BUILD_DIR/warehouse"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_CONNECT_MODE=0
exec "$SPARK_HOME/bin/spark-submit" \
  --class org.apache.spark.sql.connect.nativebridge.BridgeSmokeTest \
  --master 'local[2]' --driver-memory 1g \
  --conf spark.ui.enabled=false \
  --conf spark.connect.grpc.debug.enabled=true \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf "spark.sql.warehouse.dir=$BUILD_DIR/warehouse" \
  "$BUILD_DIR/bridge-test.jar"
