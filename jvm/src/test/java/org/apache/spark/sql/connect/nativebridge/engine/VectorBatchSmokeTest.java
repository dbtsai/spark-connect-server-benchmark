package org.apache.spark.sql.connect.nativebridge.engine;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.spark.connect.proto.Range;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.sql.SparkSession;

/** Java-side export identity, immutable-batch lifetime, and nested vector layout checks. */
public final class VectorBatchSmokeTest {
  private VectorBatchSmokeTest() {}

  public static void main(String[] arguments) throws Exception {
    SparkSession spark = SparkSession.builder().appName("Native Arrow Vector Smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    try (SparkJvmEngine engine = new SparkJvmEngine(spark)) {
      require(NativeArrowBatch.liveBatchCount() == 0, "unexpected live batch before test");
      immutableLeases(engine);
      nestedAndEmpty(engine);
      require(NativeArrowBatch.liveBatchCount() == 0, "exported batch leak after tests");
      require(!spark.sparkContext().isStopped(), "session close stopped shared SparkContext");
      System.out.println("VECTOR_BATCH_SMOKE_OK: original off-heap address identity, immutable "
          + "multiple batches, retained values after query/session close, cross-thread/idempotent "
          + "release, nested/string/null/map/decimal/timestamp vectors, empty results");
    } finally {
      spark.stop();
    }
  }

  private static SparkJvmEngine.Session open(SparkJvmEngine engine) {
    try (SparkJvmEngine.Task task = engine.newTask("vector smoke open")) {
      return engine.openSession("vector-smoke", UUID.randomUUID().toString(), task);
    }
  }

  private static SparkJvmEngine.Query prepare(SparkJvmEngine engine,
      SparkJvmEngine.Session session, Relation relation) {
    SparkJvmEngine.Task task = engine.newTask("vector smoke prepare");
    try {
      return session.prepare(relation.toByteArray(), task);
    } catch (Throwable failure) {
      task.close();
      throw failure;
    }
  }

  private static Relation sql(String value) {
    return Relation.newBuilder().setSql(SQL.newBuilder().setQuery(value)).build();
  }

  private static void batchRows(SparkJvmEngine engine, SparkJvmEngine.Session session,
      String value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(1);
    for (String field : new String[] {"spark.sql.execution.arrow.maxRecordsPerBatch", value}) {
      byte[] encoded = field.getBytes(StandardCharsets.UTF_8);
      out.writeInt(encoded.length);
      out.write(encoded);
    }
    try (SparkJvmEngine.Task task = engine.newTask("vector smoke batch size")) {
      session.config(6, bytes.toByteArray(), task);
    }
  }

  private static void immutableLeases(SparkJvmEngine engine) throws Exception {
    SparkJvmEngine.Session session = open(engine);
    NativeArrowBatch first = null;
    NativeArrowBatch second = null;
    try {
      batchRows(engine, session, "3");
      Relation range = Relation.newBuilder().setRange(Range.newBuilder()
          .setStart(0).setEnd(25).setStep(1).setNumPartitions(2)).build();
      try (SparkJvmEngine.Query query = prepare(engine, session, range)) {
        first = query.nextVectorBatch();
        require(first != null && first.descriptor().length() == 3, "first vector batch size");
        NativeArrowBatch.Node field = first.descriptor().children()[0];
        require(field.buffers().length == 2 && field.buffers()[1] != 0, "range data pointer missing");
        require(field.buffers()[1] == first.vectorBufferAddress(0, 1),
            "exported pointer differs from original ArrowBuf address");
        require(field.bufferSizes()[1] >= 3 * Long.BYTES, "range data buffer too small");
        require(((Number) first.valueAt(0, 0)).longValue() == 0, "first range value");
        second = query.nextVectorBatch();
        require(second != null && ((Number) second.valueAt(0, 0)).longValue() == 3,
            "second batch did not advance");
        require(first.descriptor().children()[0].buffers()[1]
            != second.descriptor().children()[0].buffers()[1], "retained batches reused data memory");
        require(((Number) first.valueAt(0, 0)).longValue() == 0,
            "later fetch overwrote an exported batch");
        require(NativeArrowBatch.liveBatchCount() == 2, "batch leases not counted");
      }
      session.close();
      require(((Number) first.valueAt(0, 2)).longValue() == 2,
          "query/session close freed first batch");
      require(((Number) second.valueAt(0, 2)).longValue() == 5,
          "query/session close freed second batch");
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        NativeArrowBatch release = first;
        executor.submit(release::close).get(10, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
      first.close();
      require(NativeArrowBatch.liveBatchCount() == 1, "idempotent cross-thread release changed count");
      second.close();
      require(NativeArrowBatch.liveBatchCount() == 0, "retained batch release leaked");
    } finally {
      if (first != null) first.close();
      if (second != null) second.close();
      session.close();
    }
  }

  private static void nestedAndEmpty(SparkJvmEngine engine) throws Exception {
    try (SparkJvmEngine.Session session = open(engine)) {
      Relation values = sql("SELECT 'hello 😀' AS text, CAST(NULL AS STRING) AS missing, "
          + "array(1, CAST(NULL AS INT), 3) AS items, map('x', 7) AS mapping, "
          + "named_struct('a', 1, 'b', 'nested') AS nested, "
          + "CAST('12.34' AS DECIMAL(10,2)) AS amount, "
          + "TIMESTAMP '2026-01-02 03:04:05' AS instant");
      try (SparkJvmEngine.Query query = prepare(engine, session, values)) {
        try (NativeArrowBatch batch = query.nextVectorBatch()) {
          require(batch != null && batch.descriptor().length() == 1, "nested row missing");
          NativeArrowBatch.Node[] fields = batch.descriptor().children();
          require(fields.length == 7, "nested schema fields");
          require(batch.valueAt(0, 0).toString().equals("hello 😀"), "UTF-8 value changed");
          require(batch.valueAt(1, 0) == null && fields[1].nullCount() == 1, "null validity changed");
          List<?> items = (List<?>) batch.valueAt(2, 0);
          require(items.size() == 3 && items.get(1) == null
              && ((Number) items.get(2)).intValue() == 3, "list values changed");
          require(fields[2].buffers().length == 2 && fields[2].children().length == 1,
              "list C Data layout");
          List<?> mapping = (List<?>) batch.valueAt(3, 0);
          Map<?, ?> entry = (Map<?, ?>) mapping.get(0);
          require(mapping.size() == 1 && entry.get("key").toString().equals("x")
              && ((Number) entry.get("value")).intValue() == 7, "map values changed");
          require(fields[3].buffers().length == 2 && fields[3].children().length == 1
              && fields[3].children()[0].children().length == 2, "map C Data layout");
          Map<?, ?> nested = (Map<?, ?>) batch.valueAt(4, 0);
          require(((Number) nested.get("a")).intValue() == 1, "struct values changed");
          require(fields[4].buffers().length == 1 && fields[4].children().length == 2,
              "struct C Data layout");
          require(new BigDecimal("12.34").equals(batch.valueAt(5, 0)), "decimal value changed");
          long expectedMicros = java.time.Instant.parse("2026-01-02T03:04:05Z")
              .getEpochSecond() * 1000000L;
          require(((Number) batch.valueAt(6, 0)).longValue() == expectedMicros
              && fields[6].buffers().length == 2, "timestamp value changed");
          for (int i = 0; i < fields.length; i++) {
            for (int j = 0; j < fields[i].buffers().length; j++) {
              require(fields[i].buffers()[j] == batch.vectorBufferAddress(i, j),
                  "top-level vector buffer was copied");
            }
          }
        }
        require(query.nextVectorBatch() == null && query.nextVectorBatch() == null, "EOF changed");
      }
      try (SparkJvmEngine.Query empty = prepare(engine, session,
          sql("SELECT CAST(NULL AS BIGINT) AS missing WHERE false"))) {
        require(empty.nextVectorBatch() == null, "empty query allocated a vector batch");
      }
    }
  }

  private static void require(boolean value, String message) {
    if (!value) throw new AssertionError(message);
  }
}
