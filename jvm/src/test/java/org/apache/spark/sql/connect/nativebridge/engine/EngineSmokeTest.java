package org.apache.spark.sql.connect.nativebridge.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.connect.proto.Range;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.connect.proto.SubqueryAlias;
import org.apache.spark.connect.proto.WithRelations;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService$;
import org.apache.spark.sql.util.ArrowUtils$;

/** Direct Catalyst/Arrow tests. This program does not load the native library. */
public final class EngineSmokeTest {
  private EngineSmokeTest() {}

  public static void main(String[] arguments) throws Exception {
    shutdownHelpers();
    SparkSession spark = SparkSession.builder().appName("Typed JVM Engine Smoke")
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    try (SparkJvmEngine engine = new SparkJvmEngine(spark)) {
      SparkJvmEngine.Session first = open(engine, "session-one");
      SparkJvmEngine.Session second = open(engine, "session-two");
      try {
        rangeAndEmpty(engine, first);
        configAtomicityAndIsolation(engine, first, second);
        cancelledBeforePrepare(engine, first);
        commandsCannotMutateCatalog(engine, first);
        earlyCloseKeepsContext(engine, first, second, spark);
        cancelRunningFetch(engine, first, second, spark);
        require(SparkConnectService$.MODULE$.server() == null, "Java gRPC listener was started");
        System.out.println("ENGINE_SMOKE_OK: Catalyst range and SQL; schema and Arrow batches; "
            + "config rollback/isolation; pre-cancel and running fetch cancellation; command/script/"
            + "WITH_RELATIONS rejection; early close and shared-context survival; launcher teardown");
      } finally {
        first.close();
        second.close();
      }
      require(!spark.sparkContext().isStopped(), "session release stopped the shared SparkContext");
    } finally {
      spark.stop();
    }
  }

  private static SparkJvmEngine.Session open(SparkJvmEngine engine, String id) {
    try (SparkJvmEngine.Task task = engine.newTask("open " + id)) {
      return engine.openSession("smoke-user", id, task);
    }
  }

  private static Relation range(long count) {
    return Relation.newBuilder().setRange(Range.newBuilder()
        .setStart(0).setEnd(count).setStep(1).setNumPartitions(2)).build();
  }

  private static Relation sql(String text) {
    return Relation.newBuilder().setSql(SQL.newBuilder().setQuery(text)).build();
  }

  private static SparkJvmEngine.Query query(SparkJvmEngine engine, SparkJvmEngine.Session session,
      Relation relation) {
    SparkJvmEngine.Task task = engine.newTask("prepare smoke query");
    try {
      return session.prepare(relation.toByteArray(), task);
    } catch (Throwable error) {
      task.close();
      throw error;
    }
  }

  private static void rangeAndEmpty(SparkJvmEngine engine, SparkJvmEngine.Session session)
      throws Exception {
    try (SparkJvmEngine.Query query = query(engine, session, range(25))) {
      require(!query.isStreaming(), "range became streaming");
      try (BufferAllocator allocator = allocator();
           ArrowStreamReader reader = new ArrowStreamReader(
               new ByteArrayInputStream(query.schemaIpc()), allocator)) {
        require(reader.getVectorSchemaRoot().getSchema().getFields().get(0).getName().equals("id"),
            "prepared schema is missing the range id");
        require(reader.loadNextBatch(), "schema IPC did not include its empty record batch");
        require(reader.getVectorSchemaRoot().getRowCount() == 0, "prepare executed rows");
      }
      long[] result = consume(query);
      require(Arrays.equals(result, new long[] {25, 300}), "range Arrow values changed");
      require(query.nextBatch() == null, "EOF was not stable");
    }
    try (SparkJvmEngine.Query empty = query(engine, session,
        sql("SELECT CAST(NULL AS BIGINT) AS missing WHERE false"))) {
      require(empty.nextBatch() == null, "empty SQL query returned rows");
    }
    try (SparkJvmEngine.Query join = query(engine, session,
        sql("SELECT sum(a.id + b.id) AS total FROM range(5) a JOIN range(5) b ON a.id = b.id"))) {
      require(Arrays.equals(consume(join), new long[] {1, 20}), "Catalyst SQL join changed");
    }
  }

  private static long[] consume(SparkJvmEngine.Query query) throws Exception {
    long rows = 0;
    long sum = 0;
    byte[] bytes;
    while ((bytes = query.nextBatch()) != null) {
      try (BufferAllocator allocator = allocator();
           ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
        int batches = 0;
        while (reader.loadNextBatch()) {
          batches++;
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          for (int row = 0; row < root.getRowCount(); row++) {
            rows++;
            Object value = root.getVector(0).getObject(row);
            if (value != null) sum += ((Number) value).longValue();
          }
        }
        require(batches == 1, "nextBatch did not produce one standalone Arrow batch");
      }
    }
    return new long[] {rows, sum};
  }

  private static BufferAllocator allocator() {
    return ArrowUtils$.MODULE$.rootAllocator().newChildAllocator("engine-smoke", 0, Long.MAX_VALUE);
  }

  private static Map<String, String> config(SparkJvmEngine engine, SparkJvmEngine.Session session,
      int opcode, String... alternating) throws Exception {
    try (SparkJvmEngine.Task task = engine.newTask("config smoke")) {
      return decode(session.config(opcode, encode(alternating), task));
    }
  }

  private static void configAtomicityAndIsolation(SparkJvmEngine engine,
      SparkJvmEngine.Session first, SparkJvmEngine.Session second) throws Exception {
    config(engine, first, 6, "ferrum.smoke", "hello 😀");
    require(config(engine, first, 1, "ferrum.smoke", null).get("ferrum.smoke").equals("hello 😀"),
        "config UTF-8 changed");
    require(config(engine, second, 2, "ferrum.smoke", null).get("ferrum.smoke") == null,
        "configuration leaked between sessions");
    require(config(engine, first, 3, "ferrum.default", "fallback").get("ferrum.default").equals("fallback"),
        "config default was ignored");
    require(config(engine, first, 5, "spark.sql.shuffle.partitions", null)
        .get("spark.sql.shuffle.partitions").equals("true"), "modifiable config is marked static");
    Map<String, String> before = config(engine, first, 4);
    try {
      config(engine, first, 6, "ferrum.smoke", "mutated", "ferrum.rollback.new", "new",
          "spark.sql.shuffle.partitions", "not-a-number");
      throw new AssertionError("invalid config batch was accepted");
    } catch (RuntimeException expected) {
      require(config(engine, first, 4).equals(before), "failed config batch was not rolled back");
    }
    config(engine, first, 7, "ferrum.smoke", null);
    require(config(engine, first, 2, "ferrum.smoke", null).get("ferrum.smoke") == null,
        "config unset failed");
  }

  private static void cancelledBeforePrepare(SparkJvmEngine engine, SparkJvmEngine.Session session) {
    try (SparkJvmEngine.Task task = engine.newTask("pre-cancel")) {
      task.cancel();
      expectCode(1, () -> session.prepare(range(1).toByteArray(), task));
    }
  }

  private static void commandsCannotMutateCatalog(SparkJvmEngine engine,
      SparkJvmEngine.Session session) {
    reject(engine, session, sql("CREATE TEMP VIEW engine_forbidden_command AS SELECT 1 AS id"));
    missing(engine, session, "engine_forbidden_command");
    reject(engine, session, sql(
        "BEGIN CREATE TEMP VIEW engine_forbidden_script AS SELECT 1 AS id; END"));
    missing(engine, session, "engine_forbidden_script");
    reject(engine, session, Relation.newBuilder().setWithRelations(WithRelations.newBuilder()
        .setRoot(sql("CREATE TEMP VIEW engine_forbidden_refs AS SELECT 1 AS id"))
        .addReferences(Relation.newBuilder().setSubqueryAlias(SubqueryAlias.newBuilder()
            .setAlias("unused_ref").setInput(range(1))))).build());
    missing(engine, session, "engine_forbidden_refs");
  }

  private static void reject(SparkJvmEngine engine, SparkJvmEngine.Session session, Relation relation) {
    try (SparkJvmEngine.Task task = engine.newTask("reject side effects")) {
      expectCode(12, () -> session.prepare(relation.toByteArray(), task));
    }
  }

  private static void missing(SparkJvmEngine engine, SparkJvmEngine.Session session, String table) {
    try (SparkJvmEngine.Task task = engine.newTask("assert missing view")) {
      try (SparkJvmEngine.Query ignored = session.prepare(
          sql("SELECT * FROM " + table).toByteArray(), task)) {
        throw new AssertionError("rejected SQL created view " + table);
      } catch (RuntimeException expected) {
        require(expected.getMessage().contains(table), "unexpected missing-view failure");
      }
    }
  }

  private static void earlyCloseKeepsContext(SparkJvmEngine engine,
      SparkJvmEngine.Session first, SparkJvmEngine.Session second, SparkSession spark)
      throws Exception {
    try (SparkJvmEngine.Query partial = query(engine, first, range(100000))) {
      require(partial.nextBatch() != null, "early-close query returned no batch");
    }
    require(!spark.sparkContext().isStopped(), "cursor close stopped the shared SparkContext");
    try (SparkJvmEngine.Query other = query(engine, second, sql("SELECT 7 AS value"))) {
      require(Arrays.equals(consume(other), new long[] {1, 7}), "another session broke after close");
    }
  }

  private static void cancelRunningFetch(SparkJvmEngine engine, SparkJvmEngine.Session first,
      SparkJvmEngine.Session second, SparkSession spark) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    SparkJvmEngine.Task task = engine.newTask("running cancellation");
    SparkJvmEngine.Query query = first.prepare(
        sql("SELECT sum(id) FROM range(1000000000000)").toByteArray(), task);
    try {
      Future<byte[]> fetch = executor.submit(query::nextBatch);
      long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (spark.sparkContext().statusTracker().getActiveJobIds().length == 0
          && !fetch.isDone() && System.nanoTime() < limit) Thread.sleep(10);
      require(!fetch.isDone(), "long query finished before cancellation");
      require(spark.sparkContext().statusTracker().getActiveJobIds().length != 0,
          "long query did not submit a Spark job");
      task.cancel();
      try {
        fetch.get(10, TimeUnit.SECONDS);
        throw new AssertionError("running fetch survived cancellation");
      } catch (ExecutionException error) {
        require(error.getCause() instanceof SparkJvmEngine.EngineException
            && ((SparkJvmEngine.EngineException) error.getCause()).code() == 1,
            "running cancellation returned the wrong error: " + error.getCause());
      }
    } finally {
      query.close();
      task.close();
      executor.shutdownNow();
      require(executor.awaitTermination(10, TimeUnit.SECONDS), "fetch worker did not stop");
    }
    try (SparkJvmEngine.Query other = query(engine, second, sql("SELECT 3 AS value"))) {
      require(Arrays.equals(consume(other), new long[] {1, 3}), "cancellation broke another session");
    }
  }

  private static void shutdownHelpers() {
    AtomicInteger cleaned = new AtomicInteger();
    CountDownLatch failed = new CountDownLatch(1);
    require(!EmbeddedSparkEngine.shutdown(() -> { throw new IllegalStateException("expected stop failure"); },
        cleaned::incrementAndGet, cleaned::incrementAndGet, failed, false),
        "failed native shutdown was accepted");
    require(failed.getCount() == 0 && cleaned.get() == 0,
        "native failure did not retain resources or release its waiter");
    CountDownLatch startup = new CountDownLatch(1);
    require(EmbeddedSparkEngine.shutdown(() -> {}, cleaned::incrementAndGet,
        cleaned::incrementAndGet, startup, true), "startup cleanup failed");
    require(startup.getCount() == 0 && cleaned.get() == 2, "startup cleanup did not finish");
  }

  private static byte[] encode(String... fields) throws Exception {
    require(fields.length % 2 == 0, "config fields must be pairs");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(fields.length / 2);
    for (String value : fields) {
      if (value == null) output.writeInt(-1);
      else {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(data.length);
        output.write(data);
      }
    }
    return bytes.toByteArray();
  }

  private static Map<String, String> decode(byte[] bytes) throws Exception {
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
    int count = input.readInt();
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < count; i++) result.put(readString(input), readString(input));
    require(input.available() == 0, "trailing config bytes");
    return result;
  }

  private static String readString(DataInputStream input) throws Exception {
    int len = input.readInt();
    return len == -1 ? null : new String(input.readNBytes(len), StandardCharsets.UTF_8);
  }

  private static void expectCode(int code, Runnable action) {
    try {
      action.run();
      throw new AssertionError("operation did not fail with code " + code);
    } catch (SparkJvmEngine.EngineException error) {
      require(error.code() == code, "unexpected engine error: " + error);
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
