package org.apache.spark.sql.connect.nativebridge.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.connect.planner.SparkConnectPlanner;
import org.apache.spark.sql.connect.service.SessionHolder;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.arrow.ArrowConverters$;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.util.ArrowUtils$;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.execution.arrow.ArrowConverters.ArrowBatchWithSchemaIterator;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.connect.protobuf.Message;

import scala.Option;
import scala.collection.Iterator;
import scala.collection.AbstractIterator;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

/**
 * A query engine for the Rust-owned frontend. The JNI boundary passes Catalyst inputs and Arrow
 * batches, never service RPC requests, responses, observers, or operation registries.
 *
 * This intentionally targets the cached Spark 4.2 assembly. SessionHolder is a planner resource
 * container only: its service-managed close() and SparkSession.close() are never called.
 */
public final class SparkJvmEngine implements AutoCloseable {
  private static final int MAX_IPC_BYTES = 64 * 1024 * 1024;
  private static final long MAX_ESTIMATED_BATCH_BYTES = 4 * 1024 * 1024;
  private static final int MAX_BATCH_ROWS = 10000;
  private static final int MAX_CONFIG_FIELDS = 10000;
  private static final Set<String> RELATIONS = Set.of(
      "RANGE", "LOCAL_RELATION", "SQL", "PROJECT", "FILTER", "LIMIT", "OFFSET", "TAIL",
      "JOIN", "SORT", "AGGREGATE", "DEDUPLICATE", "SET_OP", "SUBQUERY_ALIAS", "TO_DF",
      "WITH_COLUMNS_RENAMED", "WITH_COLUMNS", "DROP", "REPARTITION",
      "REPARTITION_BY_EXPRESSION", "HINT", "READ");

  private final org.apache.spark.sql.classic.SparkSession base;
  private final SparkContext sparkContext;
  private final SparkEnv sparkEnv;
  private final ClassLoader classLoader;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
  private final Set<Task> tasks = ConcurrentHashMap.newKeySet();

  public SparkJvmEngine(SparkSession session) {
    if (!(Objects.requireNonNull(session, "session")
        instanceof org.apache.spark.sql.classic.SparkSession)) {
      throw new IllegalArgumentException("a classic Spark session is required");
    }
    base = (org.apache.spark.sql.classic.SparkSession) session;
    sparkContext = base.sparkContext();
    sparkEnv = SparkEnv.get();
    ClassLoader current = Thread.currentThread().getContextClassLoader();
    classLoader = current == null ? getClass().getClassLoader() : current;
  }

  /** Allocate this control before invoking a blocking planner or cursor method. */
  public Task newTask(String description) {
    ensureOpen();
    Task task = new Task(Objects.requireNonNull(description, "description"));
    tasks.add(task);
    if (closed.get()) task.cancel();
    return task;
  }

  public Session openSession(String userId, String sessionId, Task task) {
    Objects.requireNonNull(userId, "userId");
    Objects.requireNonNull(sessionId, "sessionId");
    return task.run(null, () -> {
      ensureOpen();
      Session session = new Session(userId, sessionId);
      sessions.add(session);
      if (closed.get() || task.isCancelled()) {
        session.close();
        task.check();
        ensureOpen();
      }
      task.complete();
      return session;
    });
  }

  private void ensureOpen() {
    if (closed.get()) throw new EngineException(9, "JVM engine is closed");
  }

  /** The caller must first drain the Rust server. The launcher owns the shared SparkContext. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      Throwable failure = null;
      for (Task task : tasks) {
        try { task.cancel(); } catch (Throwable error) { failure = recordFailure(failure, error); }
      }
      for (Session session : sessions) {
        try { session.close(); } catch (Throwable error) { failure = recordFailure(failure, error); }
      }
      throwFailure(failure);
    }
  }

  @FunctionalInterface
  private interface Action<T> { T run() throws Exception; }

  /** A small stable error surface for the native adapter's tonic Status mapping. */
  public static final class EngineException extends RuntimeException {
    private final int code;

    public EngineException(int code, String message) {
      super(message);
      this.code = code;
    }

    public EngineException(int code, String message, Throwable cause) {
      super(message, cause);
      this.code = code;
    }

    public int code() { return code; }
  }

  /** Cancellation is independent of a query/session lock and remains usable during preparation. */
  public final class Task implements AutoCloseable {
    private final String jobGroup = "ferrum-engine-" + UUID.randomUUID();
    private final String description;
    private boolean cancelled;
    private boolean completed;
    private Thread running;

    private Task(String description) { this.description = description; }

    public void cancel() {
      synchronized (this) {
        if (completed || cancelled) return;
        cancelled = true;
        if (running != null) running.interrupt();
      }
      // Cancel future submissions as well: cancellation can precede lazy job submission.
      sparkContext.cancelJobGroupAndFutureJobs(jobGroup);
    }

    private synchronized boolean isCancelled() { return cancelled; }

    private synchronized void check() {
      if (cancelled) throw new EngineException(1, "JVM engine operation was cancelled");
    }

    private synchronized void complete() { completed = true; }

    private <T> T run(Session session, Action<T> action) {
      Thread thread = Thread.currentThread();
      boolean previousInterrupted;
      synchronized (this) {
        check();
        if (running != null) throw new EngineException(9, "task is already executing");
        previousInterrupted = Thread.interrupted();
        running = thread;
      }
      Properties previousProperties = sparkContext.getLocalProperties();
      ClassLoader previousLoader = thread.getContextClassLoader();
      SparkEnv previousEnv = SparkEnv.get();
      Option<SparkSession> previousSession = SparkSession.getActiveSession();
      if (session != null) session.activeTasks.add(this);
      try {
        SparkEnv.set(sparkEnv);
        thread.setContextClassLoader(classLoader);
        sparkContext.setLocalProperties(new Properties());
        sparkContext.setJobGroup(jobGroup, description, true);
        SparkSession.setActiveSession(session == null ? base : session.spark);
        check();
        if (session == null) return action.run();
        session.ensureOpen();
        return session.holder.withSession(
            new AbstractFunction1<org.apache.spark.sql.classic.SparkSession, T>() {
              @Override
              public T apply(org.apache.spark.sql.classic.SparkSession ignored) {
                try {
                  check();
                  return action.run();
                } catch (RuntimeException error) {
                  throw error;
                } catch (InterruptedException error) {
                  throw new EngineException(1, "JVM engine operation was interrupted", error);
                } catch (Exception error) {
                  throw new EngineException(13, error.getMessage(), error);
                }
              }
            });
      } catch (InterruptedException error) {
        throw new EngineException(1, "JVM engine operation was interrupted", error);
      } catch (RuntimeException error) {
        if (isCancelled() && !(error instanceof EngineException)) {
          throw new EngineException(1, "JVM engine operation was cancelled", error);
        }
        throw error;
      } catch (Exception error) {
        throw new EngineException(13, error.getMessage(), error);
      } finally {
        if (session != null) session.activeTasks.remove(this);
        synchronized (this) { running = null; }
        Thread.interrupted();
        if (previousInterrupted) thread.interrupt();
        if (previousSession.isDefined()) SparkSession.setActiveSession(previousSession.get());
        else SparkSession.clearActiveSession();
        sparkContext.setLocalProperties(previousProperties);
        thread.setContextClassLoader(previousLoader);
        SparkEnv.set(previousEnv);
      }
    }

    @Override
    public void close() {
      try {
        cancel();
      } finally {
        tasks.remove(this);
      }
    }
  }

  /** Independent Catalyst catalog/configuration; identity and wire lifetime remain in Rust. */
  public final class Session implements AutoCloseable {
    private final org.apache.spark.sql.classic.SparkSession spark;
    private final SessionHolder holder;
    private final ReentrantLock catalog = new ReentrantLock();
    private final AtomicBoolean released = new AtomicBoolean();
    private final Set<Query> queries = ConcurrentHashMap.newKeySet();
    private final Set<Task> activeTasks = ConcurrentHashMap.newKeySet();

    private Session(String userId, String sessionId) {
      spark = base.newSession();
      holder = new SessionHolder(userId, sessionId, spark);
    }

    private void ensureOpen() {
      SparkJvmEngine.this.ensureOpen();
      if (released.get()) throw new EngineException(9, "JVM engine session is closed");
    }

    public Query prepare(byte[] relationBytes, Task task) {
      Objects.requireNonNull(relationBytes, "relationBytes");
      return task.run(this, () -> {
        catalog.lockInterruptibly();
        try {
          task.check();
          ensureOpen();
          Relation relation = Relation.parseFrom(relationBytes);
          validateRelations(relation);
          LogicalPlan logical = new SparkConnectPlanner(holder, Option.empty())
              .transformRelation(relation);
          if (logical.exists(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan node) {
              return node instanceof Command || node.getClass().getSimpleName().equals("CompoundBody");
            }
          })) {
            throw new EngineException(12, "SQL commands and external writes are unsupported");
          }
          Dataset<Row> frame = Dataset.ofRows(spark, logical);
          frame.queryExecution().assertAnalyzed();
          if (frame.isStreaming()) {
            throw new EngineException(12, "streaming queries are unsupported");
          }
          Query query = new Query(this, frame, task);
          queries.add(query);
          if (released.get() || task.isCancelled()) {
            query.close();
            task.check();
            ensureOpen();
          }
          return query;
        } finally {
          catalog.unlock();
        }
      });
    }

    /**
     * Internal typed fields: BE u32 pair count, then UTF-8 key length/data and nullable value
     * length/data (signed -1 means absent). No Spark Connect request/response envelopes cross here.
     */
    public byte[] config(int opcode, byte[] fields, Task task) {
      List<Pair> input = decodePairs(fields);
      return task.run(this, () -> {
        catalog.lockInterruptibly();
        try {
          task.check();
          ensureOpen();
          List<Pair> output = new ArrayList<>();
          switch (opcode) {
            case 1:
              for (Pair pair : input) output.add(new Pair(pair.key, spark.conf().get(pair.key)));
              break;
            case 2:
              for (Pair pair : input) {
                Option<String> value = spark.conf().getOption(pair.key);
                output.add(new Pair(pair.key, value.isDefined() ? value.get() : null));
              }
              break;
            case 3:
              for (Pair pair : input) {
                if (pair.value == null) throw new EngineException(3, "missing config default");
                output.add(new Pair(pair.key, spark.conf().get(pair.key, pair.value)));
              }
              break;
            case 4:
              if (input.size() > 1) throw new EngineException(3, "getAll accepts one prefix");
              String prefix = input.isEmpty() ? "" : input.get(0).key;
              spark.conf().getAllAsJava().entrySet().stream()
                  .filter(entry -> entry.getKey().startsWith(prefix))
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(entry -> output.add(new Pair(
                      entry.getKey().substring(prefix.length()), entry.getValue())));
              break;
            case 5:
              for (Pair pair : input) {
                output.add(new Pair(pair.key, Boolean.toString(spark.conf().isModifiable(pair.key))));
              }
              break;
            case 6:
            case 7:
              applyConfig(opcode, input, task);
              break;
            default:
              throw new EngineException(12, "unsupported config opcode: " + opcode);
          }
          task.complete();
          return encodePairs(output);
        } finally {
          catalog.unlock();
        }
      });
    }

    private void applyConfig(int opcode, List<Pair> input, Task task) {
      Map<String, String> previous = new LinkedHashMap<>();
      Map<String, String> explicit = spark.conf().getAllAsJava();
      for (Pair pair : input) {
        if (opcode == 6 && pair.value == null) throw new EngineException(3, "missing config value");
        if (!spark.conf().isModifiable(pair.key) && pair.key.startsWith("spark.")) {
          throw new EngineException(3, "configuration is not modifiable: " + pair.key);
        }
        if (!previous.containsKey(pair.key)) previous.put(pair.key, explicit.get(pair.key));
      }
      synchronized (task) {
        task.check();
        try {
          for (Pair pair : input) {
            if (opcode == 6) spark.conf().set(pair.key, pair.value);
            else spark.conf().unset(pair.key);
          }
          task.complete();
        } catch (Throwable error) {
          for (Map.Entry<String, String> entry : previous.entrySet()) {
            try {
              if (entry.getValue() == null) spark.conf().unset(entry.getKey());
              else spark.conf().set(entry.getKey(), entry.getValue());
            } catch (Throwable rollback) {
              if (rollback != error) error.addSuppressed(rollback);
            }
          }
          throw error;
        }
      }
    }

    @Override
    public void close() {
      if (released.compareAndSet(false, true)) {
        Throwable failure = null;
        for (Task task : activeTasks) {
          try { task.cancel(); } catch (Throwable error) { failure = recordFailure(failure, error); }
        }
        for (Query query : queries) {
          try { query.close(); } catch (Throwable error) { failure = recordFailure(failure, error); }
        }
        catalog.lock();
        try {
          // These resources belong to this newSession only. Never close SessionHolder or Spark.
          try {
            spark.sessionState().catalog().clearTempTables();
          } catch (Throwable error) {
            failure = recordFailure(failure, error);
          }
          try {
            spark.artifactManager().close();
          } catch (Throwable error) {
            failure = recordFailure(failure, error);
          }
        } finally {
          sessions.remove(this);
          catalog.unlock();
        }
        throwFailure(failure);
      }
    }
  }

  /** An analyzed Catalyst query and a lazy pull cursor. Only one caller may fetch at a time. */
  public final class Query implements AutoCloseable {
    private final Session session;
    private final QueryExecution execution;
    private final StructType schema;
    private final Task task;
    private final String timeZone;
    private final boolean largeVarTypes;
    private final int maxRows;
    private final byte[] schemaIpc;
    private final boolean local;
    private final ReentrantLock cursor = new ReentrantLock();
    private boolean released;
    private boolean ended;
    private SparkPlan plan;
    private Iterator<InternalRow> rows;
    private ArrowBatchWithSchemaIterator batches;
    private int resultEncoding;

    private Query(Session session, Dataset<Row> frame, Task task) {
      this.session = session;
      this.execution = frame.queryExecution();
      this.schema = frame.schema();
      this.task = task;
      this.timeZone = session.spark.sessionState().conf().sessionLocalTimeZone();
      this.largeVarTypes = session.spark.sessionState().conf().arrowUseLargeVarTypes();
      int configured = session.spark.sessionState().conf().arrowMaxRecordsPerBatch();
      this.maxRows = configured > 0 ? configured : MAX_BATCH_ROWS;
      this.local = frame.isLocal();
      this.schemaIpc = checkedIpc(ArrowConverters$.MODULE$.createEmptyArrowBatch(
          schema, timeZone, false, largeVarTypes));
    }

    public byte[] schemaIpc() { return schemaIpc.clone(); }
    public boolean isLocal() { return local; }
    public boolean isStreaming() { return false; }

    public byte[] nextBatch() {
      return task.run(session, () -> {
        cursor.lockInterruptibly();
        try {
          if (released) throw new EngineException(9, "query cursor is closed");
          if (ended) return null;
          selectEncoding(1);
          task.check();
          // Each JNI fetch is a separate SQL execution scope. A single scope spanning a cursor
          // would require a persistent Java worker. Session/job-group state is restored per fetch.
          return SQLExecution.withNewExecutionId(execution, Option.apply("ferrum-engine-nextBatch"),
              new AbstractFunction0<byte[]>() {
                @Override
                public byte[] apply() { return pull(); }
              });
        } finally {
          cursor.unlock();
        }
      });
    }

    private void initializeRows() {
      task.check();
      if (rows == null) {
        plan = execution.executedPlan();
        Iterator<InternalRow> raw = plan.executeToIterator();
        // Spark's iterator can materialize one partition. Arrow conversion below bounds each
        // JNI batch; it does not establish a memory bound for Spark partition execution.
        rows = new AbstractIterator<InternalRow>() {
          @Override
          public boolean hasNext() { task.check(); return raw.hasNext(); }
          @Override
          public InternalRow next() { task.check(); return raw.next(); }
        };
      }
    }

    private byte[] pull() {
      initializeRows();
      if (!rows.hasNext()) {
        closeBatches();
        ended = true;
        task.complete();
        return null;
      }
      if (batches == null) {
        batches = ArrowConverters$.MODULE$.toBatchWithSchemaIterator(
            rows, schema, maxRows, MAX_ESTIMATED_BATCH_BYTES, timeZone, false, largeVarTypes);
      }
      // Do not call batches.hasNext(): it closes its allocator implicitly at EOF. This cursor
      // owns that close explicitly so EOF and cancellation cannot close the allocator twice.
      return checkedIpc(batches.next());
    }

    /** Native zero-copy result path. Only immutable vector descriptors cross JNI. */
    public NativeArrowBatch nextVectorBatch() {
      NativeArrowBatch[] pending = new NativeArrowBatch[1];
      try {
        return task.run(session, () -> {
          cursor.lockInterruptibly();
          try {
            if (released) throw new EngineException(9, "query cursor is closed");
            if (ended) return null;
            selectEncoding(2);
            task.check();
            return SQLExecution.withNewExecutionId(execution,
                Option.apply("ferrum-engine-nextVectorBatch"),
                new AbstractFunction0<NativeArrowBatch>() {
                  @Override
                  public NativeArrowBatch apply() {
                    pending[0] = pullVectors();
                    return pending[0];
                  }
                });
          } finally {
            cursor.unlock();
          }
        });
      } catch (Throwable error) {
        // A failure while leaving the SQL/task scopes must not leak a batch JNI never received.
        if (pending[0] != null) {
          try { pending[0].close(); } catch (Throwable cleanup) { recordFailure(error, cleanup); }
        }
        throw error;
      }
    }

    private void selectEncoding(int encoding) {
      if (resultEncoding != 0 && resultEncoding != encoding) {
        throw new EngineException(9, "query result encodings cannot be mixed");
      }
      resultEncoding = encoding;
    }

    private NativeArrowBatch pullVectors() {
      initializeRows();
      if (!rows.hasNext()) {
        ended = true;
        task.complete();
        return null;
      }
      // Exported batches have independent allocators: the next fetch never resets or overwrites
      // vectors still owned by native Arrow arrays, and query/session close cannot free them.
      BufferAllocator allocator = ArrowUtils$.MODULE$.rootAllocator()
          .newChildAllocator("ferrum-native-arrow-batch", 0, MAX_IPC_BYTES);
      VectorSchemaRoot root = null;
      try {
        root = VectorSchemaRoot.create(ArrowUtils$.MODULE$.toArrowSchema(
            schema, timeZone, false, largeVarTypes), allocator);
        ArrowWriter writer = ArrowWriter.create(root);
        long estimatedBytes = 0;
        int count = 0;
        while (count < maxRows && (count == 0 || estimatedBytes < MAX_ESTIMATED_BATCH_BYTES)
            && rows.hasNext()) {
          task.check();
          InternalRow row = rows.next();
          long size = row instanceof UnsafeRow ? ((UnsafeRow) row).getSizeInBytes()
              : (long) row.numFields() * 16;
          if (size > MAX_IPC_BYTES) {
            throw new EngineException(8, "Arrow row exceeds 64 MiB native batch limit");
          }
          writer.write(row);
          estimatedBytes += size;
          count++;
        }
        writer.finish();
        NativeArrowBatch batch = new NativeArrowBatch(root, allocator);
        root = null; // Ownership is now solely the returned immutable batch lease.
        return batch;
      } catch (Throwable error) {
        if (root != null) {
          try { root.close(); } catch (Throwable cleanup) { recordFailure(error, cleanup); }
        }
        try { allocator.close(); } catch (Throwable cleanup) { recordFailure(error, cleanup); }
        throw error;
      }
    }

    private void closeBatches() {
      if (batches != null) {
        ArrowBatchWithSchemaIterator previous = batches;
        batches = null;
        previous.close();
      }
    }

    @Override
    public void close() {
      Throwable failure = null;
      try { task.close(); } catch (Throwable error) { failure = error; }
      cursor.lock();
      try {
        if (!released) {
          released = true;
          try {
            closeBatches();
          } catch (Throwable error) {
            failure = recordFailure(failure, error);
          }
          try {
            rows = null;
            if (plan != null) plan.cleanupResources();
          } catch (Throwable error) {
            failure = recordFailure(failure, error);
          } finally {
            session.queries.remove(this);
          }
        }
      } finally {
        cursor.unlock();
      }
      throwFailure(failure);
    }
  }

  private static byte[] checkedIpc(byte[] bytes) {
    if (bytes.length > MAX_IPC_BYTES) throw new EngineException(8, "Arrow batch exceeds 64 MiB");
    return bytes;
  }

  private static Throwable recordFailure(Throwable previous, Throwable error) {
    if (previous == null) return error;
    if (previous != error) previous.addSuppressed(error);
    return previous;
  }

  private static void throwFailure(Throwable failure) {
    if (failure instanceof Error) throw (Error) failure;
    if (failure instanceof RuntimeException) throw (RuntimeException) failure;
    if (failure != null) throw new EngineException(13, "JVM engine cleanup failed", failure);
  }

  private static void validateRelations(Message message) {
    if (message instanceof Relation) {
      Relation relation = (Relation) message;
      String kind = relation.getRelTypeCase().name();
      if (!RELATIONS.contains(kind)) {
        throw new EngineException(12, "unsupported JVM engine relation: " + kind);
      }
      if (relation.hasRead() && !relation.getRead().hasNamedTable()) {
        throw new EngineException(12, "only named-table reads are supported in this prototype");
      }
    }
    String type = message.getDescriptorForType().getFullName();
    if (type.endsWith("CommonInlineUserDefinedFunction") || type.endsWith("CommonInlineUserDefinedTableFunction")) {
      throw new EngineException(12, "user-defined functions are unsupported in this prototype");
    }
    for (Object field : message.getAllFields().values()) {
      if (field instanceof Message) validateRelations((Message) field);
      else if (field instanceof List<?>) {
        for (Object item : (List<?>) field) {
          if (item instanceof Message) validateRelations((Message) item);
        }
      }
    }
  }

  private static final class Pair {
    private final String key;
    private final String value;
    private Pair(String key, String value) { this.key = key; this.value = value; }
  }

  private static List<Pair> decodePairs(byte[] bytes) {
    Objects.requireNonNull(bytes, "fields");
    if (bytes.length > MAX_IPC_BYTES) throw new EngineException(8, "config fields exceed 64 MiB");
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int count = input.readInt();
      if (count < 0 || count > MAX_CONFIG_FIELDS || count > input.available() / 8) {
        throw new EngineException(3, "invalid config field count");
      }
      List<Pair> result = new ArrayList<>(count);
      for (int i = 0; i < count; i++) result.add(new Pair(readString(input, false), readString(input, true)));
      if (input.available() != 0) throw new EngineException(3, "trailing config field bytes");
      return result;
    } catch (IOException error) {
      throw new EngineException(3, "invalid config fields", error);
    }
  }

  private static String readString(DataInputStream input, boolean nullable) throws IOException {
    int len = input.readInt();
    if (len == -1 && nullable) return null;
    if (len < 0 || len > input.available()) throw new EngineException(3, "invalid config string length");
    byte[] bytes = input.readNBytes(len);
    try {
      return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException error) {
      throw new EngineException(3, "invalid config UTF-8", error);
    }
  }

  private static byte[] encodePairs(List<Pair> pairs) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(pairs.size());
    for (Pair pair : pairs) {
      writeString(output, pair.key);
      writeString(output, pair.value);
    }
    return bytes.toByteArray();
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    if (value == null) output.writeInt(-1);
    else {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      output.writeInt(bytes.length);
      output.write(bytes);
    }
  }
}
