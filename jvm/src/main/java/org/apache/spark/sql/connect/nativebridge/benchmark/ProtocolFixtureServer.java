package org.apache.spark.sql.connect.nativebridge.benchmark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.connect.proto.*;
import org.sparkproject.connect.grpc.Context;
import org.sparkproject.connect.grpc.Contexts;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.Server;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerInterceptor;
import org.sparkproject.connect.grpc.Status;
import org.sparkproject.connect.grpc.StatusRuntimeException;
import org.sparkproject.connect.grpc.netty.NettyServerBuilder;
import org.sparkproject.connect.grpc.stub.ServerCallStreamObserver;
import org.sparkproject.connect.grpc.stub.StreamObserver;
import org.sparkproject.connect.protobuf.ByteString;
import org.sparkproject.connect.protobuf.DynamicMessage;

/**
 * Synthetic protocol fixture using Spark's generated protobuf and shaded gRPC stack.
 * No SparkSession is created, and no plan is resolved or executed. IPC/schema bytes
 * are loaded once, outside measurement, from the Rust corpus's payload manifest.
 */
public final class ProtocolFixtureServer extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
  private static final int MAX_SESSIONS = 8192;
  private static final int MAX_REGISTRY = 131072;
  private static final int MAX_OPERATIONS = 4096;
  private static final int MAX_GLOBAL_OPERATIONS = 16384;
  private static final int MAX_ARTIFACTS = 4096;
  private static final long MAX_PAYLOAD = 8L * 1024 * 1024;
  private static final long MAX_BYTES = 64L * 1024 * 1024;
  private static final Context.Key<String> PAYLOAD = Context.key("protocol-benchmark-payload");
  private static final Metadata.Key<String> PAYLOAD_HEADER =
      Metadata.Key.of("x-bench-payload", Metadata.ASCII_STRING_MARSHALLER);
  private static final String[] ROUTES = {"ExecutePlan", "AnalyzePlan", "Config", "AddArtifacts",
      "ArtifactStatus", "Interrupt", "ReattachExecute", "ReleaseExecute", "ReleaseSession",
      "FetchErrorDetails", "CloneSession", "GetStatus"};

  private final Map<String, Payload> payloads;
  private final Map<String, Session> sessions = new HashMap<>();
  private final Set<String> tombstones = new HashSet<>();
  private final Map<String, AtomicLong> calls = new LinkedHashMap<>();
  private final AtomicLong retainedOperations = new AtomicLong();

  private ProtocolFixtureServer(Path directory) throws IOException {
    payloads = loadPayloads(directory);
    for (String route : ROUTES) calls.put(route, new AtomicLong());
  }

  /** Usage: ProtocolFixtureServer <payload-dir> [port=0] [threads=4]. */
  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 3) {
      throw new IllegalArgumentException("usage: ProtocolFixtureServer <payload-dir> [port] [threads]");
    }
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 0;
    int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
    if (port < 0 || port > 65535 || threads < 1 || threads > 256) {
      throw new IllegalArgumentException("invalid fixture port/thread count");
    }
    ProtocolFixtureServer fixture = new ProtocolFixtureServer(Path.of(args[0]));
    ExecutorService workers = Executors.newFixedThreadPool(threads);
    ServerInterceptor selection = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String selected = headers.get(PAYLOAD_HEADER);
        return Contexts.interceptCall(Context.current().withValue(PAYLOAD, selected), call, headers, next);
      }
    };
    Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
        .executor(workers).maxInboundMessageSize((int) MAX_BYTES)
        .maxConcurrentCallsPerConnection(1024).intercept(selection)
        .addService(GetStatusCompat.bind(fixture.bindService(), fixture::getStatus)).build().start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      server.shutdownNow();
      try { server.awaitTermination(5, TimeUnit.SECONDS); }
      catch (InterruptedException error) { Thread.currentThread().interrupt(); }
      fixture.shutdown();
      workers.shutdownNow();
      try {
        System.out.println("PROTOCOL_FIXTURE_STATS " + new ObjectMapper().writeValueAsString(fixture.stats()));
      } catch (IOException error) { throw new IllegalStateException(error); }
      System.out.println("PROTOCOL_FIXTURE_SHUTDOWN");
    }, "protocol-fixture-shutdown"));
    System.out.println("PROTOCOL_FIXTURE_LISTENING 127.0.0.1:" + server.getPort());
    server.awaitTermination();
  }

  private static final class Payload {
    final DataType schema;
    final ByteString ipc;
    final long rows;
    Payload(DataType schema, ByteString ipc, long rows) {
      this.schema = schema; this.ipc = ipc; this.rows = rows;
    }
  }

  private static Map<String, Payload> loadPayloads(Path directory) throws IOException {
    Path base = directory.toAbsolutePath().normalize();
    JsonNode manifest = new ObjectMapper().readTree(Files.readAllBytes(base.resolve("manifest.json")));
    if (!manifest.path("payloads").isArray()) throw new IOException("payload manifest needs payloads array");
    Map<String, Payload> result = new HashMap<>();
    long total = 0;
    for (JsonNode entry : manifest.path("payloads")) {
      String key = entry.path("key").asText();
      long rows = entry.path("rows").asLong(-1);
      String schemaName = entry.path("schema_file").asText();
      String arrowName = entry.path("arrow_file").asText();
      relativeName(schemaName); relativeName(arrowName);
      Path schema = base.resolve(schemaName), arrow = base.resolve(arrowName);
      long size = Files.size(arrow);
      total = Math.addExact(total, size);
      if (key.isEmpty() || rows < 0 || size > MAX_PAYLOAD || total > MAX_BYTES
          || Files.size(schema) > MAX_PAYLOAD) throw new IOException("fixture payload exceeds limits");
      Payload value = new Payload(DataType.parseFrom(Files.readAllBytes(schema)),
          ByteString.copyFrom(Files.readAllBytes(arrow)), rows);
      if (result.put(key, value) != null) throw new IOException("duplicate fixture payload key");
    }
    if (!result.containsKey("range")) throw new IOException("fixture needs range fallback payload");
    return Collections.unmodifiableMap(result);
  }

  private static void relativeName(String name) {
    Path path = Path.of(name);
    if (name.isEmpty() || name.length() > 4096 || path.isAbsolute()) throw invalid("name must be relative");
    for (Path part : path) {
      if (part.toString().equals(".") || part.toString().equals("..")) throw invalid("name must be relative");
    }
  }

  private static StatusRuntimeException invalid(String message) {
    return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
  }
  private static StatusRuntimeException missing(String message) {
    return Status.NOT_FOUND.withDescription(message).asRuntimeException();
  }
  private static StatusRuntimeException exhausted(String message) {
    return Status.RESOURCE_EXHAUSTED.withDescription(message).asRuntimeException();
  }
  private static void uuid(String id) {
    try { UUID.fromString(id); }
    catch (IllegalArgumentException error) { throw invalid("identifier must be a UUID"); }
  }
  private static String key(UserContext user, boolean hasUser, String id) {
    uuid(id);
    if (!hasUser) throw invalid("user_context is required");
    return user.getUserId() + '\0' + id;
  }
  private synchronized Session session(UserContext user, boolean hasUser, String id,
      String observed, boolean hasObserved) {
    String key = key(user, hasUser, id);
    if (hasObserved && !observed.equals(id)) {
      throw Status.FAILED_PRECONDITION.withDescription("fixture server session id differs").asRuntimeException();
    }
    if (tombstones.contains(key)) {
      throw Status.FAILED_PRECONDITION.withDescription("fixture session is released").asRuntimeException();
    }
    Session session = sessions.get(key);
    if (session == null) {
      if (sessions.size() >= MAX_SESSIONS || sessions.size() + tombstones.size() >= MAX_REGISTRY) {
        throw exhausted("fixture session limit");
      }
      session = new Session(id);
      sessions.put(key, session);
    }
    session.ensureOpen();
    return session;
  }
  private Payload payload(List<String> tags) {
    String selected = PAYLOAD.get();
    for (String tag : tags) {
      if (tag.startsWith("bench.payload=")) { selected = tag.substring("bench.payload=".length()); break; }
    }
    Payload result = payloads.get(selected == null ? "range" : selected);
    if (result == null) throw missing("unknown fixture payload");
    return result;
  }
  private void called(String route) { calls.get(route).incrementAndGet(); }
  private <T> void unary(String route, StreamObserver<T> observer, Supplier<T> action) {
    called(route);
    try { observer.onNext(action.get()); observer.onCompleted(); }
    catch (StatusRuntimeException error) { observer.onError(error); }
    catch (RuntimeException error) { observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); }
  }

  private final class Session {
    final String id;
    final Map<String, String> configs = new TreeMap<>();
    final Map<String, Long> artifacts = new HashMap<>();
    final Map<String, Operation> operations = new HashMap<>();
    boolean released;
    Session(String id) {
      this.id = id;
      configs.put("spark.sql.session.timeZone", "UTC");
      configs.put("spark.sql.shuffle.partitions", "2");
    }
    synchronized void ensureOpen() {
      if (released) throw Status.FAILED_PRECONDITION.withDescription("fixture session is released").asRuntimeException();
    }
    synchronized void close() {
      released = true;
      retainedOperations.addAndGet(-operations.size());
      for (Operation op : operations.values()) op.cancel();
      operations.clear(); configs.clear(); artifacts.clear();
    }
  }

  private static String responseId(int index) {
    return String.format("00000000-0000-4000-8000-%012x", index);
  }
  private static ExecutePlanResponse.Builder envelope(String session, String operation, int index) {
    return ExecutePlanResponse.newBuilder().setSessionId(session).setServerSideSessionId(session)
        .setOperationId(operation).setResponseId(responseId(index));
  }
  private static final class Operation {
    final List<ExecutePlanResponse> responses;
    final List<String> tags;
    final boolean hold;
    final Set<Writer> writers = ConcurrentHashMap.newKeySet();
    volatile boolean cancelled;
    int releasedThrough = -1;
    Operation(List<ExecutePlanResponse> responses, List<String> tags, boolean hold) {
      this.responses = responses; this.tags = tags; this.hold = hold;
    }
    void cancel() {
      cancelled = true;
      for (Writer writer : writers) writer.pump();
    }
  }
  /** gRPC readiness drives one immutable response at a time, with no result queue. */
  private static final class Writer {
    final Operation operation;
    final ServerCallStreamObserver<ExecutePlanResponse> observer;
    int index;
    boolean ended;
    Writer(Operation operation, StreamObserver<ExecutePlanResponse> observer, int start) {
      this.operation = operation;
      this.observer = (ServerCallStreamObserver<ExecutePlanResponse>) observer;
      index = start;
    }
    void start() {
      operation.writers.add(this);
      observer.setOnCancelHandler(this::clientCancelled);
      observer.setOnReadyHandler(this::pump);
      pump();
    }
    synchronized void clientCancelled() {
      ended = true;
      operation.writers.remove(this);
    }
    synchronized void pump() {
      if (ended) return;
      if (observer.isCancelled()) { clientCancelled(); return; }
      if (operation.cancelled) {
        ended = true; operation.writers.remove(this);
        observer.onError(Status.CANCELLED.withDescription("fixture operation interrupted").asRuntimeException());
        return;
      }
      while (observer.isReady() && !ended) {
        if (operation.hold && index > 0) return;
        if (index == operation.responses.size()) {
          ended = true; operation.writers.remove(this); observer.onCompleted(); return;
        }
        observer.onNext(operation.responses.get(index++));
      }
    }
  }

  @Override
  public void executePlan(ExecutePlanRequest input, StreamObserver<ExecutePlanResponse> observer) {
    called("ExecutePlan");
    try {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      if (!input.hasPlan() || input.getPlan().getOpTypeCase() == Plan.OpTypeCase.OPTYPE_NOT_SET) {
        throw invalid("plan is required");
      }
      int batches = 1;
      boolean hold = false;
      for (String tag : input.getTagsList()) {
        if (tag.isEmpty() || tag.indexOf(',') >= 0) throw invalid("invalid operation tag");
        if (tag.startsWith("bench.batches=")) {
          try { batches = Integer.parseInt(tag.substring("bench.batches=".length())); }
          catch (NumberFormatException error) { throw invalid("invalid batch count"); }
        }
        if (tag.equals("bench.hold=true")) hold = true;
      }
      if (batches < 1 || batches > 64) throw invalid("batch count must be 1..64");
      Payload payload = payload(input.getTagsList());
      String operationId = input.hasOperationId() ? input.getOperationId() : UUID.randomUUID().toString();
      uuid(operationId);
      List<ExecutePlanResponse> responses = new ArrayList<>(batches + 2);
      responses.add(envelope(session.id, operationId, 0).setSchema(payload.schema).build());
      for (int index = 0; index < batches; index++) {
        responses.add(envelope(session.id, operationId, index + 1).setArrowBatch(
            ExecutePlanResponse.ArrowBatch.newBuilder().setRowCount(payload.rows).setData(payload.ipc)
                .setStartOffset(Math.multiplyExact(payload.rows, index))).build());
      }
      responses.add(envelope(session.id, operationId, batches + 1)
          .setResultComplete(ExecutePlanResponse.ResultComplete.getDefaultInstance()).build());
      Operation operation = new Operation(responses, input.getTagsList(), hold);
      synchronized (session) {
        session.ensureOpen();
        if (session.operations.containsKey(operationId)) {
          throw Status.ALREADY_EXISTS.withDescription("fixture operation already exists").asRuntimeException();
        }
        if (session.operations.size() >= MAX_OPERATIONS) throw exhausted("fixture operation limit");
        if (retainedOperations.incrementAndGet() > MAX_GLOBAL_OPERATIONS) {
          retainedOperations.decrementAndGet(); throw exhausted("fixture global operation limit");
        }
        session.operations.put(operationId, operation);
      }
      new Writer(operation, observer, 0).start();
    } catch (StatusRuntimeException error) { observer.onError(error); }
    catch (RuntimeException error) { observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); }
  }

  @Override
  public void analyzePlan(AnalyzePlanRequest input, StreamObserver<AnalyzePlanResponse> observer) {
    unary("AnalyzePlan", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      Payload payload = payload(Collections.emptyList());
      AnalyzePlanResponse.Builder output = AnalyzePlanResponse.newBuilder()
          .setSessionId(session.id).setServerSideSessionId(session.id);
      switch (input.getAnalyzeCase()) {
        case SCHEMA:
          if (!input.getSchema().hasPlan()
              || input.getSchema().getPlan().getOpTypeCase() == Plan.OpTypeCase.OPTYPE_NOT_SET) {
            throw invalid("analysis plan is required");
          }
          output.setSchema(AnalyzePlanResponse.Schema.newBuilder().setSchema(payload.schema)); break;
        case IS_LOCAL: output.setIsLocal(AnalyzePlanResponse.IsLocal.newBuilder().setIsLocal(true)); break;
        case IS_STREAMING: output.setIsStreaming(AnalyzePlanResponse.IsStreaming.newBuilder().setIsStreaming(false)); break;
        case SPARK_VERSION: output.setSparkVersion(AnalyzePlanResponse.SparkVersion.newBuilder().setVersion("protocol-fixture/4.3")); break;
        case ANALYZE_NOT_SET: throw invalid("analysis is required");
        default: throw Status.UNIMPLEMENTED.withDescription("fixture does not resolve plans, DDL or SQL").asRuntimeException();
      }
      return output.build();
    });
  }

  @Override
  public void config(ConfigRequest input, StreamObserver<ConfigResponse> observer) {
    unary("Config", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      ConfigResponse.Builder output = ConfigResponse.newBuilder().setSessionId(session.id).setServerSideSessionId(session.id);
      synchronized (session) {
        session.ensureOpen();
        switch (input.getOperation().getOpTypeCase()) {
          case SET:
            for (KeyValue pair : input.getOperation().getSet().getPairsList()) {
              if (pair.getKey().isEmpty() || !pair.hasValue()) throw invalid("config key/value missing");
            }
            if (session.configs.size() + input.getOperation().getSet().getPairsCount() > MAX_ARTIFACTS) {
              throw exhausted("fixture config limit");
            }
            for (KeyValue pair : input.getOperation().getSet().getPairsList()) session.configs.put(pair.getKey(), pair.getValue());
            break;
          case GET:
            for (String name : input.getOperation().getGet().getKeysList()) {
              String value = session.configs.get(name);
              if (value == null) throw missing("fixture config key missing");
              output.addPairs(KeyValue.newBuilder().setKey(name).setValue(value));
            }
            break;
          case GET_OPTION:
            for (String name : input.getOperation().getGetOption().getKeysList()) {
              KeyValue.Builder pair = KeyValue.newBuilder().setKey(name);
              if (session.configs.containsKey(name)) pair.setValue(session.configs.get(name));
              output.addPairs(pair);
            }
            break;
          case GET_WITH_DEFAULT:
            for (KeyValue inputPair : input.getOperation().getGetWithDefault().getPairsList()) {
              KeyValue.Builder pair = KeyValue.newBuilder().setKey(inputPair.getKey());
              if (session.configs.containsKey(inputPair.getKey())) pair.setValue(session.configs.get(inputPair.getKey()));
              else if (inputPair.hasValue()) pair.setValue(inputPair.getValue());
              output.addPairs(pair);
            }
            break;
          case GET_ALL:
            String prefix = input.getOperation().getGetAll().getPrefix();
            session.configs.forEach((name, value) -> {
              if (name.startsWith(prefix)) {
                output.addPairs(KeyValue.newBuilder().setKey(name.substring(prefix.length())).setValue(value));
              }
            });
            break;
          case UNSET:
            for (String name : input.getOperation().getUnset().getKeysList()) session.configs.remove(name);
            break;
          case IS_MODIFIABLE:
            for (String name : input.getOperation().getIsModifiable().getKeysList()) {
              output.addPairs(KeyValue.newBuilder().setKey(name).setValue("true"));
            }
            break;
          default: throw invalid("config operation is required");
        }
      }
      return output.build();
    });
  }

  @Override
  public StreamObserver<AddArtifactsRequest> addArtifacts(StreamObserver<AddArtifactsResponse> observer) {
    called("AddArtifacts");
    ServerCallStreamObserver<AddArtifactsResponse> output = (ServerCallStreamObserver<AddArtifactsResponse>) observer;
    output.disableAutoRequest();
    Upload upload = new Upload();
    output.request(1);
    return new StreamObserver<AddArtifactsRequest>() {
      Session session;
      UserContext user;
      boolean ended;
      @Override
      public void onNext(AddArtifactsRequest input) {
        if (ended) return;
        try {
          if (session == null) {
            session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
                input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
            user = input.getUserContext();
          } else if (!session.id.equals(input.getSessionId()) || !user.equals(input.getUserContext())) {
            throw invalid("artifact stream changed session identity");
          }
          if (input.hasClientObservedServerSideSessionId()
              && !input.getClientObservedServerSideSessionId().equals(session.id)) {
            throw Status.FAILED_PRECONDITION.withDescription("fixture server session id differs").asRuntimeException();
          }
          session.ensureOpen(); upload.accept(input); output.request(1);
        } catch (StatusRuntimeException error) { ended = true; output.onError(error); }
        catch (RuntimeException error) { ended = true; output.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); }
      }
      @Override
      public void onError(Throwable error) { ended = true; }
      @Override
      public void onCompleted() {
        if (ended) return;
        ended = true;
        try {
          if (session == null) throw invalid("artifact stream is empty");
          if (upload.current != null) throw invalid("incomplete chunked artifact");
          synchronized (session) {
            session.ensureOpen();
            if (session.artifacts.size() + upload.staged.size() > MAX_ARTIFACTS) throw exhausted("fixture artifact limit");
            session.artifacts.putAll(upload.staged);
          }
          output.onNext(AddArtifactsResponse.newBuilder().setSessionId(session.id)
              .setServerSideSessionId(session.id).addAllArtifacts(upload.summaries).build());
          output.onCompleted();
        } catch (StatusRuntimeException error) { output.onError(error); }
      }
    };
  }
  private static final class Chunked {
    final String name;
    final long expectedBytes, expectedChunks;
    long bytes, chunks;
    boolean crcOk = true;
    Chunked(String name, long bytes, long chunks) {
      this.name = name; expectedBytes = bytes; expectedChunks = chunks;
    }
  }
  private static final class Upload {
    long bytes;
    final Map<String, Long> staged = new HashMap<>();
    final List<AddArtifactsResponse.ArtifactSummary> summaries = new ArrayList<>();
    Chunked current;
    boolean chunk(AddArtifactsRequest.ArtifactChunk chunk) {
      bytes = Math.addExact(bytes, chunk.getData().size());
      if (chunk.getData().size() > MAX_PAYLOAD || bytes > MAX_BYTES) throw exhausted("fixture artifact upload limit");
      CRC32 crc = new CRC32(); crc.update(chunk.getData().asReadOnlyByteBuffer());
      return crc.getValue() == chunk.getCrc();
    }
    void complete(String name, long size, boolean crcOk) {
      if (summaries.size() >= MAX_ARTIFACTS) throw exhausted("fixture artifact count limit");
      if (crcOk) staged.put(name, size);
      summaries.add(AddArtifactsResponse.ArtifactSummary.newBuilder().setName(name)
          .setIsCrcSuccessful(crcOk).build());
    }
    void accept(AddArtifactsRequest input) {
      switch (input.getPayloadCase()) {
        case BATCH:
          if (current != null) throw invalid("chunked artifact incomplete");
          for (AddArtifactsRequest.SingleChunkArtifact artifact : input.getBatch().getArtifactsList()) {
            relativeName(artifact.getName());
            if (!artifact.hasData()) throw invalid("artifact data missing");
            complete(artifact.getName(), artifact.getData().getData().size(), chunk(artifact.getData()));
          }
          break;
        case BEGIN_CHUNK:
          if (current != null) throw invalid("chunked artifact incomplete");
          AddArtifactsRequest.BeginChunkedArtifact begin = input.getBeginChunk();
          relativeName(begin.getName());
          if (begin.getTotalBytes() < 0 || begin.getNumChunks() < 0) throw invalid("negative artifact dimensions");
          if (begin.getNumChunks() == 0 || begin.getNumChunks() > 65536 || begin.getTotalBytes() > MAX_BYTES) {
            throw exhausted("fixture chunked artifact dimensions");
          }
          if (!begin.hasInitialChunk()) throw invalid("initial artifact chunk missing");
          current = new Chunked(begin.getName(), begin.getTotalBytes(), begin.getNumChunks());
          acceptChunk(begin.getInitialChunk());
          break;
        case CHUNK:
          if (current == null) throw invalid("chunk without begin");
          acceptChunk(input.getChunk());
          break;
        default: throw invalid("artifact payload missing");
      }
    }
    void acceptChunk(AddArtifactsRequest.ArtifactChunk chunk) {
      current.crcOk &= chunk(chunk);
      current.bytes = Math.addExact(current.bytes, chunk.getData().size());
      current.chunks++;
      if (current.bytes > current.expectedBytes || current.chunks > current.expectedChunks) {
        throw invalid("chunked artifact dimensions differ");
      }
      if (current.chunks == current.expectedChunks) {
        if (current.bytes != current.expectedBytes) throw invalid("chunked artifact byte count differs");
        complete(current.name, current.bytes, current.crcOk); current = null;
      }
    }
  }

  @Override
  public void artifactStatus(ArtifactStatusesRequest input, StreamObserver<ArtifactStatusesResponse> observer) {
    unary("ArtifactStatus", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      ArtifactStatusesResponse.Builder output = ArtifactStatusesResponse.newBuilder()
          .setSessionId(session.id).setServerSideSessionId(session.id);
      synchronized (session) {
        session.ensureOpen();
        for (String name : input.getNamesList()) output.putStatuses(name,
            ArtifactStatusesResponse.ArtifactStatus.newBuilder().setExists(session.artifacts.containsKey(name)).build());
      }
      return output.build();
    });
  }

  @Override
  public void reattachExecute(ReattachExecuteRequest input, StreamObserver<ExecutePlanResponse> observer) {
    called("ReattachExecute");
    try {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      Operation operation;
      int start = 0;
      synchronized (session) {
        session.ensureOpen(); operation = session.operations.get(input.getOperationId());
        if (operation == null) throw missing("fixture operation missing");
        if (input.hasLastResponseId()) {
          int found = responseIndex(operation, input.getLastResponseId());
          if (found < 0) throw missing("fixture response missing");
          start = found + 1;
        }
        if (start <= operation.releasedThrough) throw missing("fixture response was released");
      }
      new Writer(operation, observer, start).start();
    } catch (StatusRuntimeException error) { observer.onError(error); }
  }
  private static int responseIndex(Operation operation, String responseId) {
    for (int i = 0; i < operation.responses.size(); i++) {
      if (operation.responses.get(i).getResponseId().equals(responseId)) return i;
    }
    return -1;
  }

  @Override
  public void releaseExecute(ReleaseExecuteRequest input, StreamObserver<ReleaseExecuteResponse> observer) {
    unary("ReleaseExecute", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      ReleaseExecuteResponse.Builder output = ReleaseExecuteResponse.newBuilder()
          .setSessionId(session.id).setServerSideSessionId(session.id);
      synchronized (session) {
        session.ensureOpen(); Operation operation = session.operations.get(input.getOperationId());
        if (operation != null) output.setOperationId(input.getOperationId());
        switch (input.getReleaseCase()) {
          case RELEASE_ALL:
            if (operation != null) {
              session.operations.remove(input.getOperationId()); retainedOperations.decrementAndGet(); operation.cancel();
            }
            break;
          case RELEASE_UNTIL:
            if (operation != null) operation.releasedThrough = Math.max(operation.releasedThrough,
                responseIndex(operation, input.getReleaseUntil().getResponseId()));
            break;
          default: throw invalid("release selector is required");
        }
      }
      return output.build();
    });
  }

  @Override
  public void interrupt(InterruptRequest input, StreamObserver<InterruptResponse> observer) {
    unary("Interrupt", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      if (input.getInterruptTypeValue() < 1 || input.getInterruptTypeValue() > 3
          || (input.getInterruptTypeValue() == 2 && (!input.hasOperationTag() || input.getOperationTag().isEmpty()))
          || (input.getInterruptTypeValue() == 3 && !input.hasOperationId())) throw invalid("invalid interrupt selector");
      List<String> interrupted = new ArrayList<>();
      synchronized (session) {
        session.ensureOpen();
        session.operations.forEach((id, operation) -> {
          boolean selected = input.getInterruptTypeValue() == 1
              || (input.getInterruptTypeValue() == 2 && operation.tags.contains(input.getOperationTag()))
              || (input.getInterruptTypeValue() == 3 && id.equals(input.getOperationId()));
          if (operation.hold && !operation.cancelled && selected) { operation.cancel(); interrupted.add(id); }
        });
      }
      Collections.sort(interrupted);
      return InterruptResponse.newBuilder().setSessionId(session.id).setServerSideSessionId(session.id)
          .addAllInterruptedIds(interrupted).build();
    });
  }

  @Override
  public synchronized void releaseSession(ReleaseSessionRequest input, StreamObserver<ReleaseSessionResponse> observer) {
    unary("ReleaseSession", observer, () -> {
      String key = key(input.getUserContext(), input.hasUserContext(), input.getSessionId());
      if (!sessions.containsKey(key) && !tombstones.contains(key)
          && sessions.size() + tombstones.size() >= MAX_REGISTRY) throw exhausted("fixture session limit");
      Session session = sessions.remove(key);
      if (session != null) session.close();
      if (input.getAllowReconnect()) tombstones.remove(key); else tombstones.add(key);
      return ReleaseSessionResponse.newBuilder().setSessionId(input.getSessionId())
          .setServerSideSessionId(input.getSessionId()).build();
    });
  }

  @Override
  public void fetchErrorDetails(FetchErrorDetailsRequest input, StreamObserver<FetchErrorDetailsResponse> observer) {
    unary("FetchErrorDetails", observer, () -> {
      Session session = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      if (input.getErrorId().isEmpty()) throw invalid("error_id is required");
      FetchErrorDetailsResponse.Builder output = FetchErrorDetailsResponse.newBuilder()
          .setSessionId(session.id).setServerSideSessionId(session.id);
      if (input.getErrorId().equals("fixture-error")) {
        output.setRootErrorIdx(0).addErrors(FetchErrorDetailsResponse.Error.newBuilder()
            .addErrorTypeHierarchy("ProtocolFixtureError")
            .addErrorTypeHierarchy("java.lang.RuntimeException")
            .setMessage("synthetic protocol fixture error")
            .addStackTrace(FetchErrorDetailsResponse.StackTraceElement.newBuilder()
                .setDeclaringClass("ProtocolFixture").setMethodName("decode")
                .setFileName("fixture").setLineNumber(1))
            .setSparkThrowable(FetchErrorDetailsResponse.SparkThrowable.newBuilder()
                .setErrorClass("FIXTURE_ERROR").setSqlState("XX000")
                .putMessageParameters("kind", "synthetic")));
      }
      return output.build();
    });
  }

  @Override
  public synchronized void cloneSession(CloneSessionRequest input, StreamObserver<CloneSessionResponse> observer) {
    unary("CloneSession", observer, () -> {
      Session original = session(input.getUserContext(), input.hasUserContext(), input.getSessionId(),
          input.getClientObservedServerSideSessionId(), input.hasClientObservedServerSideSessionId());
      String newId = input.hasNewSessionId() ? input.getNewSessionId() : UUID.randomUUID().toString();
      String key = key(input.getUserContext(), input.hasUserContext(), newId);
      if (sessions.containsKey(key) || tombstones.contains(key)) {
        throw Status.ALREADY_EXISTS.withDescription("fixture cloned session already exists").asRuntimeException();
      }
      if (sessions.size() >= MAX_SESSIONS || sessions.size() + tombstones.size() >= MAX_REGISTRY) throw exhausted("fixture session limit");
      Session cloned = new Session(newId);
      synchronized (original) {
        original.ensureOpen(); cloned.configs.clear(); cloned.configs.putAll(original.configs);
        cloned.artifacts.putAll(original.artifacts);
      }
      sessions.put(key, cloned);
      return CloneSessionResponse.newBuilder().setSessionId(original.id).setServerSideSessionId(original.id)
          .setNewSessionId(newId).setNewServerSideSessionId(newId).build();
    });
  }

  private void getStatus(DynamicMessage message, StreamObserver<DynamicMessage> observer) {
    unary("GetStatus", observer, () -> {
      GetStatusCompat.Input input = new GetStatusCompat.Input(message);
      Session session = session(input.user, input.hasUser, input.session, input.observed, input.hasObserved);
      Map<String, Integer> statuses = new LinkedHashMap<>();
      synchronized (session) {
        session.ensureOpen();
        if (input.hasOperationStatus) {
          List<String> selected = input.operationIds;
          if (selected.isEmpty()) {
            // Fixture determinism: Spark's all-ID response does not promise sorting.
            // See benchmarks/protocol/GETSTATUS_CONTRACT.md for lifecycle differences.
            selected = new ArrayList<>(session.operations.keySet());
            Collections.sort(selected);
          }
          for (String id : selected) {
            Operation operation = session.operations.get(id);
            statuses.putIfAbsent(id,
                operation == null ? 1 : operation.cancelled ? 6 : operation.hold ? 2 : 4);
          }
        }
      }
      return GetStatusCompat.response(session.id, statuses);
    });
  }

  private synchronized Map<String, Object> stats() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("profile", "synthetic-protocol-fixture"); result.put("query_executions", 0);
    result.put("sessions", sessions.size()); result.put("tombstones", tombstones.size());
    result.put("retained_operations", retainedOperations.get());
    long artifacts = 0, bytes = 0;
    for (Session session : sessions.values()) synchronized (session) {
      artifacts += session.artifacts.size();
      for (long size : session.artifacts.values()) bytes += size;
    }
    result.put("artifacts", artifacts); result.put("verified_artifact_bytes", bytes);
    Map<String, Long> count = new LinkedHashMap<>();
    calls.forEach((name, value) -> count.put(name, value.get()));
    result.put("calls", count);
    result.put("unavailable_routes", Collections.emptyList());
    result.put("get_status_descriptor", GetStatusCompat.descriptorSource());
    return result;
  }
  private synchronized void shutdown() {
    for (Session session : sessions.values()) session.close();
    sessions.clear();
  }
}
