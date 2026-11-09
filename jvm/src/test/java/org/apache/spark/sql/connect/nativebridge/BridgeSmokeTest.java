package org.apache.spark.sql.connect.nativebridge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkEnv;
import org.apache.spark.connect.proto.ConfigRequest;
import org.apache.spark.connect.proto.ConfigResponse;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.Plan;
import org.apache.spark.connect.proto.Range;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.UserContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService$;
import org.sparkproject.connect.grpc.Context;
import org.sparkproject.connect.grpc.InternalMetadata;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.MethodDescriptor;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerMethodDefinition;
import org.sparkproject.connect.grpc.Status;

/** Standalone integration checks; run through jvm/test.sh with an existing Spark assembly. */
public final class BridgeSmokeTest {
  private static final String SERVICE = "spark.connect.SparkConnectService/";
  private static final ExecutorService WORKERS = Executors.newCachedThreadPool();
  private static final MethodDescriptor.Marshaller<Object> BYTES =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(Object value) {
          return new ByteArrayInputStream((byte[]) value);
        }

        @Override
        public Object parse(InputStream stream) {
          try {
            return stream.readAllBytes();
          } catch (java.io.IOException error) {
            throw new IllegalStateException(error);
          }
        }
      };

  private BridgeSmokeTest() {}

  public static void main(String[] arguments) throws Exception {
    SparkSession session = SparkSession.builder()
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
        .getOrCreate();
    try (SparkConnectBridge bridge = new SparkConnectBridge(session)) {
      require(SparkEnv.get().conf().getBoolean("spark.connect.grpc.debug.enabled", false),
          "run this smoke with spark.connect.grpc.debug.enabled=true");
      require(SparkConnectService$.MODULE$.server() == null, "Java gRPC server was started");
      abnormalOpen(bridge);
      shutdownGuards();
      realHandlers(bridge);
      metadata(bridge);
      boundedOutput(bridge);
      cancelBlockedOutput(bridge);
      cancelBlockedInput(bridge);
      cancelHeldCallback(bridge);
      deadlinesAndUnknownMethods(bridge);
      require(SparkConnectService$.MODULE$.server() == null, "Java gRPC server appeared");
      System.out.println("BRIDGE_SMOKE_OK: real Config/ExecutePlan; metadata; bounded output; "
          + "cancel blocked output/input/callback; deadlines; unknown methods; abnormal open cleanup; "
          + "shutdown guards; debug enabled; no Java gRPC server");
    } finally {
      WORKERS.shutdownNow();
      session.stop();
    }
  }

  private static void abnormalOpen(SparkConnectBridge bridge) throws Exception {
    for (boolean deferred : new boolean[] {false, true}) {
      AssertionError expected = new AssertionError("synthetic initialization error");
      AtomicReference<SparkConnectBridge.Call> captured = new AtomicReference<>();
      String name = deferred ? "open-ready-error" : "open-start-error";
      register(bridge, name, (call, headers) -> {
        SparkConnectBridge.Call direct = (SparkConnectBridge.Call) call;
        captured.set(direct);
        if (!deferred) {
          throw expected;
        }
        call.sendHeaders(new Metadata());
        direct.poll(0); // Defer onReady until startCall has returned its listener.
        return new ServerCall.Listener<>() {
          private boolean failed;

          @Override
          public void onReady() {
            if (!failed) {
              failed = true;
              throw expected;
            }
          }
        };
      });
      try {
        bridge.open("bridge.test/" + name, null, TimeUnit.MINUTES.toNanos(5));
        throw new AssertionError("open swallowed the initialization Error");
      } catch (AssertionError actual) {
        require(actual == expected, "open replaced the initialization Error");
      }
      Field calls = SparkConnectBridge.class.getDeclaredField("calls");
      calls.setAccessible(true);
      require(((Set<?>) calls.get(bridge)).isEmpty(), "abnormal open leaked a call registry entry");
      Field context = SparkConnectBridge.Call.class.getDeclaredField("context");
      context.setAccessible(true);
      require(((Context) context.get(captured.get())).isCancelled(),
          "abnormal open retained its context deadline");
    }
  }

  private static void shutdownGuards() throws Exception {
    PrintStream previousOut = System.out;
    PrintStream previousErr = System.err;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream errors = new ByteArrayOutputStream();
    AtomicInteger bridgeStops = new AtomicInteger();
    AtomicInteger sessionStops = new AtomicInteger();
    CountDownLatch failed = new CountDownLatch(1);
    try (PrintStream capturedOut = new PrintStream(output, true, StandardCharsets.UTF_8);
        PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
      System.setOut(capturedOut);
      System.setErr(capturedErr);
      boolean success = EmbeddedSparkConnect.shutdown(() -> {
        throw new IllegalStateException("JNI stop is incomplete");
      }, bridgeStops::incrementAndGet, sessionStops::incrementAndGet, failed, false);
      require(!success && failed.getCount() == 0, "failed shutdown did not release its waiter");
      require(bridgeStops.get() == 0 && sessionStops.get() == 0,
          "native stop failure prematurely stopped Spark resources");
      require(!output.toString(StandardCharsets.UTF_8).contains(EmbeddedSparkConnect.SHUTDOWN_COMPLETE),
          "failed shutdown emitted a success marker");
      require(errors.toString(StandardCharsets.UTF_8).contains("skipping bridge and session cleanup"),
          "failed shutdown did not explain the deliberate cleanup skip");
      CountDownLatch complete = new CountDownLatch(1);
      require(EmbeddedSparkConnect.shutdown(() -> {}, bridgeStops::incrementAndGet,
          sessionStops::incrementAndGet, complete, false), "successful shutdown reported a failure");
      require(complete.getCount() == 0 && bridgeStops.get() == 1 && sessionStops.get() == 1,
          "successful shutdown did not finish all cleanup");
      require(output.toString(StandardCharsets.UTF_8).contains(EmbeddedSparkConnect.SHUTDOWN_COMPLETE),
          "successful shutdown omitted its marker");
      output.reset();
      CountDownLatch startup = new CountDownLatch(1);
      require(EmbeddedSparkConnect.shutdown(() -> {}, bridgeStops::incrementAndGet,
          sessionStops::incrementAndGet, startup, true), "startup cleanup reported a failure");
      require(startup.getCount() == 0, "startup cleanup did not release its waiter");
      require(output.toString(StandardCharsets.UTF_8)
          .contains(EmbeddedSparkConnect.STARTUP_CLEANUP_COMPLETE),
          "startup cleanup omitted its distinct marker");
      require(!output.toString(StandardCharsets.UTF_8).contains(EmbeddedSparkConnect.SHUTDOWN_COMPLETE),
          "startup cleanup emitted a normal shutdown success marker");
    } finally {
      System.setOut(previousOut);
      System.setErr(previousErr);
    }
  }

  private static void realHandlers(SparkConnectBridge bridge) throws Exception {
    String sessionId = UUID.randomUUID().toString();
    UserContext user = UserContext.newBuilder().setUserId("direct-bridge-test").build();
    ConfigRequest config = ConfigRequest.newBuilder().setSessionId(sessionId).setUserContext(user)
        .setOperation(ConfigRequest.Operation.newBuilder().setGet(
            ConfigRequest.Get.newBuilder().addKeys("spark.sql.session.timeZone")))
        .build();
    List<byte[]> configEvents = request(bridge, SERVICE + "Config", config.toByteArray());
    ConfigResponse response = ConfigResponse.parseFrom(message(configEvents.get(1)));
    require(response.getSessionId().equals(sessionId), "Config session ID changed");
    require(response.getPairsCount() == 1, "Config response is missing its value");

    ExecutePlanRequest execute = ExecutePlanRequest.newBuilder()
        .setSessionId(sessionId).setUserContext(user).setOperationId(UUID.randomUUID().toString())
        .setPlan(Plan.newBuilder().setRoot(Relation.newBuilder().setRange(
            Range.newBuilder().setStart(0).setEnd(5).setStep(1).setNumPartitions(1))))
        .build();
    long rows = 0;
    for (byte[] event : request(bridge, SERVICE + "ExecutePlan", execute.toByteArray())) {
      if (event[0] == 2) {
        ExecutePlanResponse result = ExecutePlanResponse.parseFrom(message(event));
        require(result.getSessionId().equals(sessionId), "Execute session ID changed");
        if (result.hasArrowBatch()) {
          rows += result.getArrowBatch().getRowCount();
        }
      }
    }
    require(rows == 5, "ExecutePlan returned " + rows + " rows");
  }

  private static void metadata(SparkConnectBridge bridge) throws Exception {
    byte[][] pairs = {ascii("x-test"), ascii("one"), ascii("x-test"), ascii("two"),
        ascii("grpc-status-details-bin"), new byte[] {0, (byte) 255, 17}};
    register(bridge, "metadata", (call, received) -> {
      require(Arrays.deepEquals(pairs, InternalMetadata.serialize(received)),
          "request metadata changed");
      call.request(1);
      return new ServerCall.Listener<>() {
        @Override
        public void onHalfClose() {
          call.sendHeaders(InternalMetadata.newMetadata(pairs));
          call.close(Status.INVALID_ARGUMENT.withDescription("test status"),
              InternalMetadata.newMetadata(pairs));
        }
      };
    });
    SparkConnectBridge.Call call = bridge.open("bridge.test/metadata", pairs, -1);
    require(call.send(new byte[0]), "metadata request was rejected");
    call.halfClose();
    DataInputStream headers = event(call.poll(1000), 1);
    require(Arrays.deepEquals(pairs, readMetadata(headers)), "response metadata changed");
    DataInputStream close = event(call.poll(1000), 3);
    require(close.readInt() == 3, "status code changed");
    require(new String(close.readNBytes(close.readInt()), StandardCharsets.UTF_8)
        .equals("test status"), "status description changed");
    require(Arrays.deepEquals(pairs, readMetadata(close)), "trailers changed");
    call.finish();
  }

  private static void boundedOutput(SparkConnectBridge bridge) throws Exception {
    CountDownLatch producing = new CountDownLatch(1);
    AtomicInteger completed = new AtomicInteger();
    register(bridge, "bounded", (call, headers) -> {
      call.request(1);
      return new ServerCall.Listener<>() {
        @Override
        public void onHalfClose() {
          call.sendHeaders(new Metadata());
          call.sendMessage(new byte[] {1});
          producing.countDown();
          call.sendMessage(new byte[] {2});
          call.close(Status.OK, new Metadata());
        }

        @Override
        public void onComplete() {
          completed.incrementAndGet();
        }
      };
    });
    SparkConnectBridge.Call call = bridge.open("bridge.test/bounded", null, -1);
    require(call.send(new byte[0]), "bounded request was rejected");
    Future<?> input = WORKERS.submit(call::halfClose);
    require(producing.await(5, TimeUnit.SECONDS), "bounded producer did not start");
    require(!input.isDone() && !call.isReady(), "producer bypassed the two-event bound");
    event(call.poll(1000), 1);
    require(Arrays.equals(message(call.poll(1000)), new byte[] {1}), "first message changed");
    require(Arrays.equals(message(call.poll(1000)), new byte[] {2}), "second message changed");
    require(event(call.poll(1000), 3).readInt() == 0, "bounded call failed");
    call.finish();
    input.get(5, TimeUnit.SECONDS);
    require(completed.get() == 1, "normal completion callback was lost");
  }

  private static void cancelBlockedOutput(SparkConnectBridge bridge) throws Exception {
    CountDownLatch producing = new CountDownLatch(1);
    AtomicInteger cancelled = new AtomicInteger();
    AtomicInteger completed = new AtomicInteger();
    register(bridge, "cancel-output", (call, headers) -> {
      call.request(1);
      return new ServerCall.Listener<>() {
        @Override
        public void onHalfClose() {
          call.sendMessage(new byte[] {1});
          call.sendMessage(new byte[] {2});
          producing.countDown();
          call.sendMessage(new byte[] {3});
        }

        @Override
        public void onCancel() {
          cancelled.incrementAndGet();
        }

        @Override
        public void onComplete() {
          completed.incrementAndGet();
        }
      };
    });
    SparkConnectBridge.Call call = bridge.open("bridge.test/cancel-output", null, -1);
    require(call.send(new byte[0]), "cancel-output request was rejected");
    Future<?> input = WORKERS.submit(call::halfClose);
    require(producing.await(5, TimeUnit.SECONDS), "blocked producer did not start");
    require(!call.isReady(), "cancel-output queue is not full");
    WORKERS.submit(call::cancel).get(1, TimeUnit.SECONDS);
    require(call.isCancelled() && call.isReady(), "cancel did not update readiness");
    require(event(call.poll(1000), 3).readInt() == 1, "cancel status was lost");
    call.finish();
    input.get(5, TimeUnit.SECONDS);
    require(cancelled.get() == 1 && completed.get() == 0,
        "cancel and completion callbacks were not exclusive");
  }

  private static void cancelBlockedInput(SparkConnectBridge bridge) throws Exception {
    register(bridge, "cancel-input", (call, headers) -> {
      call.request(1);
      return new ServerCall.Listener<>() {};
    });
    SparkConnectBridge.Call call = bridge.open("bridge.test/cancel-input", null, -1);
    require(call.send(new byte[0]), "first credited input was rejected");
    CountDownLatch waiting = new CountDownLatch(1);
    Future<Boolean> blocked = WORKERS.submit(() -> {
      waiting.countDown();
      return call.send(new byte[0]);
    });
    require(waiting.await(5, TimeUnit.SECONDS), "input worker did not start");
    call.cancel();
    require(!blocked.get(1, TimeUnit.SECONDS), "uncredited input survived cancellation");
    require(event(call.poll(1000), 3).readInt() == 1, "input cancel status was lost");
    call.finish();
  }

  private static void cancelHeldCallback(SparkConnectBridge bridge) throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger cancelled = new AtomicInteger();
    register(bridge, "cancel-callback", (call, headers) -> {
      call.request(2);
      return new ServerCall.Listener<>() {
        @Override
        public void onHalfClose() {
          entered.countDown();
          try {
            require(release.await(5, TimeUnit.SECONDS), "test callback was never released");
          } catch (InterruptedException error) {
            throw new IllegalStateException(error);
          }
        }

        @Override
        public void onCancel() {
          cancelled.incrementAndGet();
        }
      };
    });
    SparkConnectBridge.Call call = bridge.open("bridge.test/cancel-callback", null, -1);
    require(call.send(new byte[0]), "callback request was rejected");
    Future<?> callback = WORKERS.submit(call::halfClose);
    require(entered.await(5, TimeUnit.SECONDS), "callback did not start");
    Future<Boolean> pending = WORKERS.submit(() -> call.send(new byte[0]));
    WORKERS.submit(call::cancel).get(1, TimeUnit.SECONDS);
    require(!pending.get(1, TimeUnit.SECONDS), "cancel did not release a pending callback");
    require(event(call.poll(1000), 3).readInt() == 1, "callback cancel status was lost");
    call.finish();
    release.countDown();
    callback.get(5, TimeUnit.SECONDS);
    require(cancelled.get() == 1, "deferred cancellation callback was lost");
  }

  private static void deadlinesAndUnknownMethods(SparkConnectBridge bridge) throws Exception {
    register(bridge, "deadline", (call, headers) -> {
      call.request(1);
      return new ServerCall.Listener<>() {};
    });
    SparkConnectBridge.Call expired = bridge.open("bridge.test/deadline", null,
        TimeUnit.MILLISECONDS.toNanos(20));
    require(event(expired.poll(5000), 3).readInt() == 4, "deadline status was lost");
    expired.finish();
    SparkConnectBridge.Call unknown = bridge.open("bridge.test/unknown", null, -1);
    require(event(unknown.poll(1000), 3).readInt() == 12, "unknown method was not UNIMPLEMENTED");
    require(!unknown.send(new byte[0]), "unknown method accepted an input");
    unknown.finish();
  }

  private static List<byte[]> request(SparkConnectBridge bridge, String method, byte[] message)
      throws Exception {
    SparkConnectBridge.Call call = bridge.open(method, null, TimeUnit.SECONDS.toNanos(30));
    try {
      require(call.send(message), method + " rejected input");
      Future<?> input = WORKERS.submit(call::halfClose);
      List<byte[]> events = new ArrayList<>();
      while (true) {
        byte[] event = call.poll(35000);
        require(event != null, method + " did not produce a terminal event");
        events.add(event);
        if (event[0] == 3) {
          DataInputStream status = event(event, 3);
          int code = status.readInt();
          String description = new String(status.readNBytes(status.readInt()), StandardCharsets.UTF_8);
          require(code == 0, method + " failed: " + code + " " + description);
          break;
        }
      }
      input.get(5, TimeUnit.SECONDS);
      return events;
    } finally {
      call.finish();
    }
  }

  @SuppressWarnings("unchecked")
  private static void register(SparkConnectBridge bridge, String method,
      ServerCallHandler<Object, Object> handler) throws Exception {
    MethodDescriptor<Object, Object> descriptor = MethodDescriptor.newBuilder()
        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
        .setFullMethodName("bridge.test/" + method)
        .setRequestMarshaller(BYTES).setResponseMarshaller(BYTES).build();
    Field field = SparkConnectBridge.class.getDeclaredField("methods");
    field.setAccessible(true);
    Map<String, ServerMethodDefinition<Object, Object>> methods =
        (Map<String, ServerMethodDefinition<Object, Object>>) field.get(bridge);
    methods.put(descriptor.getFullMethodName(), ServerMethodDefinition.create(descriptor, handler));
  }

  private static byte[] message(byte[] event) throws Exception {
    event(event, 2);
    return Arrays.copyOfRange(event, 1, event.length);
  }

  private static DataInputStream event(byte[] bytes, int kind) throws Exception {
    require(bytes != null, "missing output event " + kind);
    DataInputStream event = new DataInputStream(new ByteArrayInputStream(bytes));
    require(event.readUnsignedByte() == kind, "unexpected output event kind");
    return event;
  }

  private static byte[][] readMetadata(DataInputStream input) throws Exception {
    byte[][] pairs = new byte[input.readInt() * 2][];
    for (int index = 0; index < pairs.length; index++) {
      pairs[index] = input.readNBytes(input.readInt());
    }
    return pairs;
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  private static void require(boolean condition, String description) {
    if (!condition) {
      throw new AssertionError(description);
    }
  }
}
