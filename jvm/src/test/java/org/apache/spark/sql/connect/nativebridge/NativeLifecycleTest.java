package org.apache.spark.sql.connect.nativebridge;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

import org.apache.spark.connect.proto.AddArtifactsRequest;
import org.apache.spark.connect.proto.AddArtifactsResponse;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.SparkConnectServiceGrpc;
import org.apache.spark.connect.proto.UserContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectAddArtifactsHandler;
import org.apache.spark.sql.connect.service.SparkConnectService$;
import org.sparkproject.connect.grpc.ManagedChannel;
import org.sparkproject.connect.grpc.ManagedChannelBuilder;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerMethodDefinition;
import org.sparkproject.connect.grpc.stub.ClientCallStreamObserver;
import org.sparkproject.connect.grpc.stub.ClientResponseObserver;
import org.sparkproject.connect.grpc.stub.StreamObserver;
import org.sparkproject.connect.protobuf.ByteString;

/** Native transport lifecycle checks, including shutdown with blocked input and output. */
public final class NativeLifecycleTest {
  private static final String EXECUTE = "spark.connect.SparkConnectService/ExecutePlan";

  private NativeLifecycleTest() {}

  public static void main(String[] arguments) throws Exception {
    require(arguments.length == 1, "pass the absolute native library path");
    String library = arguments[0];
    SparkSession session = SparkSession.builder()
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
        .getOrCreate();
    ExecutorService workers = Executors.newCachedThreadPool();
    try (SparkConnectBridge bridge = new SparkConnectBridge(session)) {
      expectFailure(() -> new NativeServer("relative-library", "127.0.0.1:0", bridge),
          IllegalArgumentException.class);
      expectFailure(() -> new NativeServer(library, "invalid-address", bridge),
          IllegalStateException.class);
      NativeServer server = new NativeServer(library, "127.0.0.1:0", bridge);
      try {
        String address = server.address();
        long staleHandle = handle(server);
        require(SparkConnectService$.MODULE$.server() == null, "Java Connect listener exists");
        expectFailure(() -> new NativeServer(library, address, bridge), IllegalStateException.class);
        activeShutdown(bridge, server, address, workers);
        server.close();
        expectFailure(server::address, IllegalStateException.class);
        expectFailure(() -> invokeNative("nativeAddress", staleHandle), IllegalStateException.class);
        expectFailure(() -> invokeNative("nativeStop", staleHandle), IllegalStateException.class);
        try (NativeServer replacement = new NativeServer(library, address, bridge)) {
          require(replacement.address().equals(address), "closed listener could not rebind");
          long liveHandle = handle(replacement);
          setHandle(replacement, staleHandle);
          try {
            expectFailure(replacement::close, IllegalStateException.class);
            require(handle(replacement) == staleHandle, "failed close discarded retryable handle");
          } finally {
            setHandle(replacement, liveHandle);
          }
        }
        require(activeCalls(bridge).isEmpty(), "native stop leaked Java calls");
        require(SparkConnectService$.MODULE$.server() == null, "Java Connect listener appeared");
        System.out.println("NATIVE_LIFECYCLE_OK: pid=" + ProcessHandle.current().pid()
            + "; invalid start; bind conflict; stalled output; incomplete upload cleanup; "
            + "repeated close; stale handles; retryable close; rebind; no Java Connect listener");
      } finally {
        server.close();
      }
    } finally {
      workers.shutdownNow();
      session.stop();
    }
  }

  private static void activeShutdown(SparkConnectBridge bridge, NativeServer server, String address,
      ExecutorService workers) throws Exception {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    Map<String, ServerMethodDefinition<Object, Object>> methods = methods(bridge);
    ServerMethodDefinition<Object, Object> original = methods.get(EXECUTE);
    CountDownLatch firstResponse = new CountDownLatch(1);
    CountDownLatch uploadClosed = new CountDownLatch(1);
    AtomicInteger produced = new AtomicInteger();
    AtomicInteger cancelled = new AtomicInteger();
    ByteString payload = ByteString.copyFrom(new byte[64 * 1024]);
    methods.put(EXECUTE, ServerMethodDefinition.create(original.getMethodDescriptor(),
        (call, headers) -> {
          call.request(1);
          return new ServerCall.Listener<>() {
            @Override
            public void onHalfClose() {
              call.sendHeaders(new Metadata());
              for (int index = 0; index < 100_000; index++) {
                call.sendMessage(ExecutePlanResponse.newBuilder()
                    .setSessionId("native-stalled-output")
                    .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder().setData(payload)
                        .setRowCount(1)).build());
                produced.incrementAndGet();
              }
              throw new AssertionError("native output never applied backpressure");
            }

            @Override
            public void onCancel() {
              cancelled.incrementAndGet();
            }
          };
        }));
    try {
      SparkConnectServiceGrpc.SparkConnectServiceStub stub = SparkConnectServiceGrpc.newStub(channel);
      StreamObserver<AddArtifactsRequest> upload = stub.addArtifacts(new StreamObserver<>() {
        @Override
        public void onNext(AddArtifactsResponse response) {
          throw new AssertionError("incomplete upload completed");
        }

        @Override
        public void onError(Throwable error) {
          uploadClosed.countDown();
        }

        @Override
        public void onCompleted() {
          throw new AssertionError("incomplete upload completed");
        }
      });
      byte[] chunk = {1, 2, 3};
      CRC32 crc = new CRC32();
      crc.update(chunk);
      String name = "cache/incomplete-native-lifecycle";
      upload.onNext(AddArtifactsRequest.newBuilder().setSessionId(UUID.randomUUID().toString())
          .setUserContext(UserContext.newBuilder().setUserId("native-lifecycle"))
          .setBeginChunk(AddArtifactsRequest.BeginChunkedArtifact.newBuilder().setName(name)
              .setNumChunks(2).setTotalBytes(6)
              .setInitialChunk(AddArtifactsRequest.ArtifactChunk.newBuilder()
                  .setData(ByteString.copyFrom(chunk)).setCrc(crc.getValue())))
          .build());
      Path staging = awaitUploadStaging(bridge, name);
      stub.executePlan(ExecutePlanRequest.getDefaultInstance(),
          new ClientResponseObserver<ExecutePlanRequest, ExecutePlanResponse>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<ExecutePlanRequest> stream) {
              stream.disableAutoRequestWithInitial(1);
            }

            @Override
            public void onNext(ExecutePlanResponse response) {
              firstResponse.countDown();
            }

            @Override
            public void onError(Throwable error) {}

            @Override
            public void onCompleted() {}
          });
      require(firstResponse.await(10, TimeUnit.SECONDS), "native output never started");
      await(() -> hasFullOutputQueue(bridge), "native output did not fill the bounded Java queue");
      require(produced.get() < 100_000, "stalled receiver did not constrain output");
      Future<?> stop = workers.submit(server::close);
      stop.get(10, TimeUnit.SECONDS);
      require(cancelled.get() == 1, "stalled output did not receive cancellation");
      require(uploadClosed.await(5, TimeUnit.SECONDS), "incomplete upload was not closed");
      require(Files.notExists(staging), "cancelled upload leaked its staging directory");
      require(activeCalls(bridge).isEmpty(), "stop returned before Java calls finished");
    } finally {
      methods.put(EXECUTE, original);
      channel.shutdownNow();
      require(channel.awaitTermination(10, TimeUnit.SECONDS), "native test channel did not stop");
    }
  }

  private static Path awaitUploadStaging(SparkConnectBridge bridge, String name) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      for (SparkConnectBridge.Call call : activeCalls(bridge)) {
        Object listener = field(call, "listener");
        if (listener != null && listener.getClass().getSimpleName().equals("StreamingServerCallListener")) {
          Object observer = field(listener, "requestObserver");
          if (observer instanceof SparkConnectAddArtifactsHandler) {
            Path staging = ((SparkConnectAddArtifactsHandler) observer).stagingDir();
            if (Files.exists(staging.resolve(name)) && Files.size(staging.resolve(name)) == 3) {
              return staging;
            }
          }
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("incomplete upload did not reach Spark's artifact handler");
  }

  private static boolean hasFullOutputQueue(SparkConnectBridge bridge) {
    try {
      for (SparkConnectBridge.Call call : activeCalls(bridge)) {
        if (call.getMethodDescriptor().getFullMethodName().equals(EXECUTE) && !call.isReady()) {
          return true;
        }
      }
      return false;
    } catch (ReflectiveOperationException error) {
      throw new IllegalStateException(error);
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<SparkConnectBridge.Call> activeCalls(SparkConnectBridge bridge)
      throws ReflectiveOperationException {
    return (Set<SparkConnectBridge.Call>) field(bridge, "calls");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, ServerMethodDefinition<Object, Object>> methods(SparkConnectBridge bridge)
      throws ReflectiveOperationException {
    return (Map<String, ServerMethodDefinition<Object, Object>>) field(bridge, "methods");
  }

  private static Object field(Object owner, String name) throws ReflectiveOperationException {
    Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }

  private static long handle(NativeServer server) throws ReflectiveOperationException {
    return (long) field(server, "handle");
  }

  private static void setHandle(NativeServer server, long handle) throws ReflectiveOperationException {
    Field field = NativeServer.class.getDeclaredField("handle");
    field.setAccessible(true);
    field.setLong(server, handle);
  }

  private static void invokeNative(String name, long handle) throws Exception {
    Method method = NativeServer.class.getDeclaredMethod(name, long.class);
    method.setAccessible(true);
    try {
      method.invoke(null, handle);
    } catch (InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw (Error) cause;
    }
  }

  private static void await(BooleanSupplier condition, String description) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError(description);
  }

  private static void expectFailure(ThrowingRunnable action, Class<? extends Exception> type)
      throws Exception {
    try {
      action.run();
    } catch (Exception error) {
      require(type.isInstance(error), "expected " + type.getSimpleName() + ", got " + error);
      return;
    }
    throw new AssertionError("expected " + type.getSimpleName());
  }

  private static void require(boolean condition, String description) {
    if (!condition) {
      throw new AssertionError(description);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
