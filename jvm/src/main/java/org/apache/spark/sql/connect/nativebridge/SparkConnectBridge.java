package org.apache.spark.sql.connect.nativebridge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService;
import org.apache.spark.sql.connect.service.SparkConnectService$;
import org.sparkproject.connect.grpc.Context;
import org.sparkproject.connect.grpc.InternalMetadata;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.MethodDescriptor;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerMethodDefinition;
import org.sparkproject.connect.grpc.Status;

/** Invokes Spark's generated handlers directly without starting a Java gRPC server. */
public final class SparkConnectBridge implements AutoCloseable {
  private static final int OUTPUT_CAPACITY = 2;

  private final SparkSession session;
  private final SparkContext sparkContext;
  private final ClassLoader classLoader;
  private final Map<String, ServerMethodDefinition<Object, Object>> methods = new HashMap<>();
  private final Set<Call> calls = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final ScheduledExecutorService deadlines = Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "SparkConnectNativeDeadline");
        thread.setDaemon(true);
        return thread;
      });

  /** Attach to a classic Spark session. The launcher retains ownership of that session. */
  public SparkConnectBridge(SparkSession session) {
    this.session = Objects.requireNonNull(session, "session");
    sparkContext = session.sparkContext();
    ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
    classLoader = currentLoader == null ? getClass().getClassLoader() : currentLoader;
    SparkConnectService$ singleton = SparkConnectService$.MODULE$;
    synchronized (singleton) {
      if (singleton.started() || singleton.stopped() || singleton.server() != null) {
        deadlines.shutdownNow();
        throw new IllegalStateException("Spark Connect singleton is already in use");
      }
      try {
        // start(sc) also binds Netty. Initialize only Spark's listener and progress state.
        // Its service-started/stopped events require a Netty binding and are intentionally omitted.
        Method initialize = singleton.getClass().getDeclaredMethod(
            "createListenerAndUI", SparkContext.class);
        initialize.setAccessible(true);
        initialize.invoke(singleton, sparkContext);
      } catch (ReflectiveOperationException error) {
        deadlines.shutdownNow();
        Throwable cause = error instanceof InvocationTargetException
            ? ((InvocationTargetException) error).getCause() : error;
        throw new IllegalStateException(
            "Spark assembly does not support direct handler initialization", cause);
      }
      singleton.started_$eq(true);
      singleton.stopped_$eq(false);
      try {
        boolean debugMode = SparkEnv.get().conf().getBoolean("spark.connect.grpc.debug.enabled", false);
        for (ServerMethodDefinition<?, ?> method :
            new SparkConnectService(debugMode).bindService().getMethods()) {
          addMethod(method);
        }
      } catch (RuntimeException error) {
        deadlines.shutdownNow();
        SparkConnectService.stop(scala.Option.empty(), scala.Option.empty());
        throw error;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addMethod(ServerMethodDefinition<?, ?> method) {
    methods.put(method.getMethodDescriptor().getFullMethodName(),
        (ServerMethodDefinition<Object, Object>) method);
  }

  /**
   * Open a call using alternating raw metadata keys and values. A negative timeout means no
   * deadline; zero means the call has already expired.
   */
  public Call open(String fullMethod, byte[][] rawMetadata, long timeoutNanos) {
    Objects.requireNonNull(fullMethod, "fullMethod");
    if (closed.get()) {
      throw new IllegalStateException("Spark Connect bridge is closed");
    }
    String name = fullMethod.startsWith("/") ? fullMethod.substring(1) : fullMethod;
    ServerMethodDefinition<Object, Object> definition = methods.get(name);
    Context.CancellableContext context = timeoutNanos < 0
        ? Context.ROOT.withCancellation()
        : Context.ROOT.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS, deadlines);
    Call call = new Call(definition, context);
    try {
      calls.add(call);
      context.addListener(ignored -> call.contextCancelled(), Runnable::run);
      if (closed.get()) {
        call.cancel();
        return call;
      }
      if (definition == null) {
        call.close(Status.UNIMPLEMENTED.withDescription("Unknown Spark Connect method: " + name),
            new Metadata());
        return call;
      }
      byte[][] pairs = rawMetadata == null ? new byte[0][] : rawMetadata;
      if (pairs.length % 2 != 0) {
        call.close(Status.INVALID_ARGUMENT.withDescription(
            "metadata must contain alternating key/value pairs"), new Metadata());
        return call;
      }
      call.callbackLock.lock();
      try {
        if (!call.terminal.get()) {
          call.invoke(() -> call.listener = definition.getServerCallHandler().startCall(
              call, InternalMetadata.newMetadata(pairs)));
        }
      } catch (RuntimeException error) {
        call.fail(error);
      } finally {
        call.releaseCallbacks();
      }
      return call;
    } catch (Throwable error) {
      // JNI never receives this Call on an abnormal return, so it cannot finish it for us.
      try {
        call.finish();
      } catch (Throwable cleanup) {
        if (cleanup != error) {
          error.addSuppressed(cleanup);
        }
      } finally {
        calls.remove(call);
        try {
          context.cancel(error);
        } catch (Throwable cleanup) {
          if (cleanup != error) {
            error.addSuppressed(cleanup);
          }
        }
      }
      throw error;
    }
  }

  /** Cancel attachments and stop Connect managers; the launcher stops the base Spark session. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      for (Call call : calls) {
        call.cancel();
      }
      deadlines.shutdownNow();
      SparkConnectService.stop(scala.Option.empty(), scala.Option.empty());
    }
  }

  /**
   * A bounded pull interface used by separate native input and output threads. Each output event
   * begins with a byte: 1 for headers, 2 for a protobuf message, or 3 for status and trailers.
   * Integers and metadata lengths use DataOutputStream's big-endian encoding.
   */
  public final class Call extends ServerCall<Object, Object> {
    private final ServerMethodDefinition<Object, Object> definition;
    private final Context.CancellableContext context;
    private final ReentrantLock callbackLock = new ReentrantLock();
    private final ReentrantLock inputLock = new ReentrantLock();
    private final Condition inputAvailable = inputLock.newCondition();
    private final ReentrantLock outputLock = new ReentrantLock();
    private final Condition outputAvailable = outputLock.newCondition();
    private final Condition outputWritable = outputLock.newCondition();
    private final ArrayDeque<byte[]> output = new ArrayDeque<>(OUTPUT_CAPACITY);
    private final AtomicBoolean terminal = new AtomicBoolean();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicBoolean readyPending = new AtomicBoolean();
    private final AtomicBoolean cancelPending = new AtomicBoolean();
    private final AtomicBoolean completePending = new AtomicBoolean();
    private volatile ServerCall.Listener<Object> listener;
    private volatile boolean closeConsumed;
    private boolean halfClosed;
    private long requested;

    private Call(ServerMethodDefinition<Object, Object> definition,
        Context.CancellableContext context) {
      this.definition = definition;
      this.context = context;
    }

    /** Wait for handler credit, then parse and deliver exactly one protobuf request. */
    public boolean send(byte[] message) {
      Objects.requireNonNull(message, "message");
      inputLock.lock();
      try {
        while (requested == 0 && !terminal.get() && !finished.get()) {
          inputAvailable.await();
        }
        if (terminal.get() || finished.get()) {
          return false;
        }
        requested--;
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        cancel();
        return false;
      } finally {
        inputLock.unlock();
      }
      if (!lockInputCallback()) {
        return false;
      }
      try {
        if (terminal.get() || halfClosed) {
          return false;
        }
        Object request = definition.getMethodDescriptor().parseRequest(
            new ByteArrayInputStream(message));
        invoke(() -> listener.onMessage(request));
        return !cancelled.get();
      } catch (RuntimeException error) {
        fail(error);
        return false;
      } finally {
        releaseCallbacks();
      }
    }

    /** End the request stream. Non-reattachable execution can block in this callback. */
    public void halfClose() {
      if (!lockInputCallback()) {
        return;
      }
      try {
        if (!terminal.get() && !halfClosed) {
          halfClosed = true;
          invoke(() -> listener.onHalfClose());
        }
      } catch (RuntimeException error) {
        fail(error);
      } finally {
        releaseCallbacks();
      }
    }

    private boolean lockInputCallback() {
      try {
        while (!terminal.get() && !finished.get()) {
          if (callbackLock.tryLock(25, TimeUnit.MILLISECONDS)) {
            return true;
          }
        }
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        cancel();
      }
      return false;
    }

    /** Return the next event, or null on timeout or after the close event was consumed. */
    public byte[] poll(long timeoutMillis) {
      byte[] event = null;
      boolean interrupted = false;
      outputLock.lock();
      try {
        long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (output.isEmpty() && !closeConsumed && !finished.get()) {
          if (timeoutMillis < 0) {
            outputAvailable.await();
          } else if (remaining > 0) {
            remaining = outputAvailable.awaitNanos(remaining);
          } else {
            break;
          }
        }
        event = output.poll();
        if (event != null) {
          closeConsumed = event[0] == 3;
          outputWritable.signalAll();
        }
      } catch (InterruptedException error) {
        interrupted = true;
      } finally {
        outputLock.unlock();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
        cancel();
        return poll(0);
      }
      if (event != null && !terminal.get()) {
        readyPending.set(true);
        signalCallbacks();
      }
      return event;
    }

    /** Cancel without waiting for a handler callback or a full response queue. */
    public void cancel() {
      context.cancel(Status.CANCELLED.withDescription("native transport cancelled").asException());
      contextCancelled();
    }

    private void contextCancelled() {
      Status status = context.getDeadline() != null && context.getDeadline().isExpired()
          ? Status.DEADLINE_EXCEEDED.withDescription("native transport deadline exceeded")
          : Status.CANCELLED.withDescription("native transport cancelled");
      outputLock.lock();
      try {
        if (finished.get() || closeConsumed || !cancelled.compareAndSet(false, true)) {
          return;
        }
        terminal.set(true);
        output.clear();
        output.add(closeEvent(status, new Metadata()));
        outputAvailable.signalAll();
        outputWritable.signalAll();
      } finally {
        outputLock.unlock();
      }
      signalInput();
      cancelPending.set(true);
      readyPending.set(true);
      signalCallbacks();
    }

    /** Finish after consuming close. No native reference is retained by Spark callbacks. */
    public void finish() {
      if (!closeConsumed) {
        cancel();
      }
      if (finished.compareAndSet(false, true)) {
        calls.remove(this);
        context.cancel(null);
        outputLock.lock();
        try {
          output.clear();
          outputAvailable.signalAll();
          outputWritable.signalAll();
        } finally {
          outputLock.unlock();
        }
        signalInput();
        if (!cancelled.get()) {
          completePending.set(true);
        }
        signalCallbacks();
      }
    }

    private void signalInput() {
      inputLock.lock();
      try {
        inputAvailable.signalAll();
      } finally {
        inputLock.unlock();
      }
    }

    private boolean callbacksPending() {
      return cancelPending.get() || readyPending.get() || completePending.get();
    }

    private void releaseCallbacks() {
      try {
        drainCallbacks();
      } finally {
        callbackLock.unlock();
        signalCallbacks();
      }
    }

    private void signalCallbacks() {
      // A synchronous handler may be waiting for poll() to make room. Never wait for its lock.
      if (callbackLock.isHeldByCurrentThread()) {
        return;
      }
      while (listener != null && callbacksPending() && callbackLock.tryLock()) {
        try {
          drainCallbacks();
        } finally {
          callbackLock.unlock();
        }
        // Recheck after unlocking: a poll/cancel can defer work after drainCallbacks returns.
      }
    }

    private void drainCallbacks() {
      if (listener == null) {
        return;
      }
      try {
        if (cancelPending.getAndSet(false)) {
          invoke(() -> listener.onCancel());
        }
        if (readyPending.getAndSet(false) && (!terminal.get() || cancelled.get())) {
          // Spark's sender waits on readiness even after cancellation. Wake it so the generated
          // response observer rejects its next message instead of waiting for queue capacity.
          invoke(() -> listener.onReady());
        }
        if (completePending.getAndSet(false)) {
          invoke(() -> listener.onComplete());
        }
      } catch (RuntimeException error) {
        fail(error);
      }
    }

    private void fail(Throwable error) {
      Status status = Status.fromThrowable(error);
      if (status.getCode() == Status.Code.UNKNOWN && status.getDescription() == null) {
        status = status.withDescription(error.getMessage());
      }
      Metadata trailers = Status.trailersFromThrowable(error);
      close(status, trailers == null ? new Metadata() : trailers);
    }

    private void invoke(Runnable action) {
      Context previousContext = context.attach();
      Thread thread = Thread.currentThread();
      ClassLoader previousLoader = thread.getContextClassLoader();
      Properties previousProperties = sparkContext.getLocalProperties();
      scala.Option<SparkSession> previousSession = SparkSession.getActiveSession();
      thread.setContextClassLoader(classLoader);
      sparkContext.setLocalProperties(new Properties());
      SparkSession.setActiveSession(session);
      try {
        action.run();
      } finally {
        if (previousSession.isDefined()) {
          SparkSession.setActiveSession(previousSession.get());
        } else {
          SparkSession.clearActiveSession();
        }
        sparkContext.setLocalProperties(previousProperties);
        thread.setContextClassLoader(previousLoader);
        context.detach(previousContext);
      }
    }

    @Override
    public void request(int messages) {
      if (messages <= 0) {
        throw new IllegalArgumentException("message credits must be positive");
      }
      inputLock.lock();
      try {
        requested = Long.MAX_VALUE - requested < messages ? Long.MAX_VALUE : requested + messages;
        inputAvailable.signalAll();
      } finally {
        inputLock.unlock();
      }
    }

    private boolean enqueue(byte[] event, boolean closing) {
      boolean interrupted = false;
      outputLock.lock();
      try {
        while (output.size() == OUTPUT_CAPACITY && !cancelled.get() && !finished.get()
            && (closing || !terminal.get())) {
          outputWritable.await();
        }
        if (!cancelled.get() && !finished.get() && (closing || !terminal.get())) {
          output.add(event);
          outputAvailable.signalAll();
          return true;
        }
      } catch (InterruptedException error) {
        interrupted = true;
      } finally {
        outputLock.unlock();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
        cancel();
      }
      return false;
    }

    @Override
    public void sendHeaders(Metadata metadata) {
      enqueue(metadataEvent((byte) 1, metadata), false);
    }

    @Override
    public void sendMessage(Object response) {
      if (terminal.get() || cancelled.get()) {
        throw Status.CANCELLED.asRuntimeException();
      }
      try (InputStream bytes = definition.getMethodDescriptor().streamResponse(response)) {
        ByteArrayOutputStream event = new ByteArrayOutputStream();
        event.write(2);
        bytes.transferTo(event);
        if (!enqueue(event.toByteArray(), false)) {
          throw Status.CANCELLED.asRuntimeException();
        }
      } catch (IOException error) {
        throw Status.INTERNAL.withDescription("cannot serialize Spark response")
            .withCause(error).asRuntimeException();
      }
    }

    @Override
    public boolean isReady() {
      if (cancelled.get()) {
        return true;
      }
      outputLock.lock();
      try {
        return !terminal.get() && output.size() < OUTPUT_CAPACITY;
      } finally {
        outputLock.unlock();
      }
    }

    @Override
    public void close(Status status, Metadata trailers) {
      if (terminal.compareAndSet(false, true)) {
        signalInput();
        enqueue(closeEvent(status, trailers), true);
      }
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      return definition == null ? null : definition.getMethodDescriptor();
    }
  }

  private static byte[] metadataEvent(byte kind, Metadata metadata) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeByte(kind);
      writeMetadata(output, metadata);
      return bytes.toByteArray();
    } catch (IOException error) {
      throw new IllegalStateException("cannot encode metadata", error);
    }
  }

  private static byte[] closeEvent(Status status, Metadata trailers) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeByte(3);
      output.writeInt(status.getCode().value());
      byte[] description = status.getDescription() == null ? new byte[0]
          : status.getDescription().getBytes(StandardCharsets.UTF_8);
      output.writeInt(description.length);
      output.write(description);
      writeMetadata(output, trailers);
      return bytes.toByteArray();
    } catch (IOException error) {
      throw new IllegalStateException("cannot encode status", error);
    }
  }

  private static void writeMetadata(DataOutputStream output, Metadata metadata) throws IOException {
    byte[][] pairs = InternalMetadata.serialize(metadata);
    output.writeInt(pairs == null ? 0 : pairs.length / 2);
    if (pairs != null) {
      for (byte[] value : pairs) {
        output.writeInt(value.length);
        output.write(value);
      }
    }
  }
}
