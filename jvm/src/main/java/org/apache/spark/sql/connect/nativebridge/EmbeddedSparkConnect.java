package org.apache.spark.sql.connect.nativebridge;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService$;
import org.apache.spark.util.ShutdownHookManager;

/** Spark-submit entry point for an embedded Rust transport and Spark's existing handlers. */
public final class EmbeddedSparkConnect {
  static final String SHUTDOWN_COMPLETE = "Embedded Spark Connect shutdown complete";
  static final String STARTUP_CLEANUP_COMPLETE =
      "Embedded Spark Connect startup failed; cleanup complete";

  private EmbeddedSparkConnect() {}

  public static void main(String[] arguments) throws InterruptedException {
    if (arguments.length < 1 || arguments.length > 2) {
      throw new IllegalArgumentException(
          "usage: EmbeddedSparkConnect <absolute-native-library> [listen-address]");
    }
    String listenAddress = arguments.length == 2 ? arguments[1] : "127.0.0.1:15002";
    SparkSession session = SparkSession.builder().appName("Embedded Spark Connect")
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
        .getOrCreate();
    SparkConnectBridge bridge = null;
    NativeServer server = null;
    CountDownLatch stopped = new CountDownLatch(1);
    AtomicBoolean launched = new AtomicBoolean();
    Object shutdownHook = null;
    try {
      bridge = new SparkConnectBridge(session);
      server = new NativeServer(arguments[0], listenAddress, bridge);
      SparkConnectBridge shutdownBridge = bridge;
      NativeServer shutdownServer = server;
      shutdownHook = ShutdownHookManager.addShutdownHook(
          ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY() + 1, () -> {
        shutdown(shutdownServer::close, shutdownBridge::close, session::stop, stopped,
            !launched.get());
        return scala.runtime.BoxedUnit.UNIT;
      });
      if (SparkConnectService$.MODULE$.server() != null) {
        throw new IllegalStateException("a Java Spark Connect listener was started");
      }
      System.out.println("Embedded Spark Connect listening on " + server.address());
      System.out.println("Embedded Spark Connect JVM PID " + ProcessHandle.current().pid()
          + "; Java Connect listener absent");
      launched.set(true);
      stopped.await();
    } catch (Throwable error) {
      if (shutdownHook != null) {
        try {
          ShutdownHookManager.removeShutdownHook(shutdownHook);
        } catch (Throwable cleanup) {
          if (cleanup != error) {
            error.addSuppressed(cleanup);
          }
        }
      }
      NativeServer failedServer = server;
      SparkConnectBridge failedBridge = bridge;
      shutdown(() -> {
        if (failedServer != null) {
          failedServer.close();
        }
      }, () -> {
        if (failedBridge != null) {
          failedBridge.close();
        }
      }, session::stop, stopped, !launched.get());
      throw error;
    }
  }

  static boolean shutdown(Runnable stopNative, Runnable stopBridge, Runnable stopSession,
      CountDownLatch stopped, boolean startupFailure) {
    try {
      try {
        stopNative.run();
      } catch (Throwable failure) {
        System.err.println("Embedded Spark Connect shutdown failed: native stop incomplete; "
            + "skipping bridge and session cleanup while JNI calls may still be active");
        failure.printStackTrace(System.err);
        return false;
      }
      Throwable failure = null;
      try {
        stopBridge.run();
      } catch (Throwable error) {
        failure = error;
      }
      try {
        stopSession.run();
      } catch (Throwable error) {
        if (failure == null) {
          failure = error;
        } else if (error != failure) {
          failure.addSuppressed(error);
        }
      }
      if (failure != null) {
        System.err.println("Embedded Spark Connect shutdown failed: Spark cleanup did not finish");
        failure.printStackTrace(System.err);
        return false;
      }
      System.out.println(startupFailure ? STARTUP_CLEANUP_COMPLETE : SHUTDOWN_COMPLETE);
      return true;
    } finally {
      stopped.countDown();
    }
  }
}
