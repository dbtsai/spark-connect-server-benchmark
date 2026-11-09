package org.apache.spark.sql.connect.nativebridge;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;

/** Owns the Rust gRPC listener loaded into the current Spark JVM. */
public final class NativeServer implements AutoCloseable {
  private long handle;

  public NativeServer(String absoluteLibraryPath, String listenAddress, SparkConnectBridge bridge) {
    Path library = Path.of(Objects.requireNonNull(absoluteLibraryPath, "absoluteLibraryPath"));
    if (!library.isAbsolute()) {
      throw new IllegalArgumentException("native library path must be absolute");
    }
    Objects.requireNonNull(listenAddress, "listenAddress");
    Objects.requireNonNull(bridge, "bridge");
    System.load(library.toString());
    handle = nativeStart(listenAddress.getBytes(StandardCharsets.UTF_8), bridge);
    if (handle == 0) {
      throw new IllegalStateException("native listener returned an invalid handle");
    }
  }

  public synchronized String address() {
    if (handle == 0) {
      throw new IllegalStateException("native listener is closed");
    }
    return new String(nativeAddress(handle), StandardCharsets.UTF_8);
  }

  /** A failed stop retains the handle so the owner can retry cleanup safely. */
  @Override
  public synchronized void close() {
    if (handle != 0) {
      nativeStop(handle);
      handle = 0;
    }
  }

  private static native long nativeStart(byte[] utf8Address, SparkConnectBridge bridge);

  private static native byte[] nativeAddress(long handle);

  private static native void nativeStop(long handle);
}
