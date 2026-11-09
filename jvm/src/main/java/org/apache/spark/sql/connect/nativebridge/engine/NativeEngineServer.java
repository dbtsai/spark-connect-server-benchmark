package org.apache.spark.sql.connect.nativebridge.engine;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;

/** Loads the Rust protocol frontend with the typed JVM Engine implementation. */
public final class NativeEngineServer implements AutoCloseable {
  private long handle;

  public NativeEngineServer(String absoluteLibraryPath, String listenAddress, SparkJvmEngine engine) {
    Path library = Path.of(Objects.requireNonNull(absoluteLibraryPath, "absoluteLibraryPath"));
    if (!library.isAbsolute()) throw new IllegalArgumentException("native library path must be absolute");
    Objects.requireNonNull(listenAddress, "listenAddress");
    Objects.requireNonNull(engine, "engine");
    System.load(library.toString());
    handle = nativeStart(listenAddress.getBytes(StandardCharsets.UTF_8), engine);
    if (handle == 0) throw new IllegalStateException("native engine returned an invalid handle");
  }

  public synchronized String address() {
    if (handle == 0) throw new IllegalStateException("native listener is closed");
    return new String(nativeAddress(handle), StandardCharsets.UTF_8);
  }

  /** Retains the handle after failed teardown so the owner can retry. */
  @Override
  public synchronized void close() {
    if (handle != 0) {
      nativeStop(handle);
      handle = 0;
    }
  }

  private static native long nativeStart(byte[] utf8Address, SparkJvmEngine engine);
  private static native byte[] nativeAddress(long handle);
  private static native void nativeStop(long handle);
}
