package org.apache.spark.sql.connect.nativebridge.benchmark;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;

/** Counts jobs independently of the benchmark client to audit planning-only runs. */
public final class NoExecutionListener extends SparkListener {
  private static final AtomicLong JOBS = new AtomicLong();
  private static final AtomicBoolean HOOK = new AtomicBoolean();

  public NoExecutionListener() {
    if (HOOK.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(new Thread(() ->
          System.out.println("PROTOCOL_BENCH_SPARK_JOBS {\"jobs_started\":" + JOBS.get() + "}"),
          "protocol-benchmark-job-audit"));
    }
  }

  public NoExecutionListener(SparkConf ignored) { this(); }

  @Override
  public void onJobStart(SparkListenerJobStart event) { JOBS.incrementAndGet(); }
}
