package org.nmq.benchmarks;

import static java.lang.System.*;
import static java.util.concurrent.TimeUnit.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMeter {

    private static final long NANOS_PER_S = TimeUnit.SECONDS.toNanos(1);

    private AtomicLong totalOperations;
    private long startTime;
    private long lastOperations;
    private long lastTime;

    private final ScheduledExecutorService scheduledExecutorService;
    private final String unit;

    private Runnable meterPrinter = new Runnable() {

        @Override
        public void run() {
            long now = System.nanoTime();
            long deltaOps = totalOperations.get() - lastOperations;
            long deltaTime = now - lastTime;
            lastOperations += deltaOps;
            lastTime = now;

            long seconds = NANOSECONDS.toSeconds(now - startTime);
            long operations = (deltaTime == 0) ? 0 : (NANOS_PER_S * deltaOps) / deltaTime;

            out.printf("%,4ds: %,12d %s/s.", seconds, operations, unit);
            out.printf("    (total: %,12d)\n", totalOperations.get());
            out.flush();
        }

    };

    public PerformanceMeter(String unit) {
        this.unit = unit;
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        totalOperations = new AtomicLong();
        lastOperations = 0;
        startTime = System.nanoTime();
        lastTime = startTime;
        scheduledExecutorService.scheduleAtFixedRate(meterPrinter, 1, 1, TimeUnit.SECONDS);
    }

    public void inc() {
        totalOperations.incrementAndGet();
    }

}
