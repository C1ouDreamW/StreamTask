package cn.heycloudream.streamtask.idempotent;

import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LeaseWatchdog implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaseWatchdog.class);

    private final IdempotentGuard guard;
    private final StreamTaskProperties properties;
    private final StreamTaskMetrics metrics;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "stream-task-lease-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public LeaseWatchdog(IdempotentGuard guard, StreamTaskProperties properties, StreamTaskMetrics metrics) {
        this.guard = guard;
        this.properties = properties;
        this.metrics = metrics;
    }

    public ScheduledFuture<?> watch(Lease lease) {
        if (lease == null || !lease.acquired()) {
            return null;
        }
        long periodMillis = properties.getIdempotent().getRenewInterval().toMillis();
        AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                () -> {
                    boolean renewed = guard.renew(lease);
                    if (!renewed) {
                        log.warn("[StreamTask] lease renew failed key={}", lease.key());
                        metrics.leaseLost("unknown");
                        ScheduledFuture<?> self = futureRef.get();
                        if (self != null) {
                            self.cancel(false);
                        }
                    }
                },
                periodMillis,
                periodMillis,
                TimeUnit.MILLISECONDS
        );
        futureRef.set(future);
        return future;
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }
}
