package cn.heycloudream.streamtask.idempotent;

import cn.heycloudream.streamtask.support.StreamTaskProperties;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LeaseWatchdog {
    private final IdempotentGuard guard;
    private final StreamTaskProperties properties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "stream-task-lease-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public LeaseWatchdog(IdempotentGuard guard, StreamTaskProperties properties) {
        this.guard = guard;
        this.properties = properties;
    }

    public ScheduledFuture<?> watch(Lease lease) {
        if (lease == null || !lease.acquired()) {
            return null;
        }
        long periodMillis = properties.getIdempotent().getRenewInterval().toMillis();
        return scheduler.scheduleAtFixedRate(
                () -> guard.renew(lease),
                periodMillis,
                periodMillis,
                TimeUnit.MILLISECONDS
        );
    }
}
