package cn.heycloudream.streamtask.recovery;

import cn.heycloudream.streamtask.consumer.StreamTaskExecutor;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PendingRecoveryScheduler implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(PendingRecoveryScheduler.class);

    private final StreamTaskProperties properties;
    private final PendingMessageClaimer claimer;
    private final StreamTaskExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    public PendingRecoveryScheduler(StreamTaskProperties properties, PendingMessageClaimer claimer, StreamTaskExecutor executor) {
        this.properties = properties;
        this.claimer = claimer;
        this.executor = executor;
    }

    @Override
    public void start() {
        if (!properties.getRecovery().isEnabled() || !running.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "stream-task-pending-recovery");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = properties.getRecovery().getScanInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::scanOnce, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("[StreamTask] pending recovery started consumer={}", recoveryConsumer());
    }

    @Override
    public void stop() {
        running.set(false);
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public void scanOnce() {
        try {
            String cursor = "0-0";
            do {
                AutoClaimResult result = claimer.autoClaim(recoveryConsumer(), cursor);
                cursor = result.nextStartId();
                for (ClaimedStreamRecord record : result.records()) {
                    executor.execute(record.messageId(), record.body(), recoveryConsumer());
                }
            } while (!"0-0".equals(cursor) && running.get());
        } catch (Exception e) {
            log.warn("[StreamTask] pending recovery failed", e);
        }
    }

    private String recoveryConsumer() {
        return properties.getConsumer().getName() + "-recovery";
    }
}
