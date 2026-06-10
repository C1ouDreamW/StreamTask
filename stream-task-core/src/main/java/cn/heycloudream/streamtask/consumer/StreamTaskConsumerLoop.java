package cn.heycloudream.streamtask.consumer;

import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamTaskConsumerLoop implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(StreamTaskConsumerLoop.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final StreamTaskExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService worker;

    public StreamTaskConsumerLoop(StringRedisTemplate redisTemplate, StreamTaskProperties properties, StreamTaskExecutor executor) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.executor = executor;
    }

    @Override
    public void start() {
        if (!properties.getConsumer().isEnabled() || !running.compareAndSet(false, true)) {
            return;
        }
        worker = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "stream-task-consumer-loop");
            thread.setDaemon(true);
            return thread;
        });
        worker.submit(this::runLoop);
    }

    @Override
    public void stop() {
        running.set(false);
        if (worker != null) {
            worker.shutdown();
            try {
                boolean terminated = worker.awaitTermination(
                        properties.getConsumer().getShutdownTimeout().toMillis(),
                        TimeUnit.MILLISECONDS
                );
                if (!terminated) {
                    log.warn("[StreamTask] consumer loop did not stop within shutdownTimeout={}ms, forcing shutdown",
                            properties.getConsumer().getShutdownTimeout().toMillis());
                    worker.shutdownNow();
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                worker.shutdownNow();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void runLoop() {
        log.info("[StreamTask] consumer loop started consumer={}", properties.getConsumer().getName());
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                        Consumer.from(properties.getGroup(), properties.getConsumer().getName()),
                        StreamReadOptions.empty()
                                .count(properties.getConsumer().getBatchSize())
                                .block(properties.getConsumer().getBlockTimeout()),
                        StreamOffset.create(properties.mainStreamKey(), ReadOffset.lastConsumed())
                );
                if (records == null || records.isEmpty()) {
                    continue;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    executor.execute(record, properties.getConsumer().getName());
                }
            } catch (Exception e) {
                if (running.get()) {
                    log.warn("[StreamTask] consumer loop error", e);
                }
            }
        }
        log.info("[StreamTask] consumer loop stopped consumer={}", properties.getConsumer().getName());
    }
}
