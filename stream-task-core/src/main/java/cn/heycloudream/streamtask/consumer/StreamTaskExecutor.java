package cn.heycloudream.streamtask.consumer;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.dlq.DeadLetterService;
import cn.heycloudream.streamtask.idempotent.IdempotentGuard;
import cn.heycloudream.streamtask.idempotent.Lease;
import cn.heycloudream.streamtask.idempotent.LeaseWatchdog;
import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.recovery.AttemptRepository;
import cn.heycloudream.streamtask.recovery.MaxAttemptsExceededException;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

public class StreamTaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(StreamTaskExecutor.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final StreamTaskSerializer serializer;
    private final StreamTaskHandlerRegistry handlerRegistry;
    private final AttemptRepository attemptRepository;
    private final DeadLetterService deadLetterService;
    private final IdempotentGuard idempotentGuard;
    private final LeaseWatchdog leaseWatchdog;
    private final StreamTaskMetrics metrics;

    public StreamTaskExecutor(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            StreamTaskSerializer serializer,
            StreamTaskHandlerRegistry handlerRegistry,
            AttemptRepository attemptRepository,
            DeadLetterService deadLetterService,
            IdempotentGuard idempotentGuard,
            LeaseWatchdog leaseWatchdog,
            StreamTaskMetrics metrics
    ) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.serializer = serializer;
        this.handlerRegistry = handlerRegistry;
        this.attemptRepository = attemptRepository;
        this.deadLetterService = deadLetterService;
        this.idempotentGuard = idempotentGuard;
        this.leaseWatchdog = leaseWatchdog;
        this.metrics = metrics;
    }

    public void execute(MapRecord<String, Object, Object> record, String consumerName) {
        execute(record.getId().getValue(), record.getValue(), consumerName);
    }

    public void execute(String messageId, Map<Object, Object> raw, String consumerName) {
        StreamTaskEnvelope task;
        try {
            task = deserialize(raw);
        } catch (MalformedTaskException error) {
            handleMalformedMessage(messageId, raw, error, consumerName);
            return;
        }
        int attempt = attemptRepository.increment(messageId);
        Instant startedAt = Instant.now();
        Lease lease = null;
        ScheduledFuture<?> watchdog = null;
        try {
            lease = idempotentGuard.acquire(task);
            if (lease.done()) {
                ack(messageId);
                attemptRepository.reset(messageId);
                metrics.consumed(task.taskType(), "duplicate_done");
                log.info("[StreamTask] skip done messageId={} taskType={} businessKey={} attempt={} consumer={}",
                        messageId, task.taskType(), task.businessKey(), attempt, consumerName);
                return;
            }
            if (!lease.acquired()) {
                metrics.consumed(task.taskType(), "busy");
                log.info("[StreamTask] lease busy messageId={} taskType={} businessKey={} attempt={} consumer={}",
                        messageId, task.taskType(), task.businessKey(), attempt, consumerName);
                return;
            }

            watchdog = leaseWatchdog.watch(lease);
            StreamTaskHandler handler = handlerRegistry.getRequired(task.taskType());
            handler.handle(task);
            boolean marked = idempotentGuard.markDone(lease);
            if (!marked) {
                throw new LeaseLostException("lease lost before task completion");
            }
            ack(messageId);
            attemptRepository.reset(messageId);
            metrics.consumed(task.taskType(), "success");
            metrics.execution(task.taskType(), "success", Duration.between(startedAt, Instant.now()));
            log.info("[StreamTask] success messageId={} taskType={} businessKey={} attempt={} consumer={}",
                    messageId, task.taskType(), task.businessKey(), attempt, consumerName);
        } catch (Throwable error) {
            if (lease != null && lease.acquired()) {
                idempotentGuard.release(lease);
            }
            attemptRepository.recordError(messageId, error);
            if (error instanceof LeaseLostException) {
                metrics.consumed(task.taskType(), "lease_lost");
                metrics.leaseLost(task.taskType());
                metrics.execution(task.taskType(), "lease_lost", Duration.between(startedAt, Instant.now()));
                log.warn("[StreamTask] lease lost before ack messageId={} taskType={} businessKey={} attempt={} consumer={}",
                        messageId, task.taskType(), task.businessKey(), attempt, consumerName);
                return;
            }
            metrics.consumed(task.taskType(), "failed");
            metrics.execution(task.taskType(), "failed", Duration.between(startedAt, Instant.now()));
            log.warn("[StreamTask] failed messageId={} taskType={} businessKey={} attempt={} consumer={} error={}",
                    messageId, task.taskType(), task.businessKey(), attempt, consumerName, error.toString());
            if (attempt >= properties.getRetry().getMaxAttempts()) {
                Throwable dlqError = error instanceof MaxAttemptsExceededException
                        ? error
                        : new MaxAttemptsExceededException("max attempts exceeded: " + error.getMessage());
                deadLetterService.moveToDeadLetterAtomically(
                        deadLetterService.fromFailure(messageId, task, attempt, dlqError, consumerName)
                );
                log.warn("[StreamTask] moved to DLQ messageId={} taskType={} businessKey={} attempt={} consumer={}",
                        messageId, task.taskType(), task.businessKey(), attempt, consumerName);
            } else {
                metrics.retry(task.taskType());
            }
        } finally {
            if (watchdog != null) {
                watchdog.cancel(false);
            }
        }
    }

    private StreamTaskEnvelope deserialize(Map<Object, Object> raw) {
        try {
            StreamTaskEnvelope task = serializer.fromMap(raw);
            if (task.taskType() == null || task.taskType().isBlank()) {
                throw new MalformedTaskException("taskType is required");
            }
            if (task.businessKey() == null || task.businessKey().isBlank()) {
                throw new MalformedTaskException("businessKey is required");
            }
            return task;
        } catch (MalformedTaskException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new MalformedTaskException("invalid stream task payload", error);
        }
    }

    private void handleMalformedMessage(
            String messageId,
            Map<Object, Object> raw,
            MalformedTaskException error,
            String consumerName
    ) {
        deadLetterService.moveToDeadLetterAtomically(
                deadLetterService.fromMalformed(messageId, raw, error, consumerName)
        );
        log.warn("[StreamTask] malformed message moved to DLQ messageId={} consumer={} error={}",
                messageId, consumerName, error.getMessage());
    }

    private void ack(String messageId) {
        redisTemplate.opsForStream().acknowledge(properties.mainStreamKey(), properties.getGroup(), messageId);
    }
}
