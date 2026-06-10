package cn.heycloudream.streamtask;

import cn.heycloudream.streamtask.consumer.ConsumerGroupInitializer;
import cn.heycloudream.streamtask.consumer.StreamTaskConsumerLoop;
import cn.heycloudream.streamtask.consumer.StreamTaskExecutor;
import cn.heycloudream.streamtask.consumer.StreamTaskHandlerRegistry;
import cn.heycloudream.streamtask.dlq.DeadLetterReplayService;
import cn.heycloudream.streamtask.dlq.DeadLetterService;
import cn.heycloudream.streamtask.idempotent.IdempotentGuard;
import cn.heycloudream.streamtask.idempotent.Lease;
import cn.heycloudream.streamtask.idempotent.RedisIdempotentGuard;
import cn.heycloudream.streamtask.idempotent.LeaseWatchdog;
import cn.heycloudream.streamtask.metrics.NoopStreamTaskMetrics;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.producer.RedisStreamTaskTemplate;
import cn.heycloudream.streamtask.recovery.AttemptRepository;
import cn.heycloudream.streamtask.recovery.AutoClaimResult;
import cn.heycloudream.streamtask.recovery.PendingMessageClaimer;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class RedisStreamTaskIntegrationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    private StringRedisTemplate redisTemplate;
    private StreamTaskProperties properties;
    private StreamTaskSerializer serializer;
    private RedisStreamTaskTemplate template;

    @BeforeEach
    void setUp() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration);
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        redisTemplate.getRequiredConnectionFactory().getConnection().serverCommands().flushAll();

        properties = new StreamTaskProperties();
        properties.setNamespace("test-" + UUID.randomUUID());
        properties.setGroup("stream-task-test-group");
        properties.getConsumer().setName("test-consumer");
        properties.getConsumer().setBatchSize(10);
        properties.getRecovery().setMinIdleTime(Duration.ZERO);

        serializer = new StreamTaskSerializer(new ObjectMapper());
        template = new RedisStreamTaskTemplate(
                redisTemplate,
                properties,
                serializer,
                new StreamTaskEnvelopeValidator(256 * 1024),
                new NoopStreamTaskMetrics()
        );
        new ConsumerGroupInitializer(redisTemplate, properties).afterSingletonsInstantiated();
    }

    @Test
    void publishesTaskToStream() {
        template.publish("demo.success", "biz-1", "{}");

        assertThat(redisTemplate.opsForStream().size(properties.mainStreamKey())).isEqualTo(1);
    }

    @Test
    void consumesSuccessfullyAndAcknowledges() {
        AtomicInteger handled = new AtomicInteger();
        StreamTaskExecutor executor = executor(task -> handled.incrementAndGet());
        template.publish("demo.success", "biz-1", "{}");

        executor.execute(readOne(), properties.getConsumer().getName());

        assertThat(handled).hasValue(1);
        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isZero();
    }

    @Test
    void failedTaskStaysPendingBeforeMaxAttempts() {
        properties.getRetry().setMaxAttempts(3);
        StreamTaskExecutor executor = executor(task -> {
            throw new IllegalStateException("boom");
        });
        template.publish("demo.fail", "biz-1", "{}");

        executor.execute(readOne(), properties.getConsumer().getName());

        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isEqualTo(1);
        assertThat(redisTemplate.opsForStream().size(properties.dlqStreamKey())).isEqualTo(0);
    }

    @Test
    void failedTaskMovesToDlqWhenMaxAttemptsReached() {
        properties.getRetry().setMaxAttempts(1);
        StreamTaskExecutor executor = executor(task -> {
            throw new IllegalStateException("boom");
        });
        template.publish("demo.fail", "biz-1", "{}");

        executor.execute(readOne(), properties.getConsumer().getName());

        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isZero();
        assertThat(redisTemplate.opsForStream().size(properties.dlqStreamKey())).isEqualTo(1);
        assertThat(redisTemplate.opsForHash().hasKey(properties.attemptsKey(), recordId())).isFalse();
        assertThat(redisTemplate.opsForHash().hasKey(properties.lastErrorKey(), recordId())).isFalse();
    }

    @Test
    void malformedMessageMovesToDlqAndAcknowledges() {
        StreamTaskExecutor executor = executor(task -> {
            throw new AssertionError("malformed messages must not reach handler");
        });
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("businessKey", "broken-biz");
        fields.put("payload", "{}");
        redisTemplate.opsForStream().add(properties.mainStreamKey(), fields);

        executor.execute(readOne(), properties.getConsumer().getName());

        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isZero();
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(properties.dlqStreamKey(), org.springframework.data.domain.Range.unbounded());
        assertThat(dlqRecords).hasSize(1);
        assertThat(dlqRecords.get(0).getValue())
                .containsEntry("malformed", "true")
                .containsEntry("businessKey", "broken-biz");
    }

    @Test
    void shouldNotAckWhenLeaseIsLostBeforeMarkDone() {
        StreamTaskExecutor executor = executor(task ->
                redisTemplate.delete(properties.idempotentKey(task.taskType(), task.businessKey()))
        );
        template.publish("demo.success", "lost-lease", "{}");

        executor.execute(readOne(), properties.getConsumer().getName());

        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isEqualTo(1);
        assertThat(redisTemplate.opsForStream().size(properties.dlqStreamKey())).isEqualTo(0);
    }

    @Test
    void shouldScanAllPendingMessagesAcrossMultipleAutoClaimPages() throws InterruptedException {
        properties.getRecovery().setMinIdleTime(Duration.ofMillis(1));
        properties.getRecovery().setClaimBatchSize(2);
        for (int i = 0; i < 5; i++) {
            template.publish("demo.success", "biz-" + i, "{}");
        }
        read(5, "crashed-consumer");
        Thread.sleep(20);

        PendingMessageClaimer claimer = new PendingMessageClaimer(redisTemplate, properties);
        Set<String> claimedIds = new LinkedHashSet<>();
        String cursor = "0-0";
        do {
            AutoClaimResult result = claimer.autoClaim("recovery-consumer", cursor);
            cursor = result.nextStartId();
            result.records().forEach(record -> claimedIds.add(record.messageId()));
        } while (!"0-0".equals(cursor));

        assertThat(claimedIds).hasSize(5);
    }

    @Test
    void duplicateBusinessKeySkipsBusinessHandlerAfterDone() {
        AtomicInteger handled = new AtomicInteger();
        StreamTaskExecutor executor = executor(task -> handled.incrementAndGet());
        template.publish("demo.success", "same-biz", "{}");
        template.publish("demo.success", "same-biz", "{}");

        List<MapRecord<String, Object, Object>> records = read(2);
        records.forEach(record -> executor.execute(record, properties.getConsumer().getName()));

        assertThat(handled).hasValue(1);
        assertThat(redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup()).getTotalPendingMessages())
                .isZero();
    }

    @Test
    void shouldReplayDeadLetterOnlyOnce() {
        properties.getRetry().setMaxAttempts(1);
        StreamTaskExecutor executor = executor(task -> {
            throw new IllegalStateException("boom");
        });
        template.publish("demo.success", "replay-once", "{}");
        executor.execute(readOne(), properties.getConsumer().getName());
        String dlqMessageId = redisTemplate.opsForStream()
                .range(properties.dlqStreamKey(), org.springframework.data.domain.Range.unbounded())
                .get(0)
                .getId()
                .getValue();

        DeadLetterService deadLetterService = new DeadLetterService(redisTemplate, properties, new NoopStreamTaskMetrics());
        DeadLetterReplayService replayService = new DeadLetterReplayService(
                redisTemplate,
                properties,
                deadLetterService,
                serializer,
                new StreamTaskEnvelopeValidator(256 * 1024)
        );
        RecordId first = replayService.replay(dlqMessageId);
        RecordId second = replayService.replay(dlqMessageId);

        assertThat(second).isEqualTo(first);
        assertThat(redisTemplate.opsForStream().size(properties.mainStreamKey())).isEqualTo(2);
        assertThat(redisTemplate.opsForValue().get(properties.dlqReplayKey(dlqMessageId))).isEqualTo(first.getValue());
    }

    @Test
    void watchdogCancelsItselfWhenRenewFails() throws InterruptedException {
        properties.getIdempotent().setRenewInterval(Duration.ofMillis(10));
        CountDownLatch renewCalled = new CountDownLatch(1);
        CountingMetrics metrics = new CountingMetrics();
        IdempotentGuard guard = new IdempotentGuard() {
            @Override
            public Lease acquire(StreamTaskEnvelope task) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean markDone(Lease lease) {
                return false;
            }

            @Override
            public boolean release(Lease lease) {
                return false;
            }

            @Override
            public boolean renew(Lease lease) {
                renewCalled.countDown();
                return false;
            }
        };
        LeaseWatchdog watchdog = new LeaseWatchdog(guard, properties, metrics);

        ScheduledFuture<?> future = watchdog.watch(new Lease("lease-key", "token", cn.heycloudream.streamtask.idempotent.LeaseStatus.ACQUIRED));

        assertThat(renewCalled.await(1, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);
        assertThat(future.isCancelled()).isTrue();
        assertThat(metrics.leaseLostCount).hasValue(1);
        watchdog.shutdown();
    }

    @Test
    void streamMaxLenIsAppliedDuringPublish() {
        properties.getStream().setMaxLen(2);
        properties.getStream().setApproximateTrimming(false);

        template.publish("demo.success", "trim-1", "{}");
        template.publish("demo.success", "trim-2", "{}");
        template.publish("demo.success", "trim-3", "{}");

        assertThat(redisTemplate.opsForStream().size(properties.mainStreamKey())).isEqualTo(2);
    }

    @Test
    void consumerLoopWaitsForShutdownTimeoutBeforeForcingStop() {
        properties.getConsumer().setShutdownTimeout(Duration.ofMillis(120));
        properties.getConsumer().setBlockTimeout(Duration.ofSeconds(1));
        StreamTaskConsumerLoop loop = new StreamTaskConsumerLoop(redisTemplate, properties, executor(task -> {
        }));
        loop.start();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long started = System.nanoTime();

        loop.stop();

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(100);
        assertThat(loop.isRunning()).isFalse();
    }

    private StreamTaskExecutor executor(ThrowingHandler handler) {
        AttemptRepository attemptRepository = new AttemptRepository(redisTemplate, properties);
        DeadLetterService deadLetterService = new DeadLetterService(redisTemplate, properties, new NoopStreamTaskMetrics());
        RedisIdempotentGuard guard = new RedisIdempotentGuard(redisTemplate, properties);
        return new StreamTaskExecutor(
                redisTemplate,
                properties,
                serializer,
                new StreamTaskHandlerRegistry(List.of(new TestHandler(handler))),
                attemptRepository,
                deadLetterService,
                guard,
                new LeaseWatchdog(guard, properties, new NoopStreamTaskMetrics()),
                new NoopStreamTaskMetrics()
        );
    }

    private MapRecord<String, Object, Object> readOne() {
        return read(1).get(0);
    }

    private List<MapRecord<String, Object, Object>> read(int count) {
        return read(count, properties.getConsumer().getName());
    }

    private List<MapRecord<String, Object, Object>> read(int count, String consumerName) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(properties.getGroup(), consumerName),
                StreamReadOptions.empty().count(count).block(Duration.ofSeconds(1)),
                StreamOffset.create(properties.mainStreamKey(), ReadOffset.lastConsumed())
        );
        assertThat(records).isNotNull().hasSize(count);
        return records;
    }

    private String recordId() {
        return redisTemplate.opsForStream()
                .range(properties.dlqStreamKey(), org.springframework.data.domain.Range.unbounded())
                .get(0)
                .getValue()
                .get("originalMessageId")
                .toString();
    }

    private interface ThrowingHandler {
        void handle(StreamTaskEnvelope task) throws Exception;
    }

    private record TestHandler(ThrowingHandler delegate) implements cn.heycloudream.streamtask.api.StreamTaskHandler {
        @Override
        public String taskType() {
            return "demo.success";
        }

        @Override
        public void handle(StreamTaskEnvelope task) throws Exception {
            delegate.handle(task);
        }
    }

    private static class CountingMetrics extends NoopStreamTaskMetrics {
        private final AtomicInteger leaseLostCount = new AtomicInteger();

        @Override
        public void leaseLost(String taskType) {
            leaseLostCount.incrementAndGet();
        }
    }
}
