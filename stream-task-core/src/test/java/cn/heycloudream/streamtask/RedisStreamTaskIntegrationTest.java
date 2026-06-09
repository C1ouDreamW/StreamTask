package cn.heycloudream.streamtask;

import cn.heycloudream.streamtask.consumer.ConsumerGroupInitializer;
import cn.heycloudream.streamtask.consumer.StreamTaskExecutor;
import cn.heycloudream.streamtask.consumer.StreamTaskHandlerRegistry;
import cn.heycloudream.streamtask.dlq.DeadLetterService;
import cn.heycloudream.streamtask.idempotent.RedisIdempotentGuard;
import cn.heycloudream.streamtask.idempotent.LeaseWatchdog;
import cn.heycloudream.streamtask.metrics.NoopStreamTaskMetrics;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.producer.RedisStreamTaskTemplate;
import cn.heycloudream.streamtask.recovery.AttemptRepository;
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
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
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
                new LeaseWatchdog(guard, properties),
                new NoopStreamTaskMetrics()
        );
    }

    private MapRecord<String, Object, Object> readOne() {
        return read(1).get(0);
    }

    private List<MapRecord<String, Object, Object>> read(int count) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(properties.getGroup(), properties.getConsumer().getName()),
                StreamReadOptions.empty().count(count).block(Duration.ofSeconds(1)),
                StreamOffset.create(properties.mainStreamKey(), ReadOffset.lastConsumed())
        );
        assertThat(records).isNotNull().hasSize(count);
        return records;
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
}
