package cn.heycloudream.streamtask.autoconfigure;

import cn.heycloudream.streamtask.admin.StreamTaskAdminController;
import cn.heycloudream.streamtask.admin.StreamTaskOverviewService;
import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.api.StreamTaskTemplate;
import cn.heycloudream.streamtask.consumer.ConsumerGroupInitializer;
import cn.heycloudream.streamtask.consumer.StreamTaskConsumerLoop;
import cn.heycloudream.streamtask.consumer.StreamTaskExecutor;
import cn.heycloudream.streamtask.consumer.StreamTaskHandlerRegistry;
import cn.heycloudream.streamtask.dlq.DeadLetterReplayService;
import cn.heycloudream.streamtask.dlq.DeadLetterService;
import cn.heycloudream.streamtask.idempotent.IdempotentGuard;
import cn.heycloudream.streamtask.idempotent.LeaseWatchdog;
import cn.heycloudream.streamtask.idempotent.NoopIdempotentGuard;
import cn.heycloudream.streamtask.idempotent.RedisIdempotentGuard;
import cn.heycloudream.streamtask.metrics.NoopStreamTaskMetrics;
import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.producer.RedisStreamTaskTemplate;
import cn.heycloudream.streamtask.recovery.AttemptRepository;
import cn.heycloudream.streamtask.recovery.PendingMessageClaimer;
import cn.heycloudream.streamtask.recovery.PendingRecoveryScheduler;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(StringRedisTemplate.class)
@ConditionalOnProperty(prefix = "stream-task", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StreamTaskAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "stream-task")
    @ConditionalOnMissingBean
    public StreamTaskProperties streamTaskProperties() {
        return new StreamTaskProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper streamTaskObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskSerializer streamTaskSerializer(ObjectMapper objectMapper) {
        return new StreamTaskSerializer(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskEnvelopeValidator streamTaskEnvelopeValidator() {
        return new StreamTaskEnvelopeValidator(256 * 1024);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskMetrics streamTaskMetrics(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        MeterRegistry registry = meterRegistryProvider.getIfAvailable();
        return registry == null ? new NoopStreamTaskMetrics() : new StreamTaskMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskTemplate streamTaskTemplate(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            StreamTaskSerializer serializer,
            StreamTaskEnvelopeValidator validator,
            StreamTaskMetrics metrics
    ) {
        return new RedisStreamTaskTemplate(redisTemplate, properties, serializer, validator, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskHandlerRegistry streamTaskHandlerRegistry(ObjectProvider<List<StreamTaskHandler>> handlers) {
        return new StreamTaskHandlerRegistry(handlers.getIfAvailable(List::of));
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerGroupInitializer consumerGroupInitializer(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        return new ConsumerGroupInitializer(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public AttemptRepository attemptRepository(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        return new AttemptRepository(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterService deadLetterService(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            StreamTaskMetrics metrics
    ) {
        return new DeadLetterService(redisTemplate, properties, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public IdempotentGuard idempotentGuard(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        return properties.getIdempotent().isEnabled()
                ? new RedisIdempotentGuard(redisTemplate, properties)
                : new NoopIdempotentGuard();
    }

    @Bean
    @ConditionalOnMissingBean
    public LeaseWatchdog leaseWatchdog(IdempotentGuard guard, StreamTaskProperties properties) {
        return new LeaseWatchdog(guard, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskExecutor streamTaskExecutor(
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
        return new StreamTaskExecutor(
                redisTemplate,
                properties,
                serializer,
                handlerRegistry,
                attemptRepository,
                deadLetterService,
                idempotentGuard,
                leaseWatchdog,
                metrics
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskConsumerLoop streamTaskConsumerLoop(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            StreamTaskExecutor executor
    ) {
        return new StreamTaskConsumerLoop(redisTemplate, properties, executor);
    }

    @Bean
    @ConditionalOnMissingBean
    public PendingMessageClaimer pendingMessageClaimer(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        return new PendingMessageClaimer(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public PendingRecoveryScheduler pendingRecoveryScheduler(
            StreamTaskProperties properties,
            PendingMessageClaimer claimer,
            StreamTaskExecutor executor
    ) {
        return new PendingRecoveryScheduler(properties, claimer, executor);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterReplayService deadLetterReplayService(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            DeadLetterService deadLetterService,
            StreamTaskTemplate streamTaskTemplate,
            AttemptRepository attemptRepository
    ) {
        return new DeadLetterReplayService(redisTemplate, properties, deadLetterService, streamTaskTemplate, attemptRepository);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamTaskOverviewService streamTaskOverviewService(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            DeadLetterService deadLetterService
    ) {
        return new StreamTaskOverviewService(redisTemplate, properties, deadLetterService);
    }

    @Bean
    @ConditionalOnBean(StreamTaskOverviewService.class)
    @ConditionalOnProperty(prefix = "stream-task.admin", name = "enabled", havingValue = "true")
    public StreamTaskAdminController streamTaskAdminController(
            StreamTaskOverviewService overviewService,
            DeadLetterReplayService replayService
    ) {
        return new StreamTaskAdminController(overviewService, replayService);
    }
}
