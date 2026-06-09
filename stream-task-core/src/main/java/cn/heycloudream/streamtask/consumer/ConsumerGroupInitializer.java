package cn.heycloudream.streamtask.consumer;

import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;

public class ConsumerGroupInitializer implements SmartInitializingSingleton {
    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupInitializer.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;

    public ConsumerGroupInitializer(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try {
            redisTemplate.execute((RedisCallback<Object>) connection -> {
                connection.streamCommands().xGroupCreate(
                        properties.mainStreamKey().getBytes(StandardCharsets.UTF_8),
                        properties.getGroup(),
                        ReadOffset.from("0-0"),
                        true
                );
                return null;
            });
            log.info("[StreamTask] consumer group created stream={} group={}", properties.mainStreamKey(), properties.getGroup());
        } catch (Exception e) {
            if (isBusyGroup(e)) {
                log.info("[StreamTask] consumer group already exists stream={} group={}", properties.mainStreamKey(), properties.getGroup());
                return;
            }
            throw e;
        }
    }

    private static boolean isBusyGroup(Throwable e) {
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("BUSYGROUP")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
