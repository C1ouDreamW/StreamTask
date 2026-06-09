package cn.heycloudream.streamtask.recovery;

import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.springframework.data.redis.core.StringRedisTemplate;

public class AttemptRepository {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;

    public AttemptRepository(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public int increment(String messageId) {
        Long attempt = redisTemplate.opsForHash().increment(properties.attemptsKey(), messageId, 1);
        return attempt == null ? 1 : attempt.intValue();
    }

    public int get(String messageId) {
        Object value = redisTemplate.opsForHash().get(properties.attemptsKey(), messageId);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(String.valueOf(value));
    }

    public void reset(String messageId) {
        redisTemplate.opsForHash().delete(properties.attemptsKey(), messageId);
        redisTemplate.opsForHash().delete(properties.lastErrorKey(), messageId);
    }

    public void recordError(String messageId, Throwable error) {
        String message = error.getClass().getSimpleName() + ":" + String.valueOf(error.getMessage());
        redisTemplate.opsForHash().put(properties.lastErrorKey(), messageId, message);
    }
}
