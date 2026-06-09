package cn.heycloudream.streamtask.idempotent;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.UUID;

public class RedisIdempotentGuard implements IdempotentGuard {
    private static final String DONE = "DONE";
    private static final String PROCESSING_PREFIX = "PROCESSING:";

    private static final DefaultRedisScript<Long> MARK_DONE_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('SET', KEYS[1], 'DONE', 'PX', ARGV[2])
                return 1
            end
            return 0
            """, Long.class);

    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;

    public RedisIdempotentGuard(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    @Override
    public Lease acquire(StreamTaskEnvelope task) {
        String key = properties.idempotentKey(task.taskType(), task.businessKey());
        String current = redisTemplate.opsForValue().get(key);
        if (DONE.equals(current)) {
            return new Lease(key, "", LeaseStatus.DONE);
        }
        String token = UUID.randomUUID().toString();
        String value = PROCESSING_PREFIX + token;
        Boolean ok = redisTemplate.opsForValue().setIfAbsent(key, value, properties.getIdempotent().getLeaseTime());
        return Boolean.TRUE.equals(ok)
                ? new Lease(key, value, LeaseStatus.ACQUIRED)
                : new Lease(key, token, LeaseStatus.BUSY);
    }

    @Override
    public boolean markDone(Lease lease) {
        if (lease.status() == LeaseStatus.DISABLED) {
            return true;
        }
        Long result = redisTemplate.execute(
                MARK_DONE_SCRIPT,
                List.of(lease.key()),
                lease.token(),
                String.valueOf(properties.getIdempotent().getDoneTtl().toMillis())
        );
        return result != null && result == 1;
    }

    @Override
    public boolean release(Lease lease) {
        if (lease.status() == LeaseStatus.DISABLED) {
            return true;
        }
        Long result = redisTemplate.execute(RELEASE_SCRIPT, List.of(lease.key()), lease.token());
        return result != null && result == 1;
    }

    @Override
    public boolean renew(Lease lease) {
        if (lease.status() == LeaseStatus.DISABLED) {
            return true;
        }
        Long result = redisTemplate.execute(
                RENEW_SCRIPT,
                List.of(lease.key()),
                lease.token(),
                String.valueOf(properties.getIdempotent().getLeaseTime().toMillis())
        );
        return result != null && result == 1;
    }
}
