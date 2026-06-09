package cn.heycloudream.streamtask.recovery;

import cn.heycloudream.streamtask.support.StreamTaskProperties;
import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.models.stream.ClaimedMessages;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PendingMessageClaimer {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;

    public PendingMessageClaimer(StringRedisTemplate redisTemplate, StreamTaskProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public AutoClaimResult autoClaim(String consumerName, String startId) {
        return redisTemplate.execute((RedisCallback<AutoClaimResult>) connection -> {
            Object nativeConnection = connection.getNativeConnection();
            if (!(nativeConnection instanceof RedisStreamAsyncCommands<?, ?> streamCommands)) {
                throw new IllegalStateException("XAUTOCLAIM requires Lettuce RedisStreamAsyncCommands");
            }
            @SuppressWarnings("unchecked")
            RedisStreamAsyncCommands<byte[], byte[]> commands =
                    (RedisStreamAsyncCommands<byte[], byte[]>) streamCommands;
            XAutoClaimArgs<byte[]> args = new XAutoClaimArgs<byte[]>()
                    .consumer(Consumer.from(bytes(properties.getGroup()), bytes(consumerName)))
                    .minIdleTime(properties.getRecovery().getMinIdleTime())
                    .startId(startId)
                    .count(properties.getRecovery().getClaimBatchSize());
            try {
                return fromClaimedMessages(commands.xautoclaim(bytes(properties.mainStreamKey()), args).get());
            } catch (Exception error) {
                throw new IllegalStateException("XAUTOCLAIM failed", error);
            }
        });
    }

    private static AutoClaimResult fromClaimedMessages(ClaimedMessages<byte[], byte[]> claimed) {
        if (claimed == null) {
            return AutoClaimResult.empty();
        }
        List<ClaimedStreamRecord> records = new ArrayList<>();
        for (StreamMessage<byte[], byte[]> message : claimed.getMessages()) {
            Map<Object, Object> body = new LinkedHashMap<>();
            message.getBody().forEach((key, value) -> body.put(text(key), text(value)));
            String messageId = message.getId();
            records.add(new ClaimedStreamRecord(messageId, body));
        }
        return new AutoClaimResult(claimed.getId(), records);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }
}
