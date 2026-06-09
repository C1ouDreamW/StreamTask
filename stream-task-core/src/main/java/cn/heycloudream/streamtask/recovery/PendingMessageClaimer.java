package cn.heycloudream.streamtask.recovery;

import cn.heycloudream.streamtask.support.StreamTaskProperties;
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

    public List<ClaimedStreamRecord> autoClaim(String consumerName) {
        return redisTemplate.execute((RedisCallback<List<ClaimedStreamRecord>>) connection -> {
            Object raw = connection.execute("XAUTOCLAIM",
                    bytes(properties.mainStreamKey()),
                    bytes(properties.getGroup()),
                    bytes(consumerName),
                    bytes(String.valueOf(properties.getRecovery().getMinIdleTime().toMillis())),
                    bytes("0-0"),
                    bytes("COUNT"),
                    bytes(String.valueOf(properties.getRecovery().getClaimBatchSize()))
            );
            return parse(raw);
        });
    }

    private static List<ClaimedStreamRecord> parse(Object raw) {
        if (!(raw instanceof List<?> root) || root.size() < 2 || !(root.get(1) instanceof List<?> entries)) {
            return List.of();
        }
        List<ClaimedStreamRecord> records = new ArrayList<>();
        for (Object item : entries) {
            if (!(item instanceof List<?> entry) || entry.size() < 2) {
                continue;
            }
            String messageId = text(entry.get(0));
            Map<Object, Object> body = parseBody(entry.get(1));
            records.add(new ClaimedStreamRecord(messageId, body));
        }
        return records;
    }

    private static Map<Object, Object> parseBody(Object fields) {
        Map<Object, Object> body = new LinkedHashMap<>();
        if (!(fields instanceof List<?> values)) {
            return body;
        }
        for (int i = 0; i + 1 < values.size(); i += 2) {
            body.put(text(values.get(i)), text(values.get(i + 1)));
        }
        return body;
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
