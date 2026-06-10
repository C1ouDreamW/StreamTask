package cn.heycloudream.streamtask.producer;

import cn.heycloudream.streamtask.api.StreamTaskTemplate;
import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisStreamTaskTemplate implements StreamTaskTemplate {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final StreamTaskSerializer serializer;
    private final StreamTaskEnvelopeValidator validator;
    private final StreamTaskMetrics metrics;

    public RedisStreamTaskTemplate(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            StreamTaskSerializer serializer,
            StreamTaskEnvelopeValidator validator,
            StreamTaskMetrics metrics
    ) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.serializer = serializer;
        this.validator = validator;
        this.metrics = metrics;
    }

    @Override
    public RecordId publish(String taskType, String businessKey, Object payload) {
        return publish(StreamTaskEnvelope.create(taskType, businessKey, serializer.toJson(payload)));
    }

    @Override
    public RecordId publish(StreamTaskEnvelope task) {
        validator.validate(task);
        Map<String, String> fields = serializer.toMap(task);
        RecordId recordId = xadd(fields);
        metrics.published(task.taskType());
        return recordId;
    }

    private RecordId xadd(Map<String, String> fields) {
        String id = redisTemplate.execute((RedisCallback<String>) connection -> {
            List<byte[]> args = new ArrayList<>();
            args.add(bytes(properties.mainStreamKey()));
            if (properties.getStream().getMaxLen() > 0) {
                args.add(bytes("MAXLEN"));
                if (properties.getStream().isApproximateTrimming()) {
                    args.add(bytes("~"));
                }
                args.add(bytes(String.valueOf(properties.getStream().getMaxLen())));
            }
            args.add(bytes("*"));
            fields.forEach((key, value) -> {
                args.add(bytes(key));
                args.add(bytes(value));
            });
            Object raw = connection.execute("XADD", args.toArray(byte[][]::new));
            if (raw instanceof byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return raw == null ? null : raw.toString();
        });
        return RecordId.of(id == null ? "0-0" : id);
    }

    private static byte[] bytes(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
    }
}
