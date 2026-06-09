package cn.heycloudream.streamtask.dlq;

import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.model.DeadLetterTask;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

public class DeadLetterService {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final StreamTaskMetrics metrics;

    public DeadLetterService(StringRedisTemplate redisTemplate, StreamTaskProperties properties, StreamTaskMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metrics = metrics;
    }

    public RecordId write(DeadLetterTask task) {
        Map<String, String> fields = toMap(task);
        RecordId id = redisTemplate.opsForStream().add(properties.dlqStreamKey(), fields);
        if (properties.getDlq().getMaxLen() > 0) {
            redisTemplate.opsForStream().trim(properties.dlqStreamKey(), properties.getDlq().getMaxLen(), true);
        }
        metrics.dlq(task.taskType());
        return id;
    }

    public DeadLetterTask fromFailure(
            String originalMessageId,
            StreamTaskEnvelope task,
            int attempts,
            Throwable error,
            String consumerName
    ) {
        return new DeadLetterTask(
                properties.mainStreamKey(),
                originalMessageId,
                task.taskType(),
                task.businessKey(),
                task.payload(),
                attempts,
                error.getClass().getName(),
                safeMessage(error),
                System.currentTimeMillis(),
                consumerName
        );
    }

    public DeadLetterTask fromMap(Map<Object, Object> map) {
        return new DeadLetterTask(
                value(map, "originalStream"),
                value(map, "originalMessageId"),
                value(map, "taskType"),
                value(map, "businessKey"),
                value(map, "payload"),
                Integer.parseInt(value(map, "attempts")),
                value(map, "errorType"),
                value(map, "errorMessage"),
                Long.parseLong(value(map, "failedAt")),
                value(map, "consumerName")
        );
    }

    private static Map<String, String> toMap(DeadLetterTask task) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("originalStream", task.originalStream());
        fields.put("originalMessageId", task.originalMessageId());
        fields.put("taskType", task.taskType());
        fields.put("businessKey", task.businessKey());
        fields.put("payload", task.payload());
        fields.put("attempts", String.valueOf(task.attempts()));
        fields.put("errorType", task.errorType());
        fields.put("errorMessage", task.errorMessage());
        fields.put("failedAt", String.valueOf(task.failedAt()));
        fields.put("consumerName", task.consumerName());
        return fields;
    }

    private static String value(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static String safeMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null) {
            return "";
        }
        return message.length() > 512 ? message.substring(0, 512) : message;
    }
}
