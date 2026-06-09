package cn.heycloudream.streamtask.dlq;

import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.model.DeadLetterTask;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeadLetterService {
    private static final DefaultRedisScript<String> MOVE_TO_DLQ_SCRIPT = new DefaultRedisScript<>("""
            local dlqId = redis.call(
                'XADD',
                KEYS[2],
                '*',
                'originalStream', ARGV[3],
                'originalMessageId', ARGV[2],
                'taskType', ARGV[4],
                'businessKey', ARGV[5],
                'payload', ARGV[6],
                'attempts', ARGV[7],
                'errorType', ARGV[8],
                'errorMessage', ARGV[9],
                'failedAt', ARGV[10],
                'consumerName', ARGV[11],
                'malformed', ARGV[12],
                'rawFieldsJson', ARGV[13]
            )
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            redis.call('HDEL', KEYS[3], ARGV[2])
            redis.call('HDEL', KEYS[4], ARGV[2])
            if tonumber(ARGV[14]) > 0 then
                redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', ARGV[14])
            end
            return dlqId
            """, String.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    public RecordId moveToDeadLetterAtomically(DeadLetterTask task) {
        String id = redisTemplate.execute(
                MOVE_TO_DLQ_SCRIPT,
                List.of(
                        properties.mainStreamKey(),
                        properties.dlqStreamKey(),
                        properties.attemptsKey(),
                        properties.lastErrorKey()
                ),
                properties.getGroup(),
                task.originalMessageId(),
                nullToEmpty(task.originalStream()),
                nullToEmpty(task.taskType()),
                nullToEmpty(task.businessKey()),
                nullToEmpty(task.payload()),
                String.valueOf(task.attempts()),
                nullToEmpty(task.errorType()),
                nullToEmpty(task.errorMessage()),
                String.valueOf(task.failedAt()),
                nullToEmpty(task.consumerName()),
                String.valueOf(Boolean.TRUE.equals(task.malformed())),
                nullToEmpty(task.rawFieldsJson()),
                String.valueOf(properties.getDlq().getMaxLen())
        );
        metrics.dlq(task.taskType());
        if (Boolean.TRUE.equals(task.malformed())) {
            metrics.malformed();
        }
        return RecordId.of(id == null ? "0-0" : id);
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

    public DeadLetterTask fromMalformed(
            String originalMessageId,
            Map<Object, Object> raw,
            Throwable error,
            String consumerName
    ) {
        return new DeadLetterTask(
                properties.mainStreamKey(),
                originalMessageId,
                value(raw, "taskType"),
                value(raw, "businessKey"),
                value(raw, "payload"),
                0,
                error.getClass().getName(),
                safeMessage(error),
                System.currentTimeMillis(),
                consumerName,
                true,
                rawFieldsJson(raw)
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
                value(map, "consumerName"),
                Boolean.parseBoolean(value(map, "malformed")),
                value(map, "rawFieldsJson")
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
        fields.put("malformed", String.valueOf(Boolean.TRUE.equals(task.malformed())));
        fields.put("rawFieldsJson", nullToEmpty(task.rawFieldsJson()));
        return fields;
    }

    private static String value(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static String rawFieldsJson(Map<Object, Object> raw) {
        Map<String, String> fields = new LinkedHashMap<>();
        raw.forEach((key, value) -> fields.put(String.valueOf(key), String.valueOf(value)));
        try {
            return OBJECT_MAPPER.writeValueAsString(fields);
        } catch (JsonProcessingException ignored) {
            return fields.toString();
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String safeMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null) {
            return "";
        }
        return message.length() > 512 ? message.substring(0, 512) : message;
    }
}
