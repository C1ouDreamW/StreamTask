package cn.heycloudream.streamtask.producer;

import cn.heycloudream.streamtask.api.StreamTaskTemplate;
import cn.heycloudream.streamtask.metrics.StreamTaskMetrics;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;

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
        RecordId recordId = redisTemplate.opsForStream().add(properties.mainStreamKey(), fields);
        if (properties.getStream().getMaxLen() > 0) {
            redisTemplate.opsForStream().trim(
                    properties.mainStreamKey(),
                    properties.getStream().getMaxLen(),
                    properties.getStream().isApproximateTrimming()
            );
        }
        metrics.published(task.taskType());
        return recordId;
    }
}
