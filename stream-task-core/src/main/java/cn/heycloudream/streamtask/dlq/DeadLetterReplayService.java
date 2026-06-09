package cn.heycloudream.streamtask.dlq;

import cn.heycloudream.streamtask.api.StreamTaskTemplate;
import cn.heycloudream.streamtask.model.DeadLetterTask;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.recovery.AttemptRepository;
import cn.heycloudream.streamtask.support.StreamTaskException;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

public class DeadLetterReplayService {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final DeadLetterService deadLetterService;
    private final StreamTaskTemplate streamTaskTemplate;
    private final AttemptRepository attemptRepository;

    public DeadLetterReplayService(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            DeadLetterService deadLetterService,
            StreamTaskTemplate streamTaskTemplate,
            AttemptRepository attemptRepository
    ) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.deadLetterService = deadLetterService;
        this.streamTaskTemplate = streamTaskTemplate;
        this.attemptRepository = attemptRepository;
    }

    public RecordId replay(String dlqMessageId) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(properties.dlqStreamKey(), Range.closed(dlqMessageId, dlqMessageId));
        if (records == null || records.isEmpty()) {
            throw new StreamTaskException("DLQ message not found: " + dlqMessageId);
        }
        DeadLetterTask dlq = deadLetterService.fromMap(records.get(0).getValue());
        StreamTaskEnvelope task = StreamTaskEnvelope.create(dlq.taskType(), dlq.businessKey(), dlq.payload())
                .withReplaySource(dlqMessageId);
        RecordId newId = streamTaskTemplate.publish(task);
        attemptRepository.reset(newId.getValue());
        return newId;
    }
}
