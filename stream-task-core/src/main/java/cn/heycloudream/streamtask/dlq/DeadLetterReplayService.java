package cn.heycloudream.streamtask.dlq;

import cn.heycloudream.streamtask.model.DeadLetterTask;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskException;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeadLetterReplayService {
    private static final DefaultRedisScript<String> REPLAY_SCRIPT = new DefaultRedisScript<>("""
            local existing = redis.call('GET', KEYS[2])
            if existing then
                return existing
            end
            local fields = {}
            for i = 2, #ARGV do
                table.insert(fields, ARGV[i])
            end
            local newId = redis.call('XADD', KEYS[1], '*', unpack(fields))
            redis.call('SET', KEYS[2], newId, 'EX', ARGV[1])
            return newId
            """, String.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final DeadLetterService deadLetterService;
    private final StreamTaskSerializer serializer;
    private final StreamTaskEnvelopeValidator validator;

    public DeadLetterReplayService(
            StringRedisTemplate redisTemplate,
            StreamTaskProperties properties,
            DeadLetterService deadLetterService,
            StreamTaskSerializer serializer,
            StreamTaskEnvelopeValidator validator
    ) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.deadLetterService = deadLetterService;
        this.serializer = serializer;
        this.validator = validator;
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
        validator.validate(task);
        String newId = redisTemplate.execute(
                REPLAY_SCRIPT,
                List.of(properties.mainStreamKey(), properties.dlqReplayKey(dlqMessageId)),
                replayArgs(task)
        );
        return RecordId.of(newId == null ? "0-0" : newId);
    }

    private String replayTtlSeconds() {
        long seconds = properties.getDlq().getReplayTtl().toSeconds();
        return String.valueOf(Math.max(1, seconds));
    }

    private String[] replayArgs(StreamTaskEnvelope task) {
        Map<String, String> fields = serializer.toMap(task);
        List<String> args = new ArrayList<>();
        args.add(replayTtlSeconds());
        fields.forEach((key, value) -> {
            args.add(key);
            args.add(value == null ? "" : value);
        });
        return args.toArray(String[]::new);
    }
}
