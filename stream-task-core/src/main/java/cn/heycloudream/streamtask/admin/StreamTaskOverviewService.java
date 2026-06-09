package cn.heycloudream.streamtask.admin;

import cn.heycloudream.streamtask.dlq.DeadLetterService;
import cn.heycloudream.streamtask.model.DeadLetterTask;
import cn.heycloudream.streamtask.model.PendingTaskInfo;
import cn.heycloudream.streamtask.model.StreamTaskOverview;
import cn.heycloudream.streamtask.support.StreamTaskProperties;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

public class StreamTaskOverviewService {
    private final StringRedisTemplate redisTemplate;
    private final StreamTaskProperties properties;
    private final DeadLetterService deadLetterService;

    public StreamTaskOverviewService(StringRedisTemplate redisTemplate, StreamTaskProperties properties, DeadLetterService deadLetterService) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.deadLetterService = deadLetterService;
    }

    public StreamTaskOverview overview() {
        Long streamLength = redisTemplate.opsForStream().size(properties.mainStreamKey());
        Long dlqLength = redisTemplate.opsForStream().size(properties.dlqStreamKey());
        long pendingCount = 0;
        try {
            PendingMessagesSummary summary = redisTemplate.opsForStream().pending(properties.mainStreamKey(), properties.getGroup());
            pendingCount = summary.getTotalPendingMessages();
        } catch (Exception ignored) {
            pendingCount = 0;
        }
        return new StreamTaskOverview(
                properties.getNamespace(),
                streamLength == null ? 0 : streamLength,
                pendingCount,
                null,
                dlqLength == null ? 0 : dlqLength
        );
    }

    public List<PendingTaskInfo> pending() {
        PendingMessages messages = redisTemplate.opsForStream()
                .pending(properties.mainStreamKey(), properties.getGroup(), Range.unbounded(), 100);
        return messages.stream()
                .map(this::toInfo)
                .toList();
    }

    public List<DeadLetterTask> dlq() {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .reverseRange(properties.dlqStreamKey(), Range.unbounded(), Limit.limit().count(100));
        if (records == null) {
            return List.of();
        }
        return records.stream()
                .map(record -> deadLetterService.fromMap(record.getValue()))
                .toList();
    }

    private PendingTaskInfo toInfo(PendingMessage message) {
        return new PendingTaskInfo(
                message.getIdAsString(),
                message.getConsumerName(),
                message.getElapsedTimeSinceLastDelivery().toMillis(),
                message.getTotalDeliveryCount()
        );
    }
}
