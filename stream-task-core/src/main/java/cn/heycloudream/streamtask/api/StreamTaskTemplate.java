package cn.heycloudream.streamtask.api;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import org.springframework.data.redis.connection.stream.RecordId;

public interface StreamTaskTemplate {
    RecordId publish(String taskType, String businessKey, Object payload);

    RecordId publish(StreamTaskEnvelope task);
}
