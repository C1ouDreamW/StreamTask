package cn.heycloudream.streamtask.support;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class StreamTaskEnvelopeValidator {
    private static final Pattern TASK_TYPE_PATTERN = Pattern.compile("^[a-zA-Z0-9_.-]{1,128}$");

    private final int maxPayloadBytes;

    public StreamTaskEnvelopeValidator(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public void validate(StreamTaskEnvelope task) {
        if (task == null) {
            throw new StreamTaskException("task must not be null");
        }
        if (!StringUtils.hasText(task.taskType()) || !TASK_TYPE_PATTERN.matcher(task.taskType()).matches()) {
            throw new StreamTaskException("taskType must match " + TASK_TYPE_PATTERN.pattern());
        }
        if (!StringUtils.hasText(task.businessKey())) {
            throw new StreamTaskException("businessKey must not be blank");
        }
        if (!StringUtils.hasText(task.payload())) {
            throw new StreamTaskException("payload must not be blank");
        }
        int payloadBytes = task.payload().getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > maxPayloadBytes) {
            throw new StreamTaskException("payload exceeds max size: " + maxPayloadBytes + " bytes");
        }
        if (task.schemaVersion() == null || task.schemaVersion() < 1) {
            throw new StreamTaskException("schemaVersion must be positive");
        }
        if (task.createdAt() == null || task.createdAt() <= 0) {
            throw new StreamTaskException("createdAt must be positive");
        }
    }
}
