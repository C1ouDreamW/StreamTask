package cn.heycloudream.streamtask.support;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class StreamTaskSerializer {
    private final ObjectMapper objectMapper;

    public StreamTaskSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(Object payload) {
        if (payload instanceof String value) {
            return value;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new StreamTaskException("Failed to serialize payload", e);
        }
    }

    public Map<String, String> toMap(StreamTaskEnvelope task) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("taskType", task.taskType());
        map.put("businessKey", task.businessKey());
        map.put("payload", task.payload());
        map.put("traceId", nullToEmpty(task.traceId()));
        map.put("source", nullToEmpty(task.source()));
        map.put("schemaVersion", String.valueOf(task.schemaVersion()));
        map.put("createdAt", String.valueOf(task.createdAt()));
        map.put("replayedFrom", nullToEmpty(task.replayedFrom()));
        return map;
    }

    public StreamTaskEnvelope fromMap(Map<Object, Object> raw) {
        return new StreamTaskEnvelope(
                value(raw, "taskType"),
                value(raw, "businessKey"),
                value(raw, "payload"),
                blankToNull(value(raw, "traceId")),
                blankToNull(value(raw, "source")),
                parseInt(value(raw, "schemaVersion"), 1),
                parseLong(value(raw, "createdAt"), System.currentTimeMillis()),
                blankToNull(value(raw, "replayedFrom"))
        );
    }

    private static String value(Map<Object, Object> raw, String key) {
        Object value = raw.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static Integer parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static Long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (Exception ignored) {
            return fallback;
        }
    }
}
