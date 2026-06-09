package cn.heycloudream.streamtask.recovery;

import java.util.Map;

public record ClaimedStreamRecord(String messageId, Map<Object, Object> body) {
}
