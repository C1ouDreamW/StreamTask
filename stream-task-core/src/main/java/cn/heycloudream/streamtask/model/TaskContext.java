package cn.heycloudream.streamtask.model;

public record TaskContext(
        String stream,
        String messageId,
        String consumerName,
        int attempt,
        String traceId
) {
}
