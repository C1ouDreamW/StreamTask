package cn.heycloudream.streamtask.model;

public record DeadLetterTask(
        String originalStream,
        String originalMessageId,
        String taskType,
        String businessKey,
        String payload,
        Integer attempts,
        String errorType,
        String errorMessage,
        Long failedAt,
        String consumerName,
        Boolean malformed,
        String rawFieldsJson
) {
    public DeadLetterTask(
            String originalStream,
            String originalMessageId,
            String taskType,
            String businessKey,
            String payload,
            Integer attempts,
            String errorType,
            String errorMessage,
            Long failedAt,
            String consumerName
    ) {
        this(originalStream, originalMessageId, taskType, businessKey, payload, attempts, errorType, errorMessage,
                failedAt, consumerName, false, "");
    }
}
