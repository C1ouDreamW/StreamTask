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
        String consumerName
) {
}
