package cn.heycloudream.streamtask.model;

public record PendingTaskInfo(
        String messageId,
        String consumerName,
        long idleTimeMillis,
        long deliveryCount
) {
}
