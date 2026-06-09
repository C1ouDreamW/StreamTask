package cn.heycloudream.streamtask.model;

public record StreamTaskOverview(
        String namespace,
        long streamLength,
        long pendingCount,
        Long groupLag,
        long dlqLength
) {
}
