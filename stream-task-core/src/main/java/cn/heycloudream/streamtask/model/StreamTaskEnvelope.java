package cn.heycloudream.streamtask.model;

public record StreamTaskEnvelope(
        String taskType,
        String businessKey,
        String payload,
        String traceId,
        String source,
        Integer schemaVersion,
        Long createdAt,
        String replayedFrom
) {
    public static StreamTaskEnvelope create(String taskType, String businessKey, String payload) {
        return new StreamTaskEnvelope(taskType, businessKey, payload, null, null, 1, System.currentTimeMillis(), null);
    }

    public StreamTaskEnvelope withReplaySource(String replayedFrom) {
        return new StreamTaskEnvelope(taskType, businessKey, payload, traceId, source, schemaVersion, createdAt, replayedFrom);
    }
}
