package cn.heycloudream.streamtask.model;

public record TaskExecutionResult(boolean success, String status, Throwable error) {
    public static TaskExecutionResult success(String status) {
        return new TaskExecutionResult(true, status, null);
    }

    public static TaskExecutionResult failed(Throwable error) {
        return new TaskExecutionResult(false, "FAILED", error);
    }
}
