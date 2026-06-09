package cn.heycloudream.streamtask.idempotent;

public record Lease(String key, String token, LeaseStatus status) {
    public boolean acquired() {
        return status == LeaseStatus.ACQUIRED;
    }

    public boolean done() {
        return status == LeaseStatus.DONE;
    }
}
