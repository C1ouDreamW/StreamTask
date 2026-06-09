package cn.heycloudream.streamtask.idempotent;

public enum LeaseStatus {
    ACQUIRED,
    DONE,
    BUSY,
    DISABLED
}
