package cn.heycloudream.streamtask.idempotent;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;

public interface IdempotentGuard {
    Lease acquire(StreamTaskEnvelope task);

    boolean markDone(Lease lease);

    boolean release(Lease lease);

    boolean renew(Lease lease);
}
