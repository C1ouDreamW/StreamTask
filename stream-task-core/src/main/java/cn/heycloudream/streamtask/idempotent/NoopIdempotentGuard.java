package cn.heycloudream.streamtask.idempotent;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;

public class NoopIdempotentGuard implements IdempotentGuard {
    @Override
    public Lease acquire(StreamTaskEnvelope task) {
        return new Lease("", "", LeaseStatus.DISABLED);
    }

    @Override
    public boolean markDone(Lease lease) {
        return true;
    }

    @Override
    public boolean release(Lease lease) {
        return true;
    }

    @Override
    public boolean renew(Lease lease) {
        return true;
    }
}
