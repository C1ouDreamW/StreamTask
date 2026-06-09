package cn.heycloudream.streamtask.metrics;

import java.time.Duration;

public class NoopStreamTaskMetrics extends StreamTaskMetrics {
    public NoopStreamTaskMetrics() {
        super(io.micrometer.core.instrument.Metrics.globalRegistry);
    }

    @Override
    public void published(String taskType) {
    }

    @Override
    public void consumed(String taskType, String status) {
    }

    @Override
    public void retry(String taskType) {
    }

    @Override
    public void dlq(String taskType) {
    }

    @Override
    public void execution(String taskType, String status, Duration duration) {
    }
}
