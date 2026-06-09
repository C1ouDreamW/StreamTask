package cn.heycloudream.streamtask.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

public class StreamTaskMetrics {
    private final MeterRegistry registry;

    public StreamTaskMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void published(String taskType) {
        registry.counter("stream.task.published", "task.type", taskType).increment();
    }

    public void consumed(String taskType, String status) {
        registry.counter("stream.task.consumed", "task.type", taskType, "status", status).increment();
    }

    public void retry(String taskType) {
        registry.counter("stream.task.retry", "task.type", taskType).increment();
    }

    public void dlq(String taskType) {
        registry.counter("stream.task.dlq", "task.type", taskType).increment();
    }

    public void execution(String taskType, String status, Duration duration) {
        Timer.builder("stream.task.execution")
                .tag("task.type", taskType)
                .tag("status", status)
                .register(registry)
                .record(duration);
    }
}
