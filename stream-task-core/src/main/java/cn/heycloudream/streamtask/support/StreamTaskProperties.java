package cn.heycloudream.streamtask.support;

import java.time.Duration;

public class StreamTaskProperties {
    private boolean enabled = true;
    private String namespace = "default";
    private String streamKey;
    private String group = "stream-task-group";
    private Consumer consumer = new Consumer();
    private Retry retry = new Retry();
    private Recovery recovery = new Recovery();
    private Idempotent idempotent = new Idempotent();
    private Metrics metrics = new Metrics();
    private Admin admin = new Admin();
    private Stream stream = new Stream();
    private Dlq dlq = new Dlq();

    public String mainStreamKey() {
        return streamKey == null || streamKey.isBlank() ? "stream-task:{" + namespace + "}:main" : streamKey;
    }

    public String dlqStreamKey() {
        return "stream-task:{" + namespace + "}:dlq";
    }

    public String attemptsKey() {
        return "stream-task:{" + namespace + "}:attempts";
    }

    public String lastErrorKey() {
        return "stream-task:{" + namespace + "}:last-error";
    }

    public String idempotentKey(String taskType, String businessKey) {
        return "stream-task:{" + namespace + "}:idem:" + taskType + ":" + businessKey;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getStreamKey() {
        return streamKey;
    }

    public void setStreamKey(String streamKey) {
        this.streamKey = streamKey;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Recovery getRecovery() {
        return recovery;
    }

    public void setRecovery(Recovery recovery) {
        this.recovery = recovery;
    }

    public Idempotent getIdempotent() {
        return idempotent;
    }

    public void setIdempotent(Idempotent idempotent) {
        this.idempotent = idempotent;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }

    public Stream getStream() {
        return stream;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    public Dlq getDlq() {
        return dlq;
    }

    public void setDlq(Dlq dlq) {
        this.dlq = dlq;
    }

    public static class Consumer {
        private boolean enabled = true;
        private String name = "stream-task-consumer";
        private int batchSize = 10;
        private Duration blockTimeout = Duration.ofSeconds(2);
        private Duration shutdownTimeout = Duration.ofSeconds(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getBlockTimeout() {
            return blockTimeout;
        }

        public void setBlockTimeout(Duration blockTimeout) {
            this.blockTimeout = blockTimeout;
        }

        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }
    }

    public static class Retry {
        private int maxAttempts = 3;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }
    }

    public static class Recovery {
        private boolean enabled = true;
        private Duration scanInterval = Duration.ofSeconds(10);
        private Duration minIdleTime = Duration.ofSeconds(60);
        private int claimBatchSize = 20;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getScanInterval() {
            return scanInterval;
        }

        public void setScanInterval(Duration scanInterval) {
            this.scanInterval = scanInterval;
        }

        public Duration getMinIdleTime() {
            return minIdleTime;
        }

        public void setMinIdleTime(Duration minIdleTime) {
            this.minIdleTime = minIdleTime;
        }

        public int getClaimBatchSize() {
            return claimBatchSize;
        }

        public void setClaimBatchSize(int claimBatchSize) {
            this.claimBatchSize = claimBatchSize;
        }
    }

    public static class Idempotent {
        private boolean enabled = true;
        private Duration leaseTime = Duration.ofSeconds(120);
        private Duration renewInterval = Duration.ofSeconds(30);
        private Duration doneTtl = Duration.ofDays(7);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getLeaseTime() {
            return leaseTime;
        }

        public void setLeaseTime(Duration leaseTime) {
            this.leaseTime = leaseTime;
        }

        public Duration getRenewInterval() {
            return renewInterval;
        }

        public void setRenewInterval(Duration renewInterval) {
            this.renewInterval = renewInterval;
        }

        public Duration getDoneTtl() {
            return doneTtl;
        }

        public void setDoneTtl(Duration doneTtl) {
            this.doneTtl = doneTtl;
        }
    }

    public static class Metrics {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Admin {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Stream {
        private long maxLen = 10000;
        private boolean approximateTrimming = true;

        public long getMaxLen() {
            return maxLen;
        }

        public void setMaxLen(long maxLen) {
            this.maxLen = maxLen;
        }

        public boolean isApproximateTrimming() {
            return approximateTrimming;
        }

        public void setApproximateTrimming(boolean approximateTrimming) {
            this.approximateTrimming = approximateTrimming;
        }
    }

    public static class Dlq {
        private long maxLen = 5000;

        public long getMaxLen() {
            return maxLen;
        }

        public void setMaxLen(long maxLen) {
            this.maxLen = maxLen;
        }
    }
}
