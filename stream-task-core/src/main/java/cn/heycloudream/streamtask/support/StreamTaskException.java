package cn.heycloudream.streamtask.support;

public class StreamTaskException extends RuntimeException {
    public StreamTaskException(String message) {
        super(message);
    }

    public StreamTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
