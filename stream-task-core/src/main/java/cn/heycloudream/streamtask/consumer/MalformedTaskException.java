package cn.heycloudream.streamtask.consumer;

public class MalformedTaskException extends RuntimeException {
    public MalformedTaskException(String message) {
        super(message);
    }

    public MalformedTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
