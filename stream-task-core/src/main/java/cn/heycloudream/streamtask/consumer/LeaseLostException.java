package cn.heycloudream.streamtask.consumer;

public class LeaseLostException extends RuntimeException {
    public LeaseLostException(String message) {
        super(message);
    }
}
