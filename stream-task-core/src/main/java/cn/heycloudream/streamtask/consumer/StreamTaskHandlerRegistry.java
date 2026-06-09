package cn.heycloudream.streamtask.consumer;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.support.StreamTaskException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamTaskHandlerRegistry {
    private final Map<String, StreamTaskHandler> handlers = new ConcurrentHashMap<>();

    public StreamTaskHandlerRegistry(Collection<StreamTaskHandler> handlers) {
        handlers.forEach(this::register);
    }

    public void register(StreamTaskHandler handler) {
        StreamTaskHandler previous = handlers.putIfAbsent(handler.taskType(), handler);
        if (previous != null) {
            throw new StreamTaskException("Duplicate StreamTaskHandler taskType: " + handler.taskType());
        }
    }

    public StreamTaskHandler getRequired(String taskType) {
        StreamTaskHandler handler = handlers.get(taskType);
        if (handler == null) {
            throw new StreamTaskException("No StreamTaskHandler for taskType: " + taskType);
        }
        return handler;
    }
}
