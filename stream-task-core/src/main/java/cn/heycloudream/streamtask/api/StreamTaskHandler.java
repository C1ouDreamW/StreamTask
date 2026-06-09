package cn.heycloudream.streamtask.api;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;

public interface StreamTaskHandler {
    String taskType();

    void handle(StreamTaskEnvelope task) throws Exception;
}
