package cn.heycloudream.streamtask.demo;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import org.springframework.stereotype.Component;

@Component
public class DemoFailTaskHandler implements StreamTaskHandler {
    @Override
    public String taskType() {
        return "demo.fail";
    }

    @Override
    public void handle(StreamTaskEnvelope task) {
        throw new IllegalStateException("demo fail task always fails");
    }
}
