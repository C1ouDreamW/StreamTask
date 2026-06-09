package cn.heycloudream.streamtask.demo;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DemoSlowTaskHandler implements StreamTaskHandler {
    private static final Logger log = LoggerFactory.getLogger(DemoSlowTaskHandler.class);

    @Override
    public String taskType() {
        return "demo.slow";
    }

    @Override
    public void handle(StreamTaskEnvelope task) throws Exception {
        Thread.sleep(90_000);
        log.info("[Demo] slow handled taskType={} businessKey={}", task.taskType(), task.businessKey());
    }
}
