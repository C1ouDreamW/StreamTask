package cn.heycloudream.streamtask.demo;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DemoSuccessTaskHandler implements StreamTaskHandler {
    private static final Logger log = LoggerFactory.getLogger(DemoSuccessTaskHandler.class);

    @Override
    public String taskType() {
        return "demo.success";
    }

    @Override
    public void handle(StreamTaskEnvelope task) throws Exception {
        Thread.sleep(1000);
        log.info("[Demo] success handled taskType={} businessKey={}", task.taskType(), task.businessKey());
    }
}
