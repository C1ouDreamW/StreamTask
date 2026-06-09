package cn.heycloudream.streamtask.demo;

import cn.heycloudream.streamtask.api.StreamTaskTemplate;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/demo/tasks")
public class DemoTaskController {
    private final StreamTaskTemplate streamTaskTemplate;

    public DemoTaskController(StreamTaskTemplate streamTaskTemplate) {
        this.streamTaskTemplate = streamTaskTemplate;
    }

    @PostMapping("/success")
    public Map<String, Object> success() {
        return publish("demo.success");
    }

    @PostMapping("/fail")
    public Map<String, Object> fail() {
        return publish("demo.fail");
    }

    @PostMapping("/slow")
    public Map<String, Object> slow() {
        return publish("demo.slow");
    }

    private Map<String, Object> publish(String taskType) {
        String businessKey = taskType + "-" + UUID.randomUUID();
        RecordId recordId = streamTaskTemplate.publish(taskType, businessKey, Map.of(
                "businessKey", businessKey,
                "taskType", taskType,
                "createdBy", "stream-task-demo"
        ));
        return Map.of(
                "messageId", recordId.getValue(),
                "taskType", taskType,
                "businessKey", businessKey
        );
    }
}
