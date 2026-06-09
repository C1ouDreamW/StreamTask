package cn.heycloudream.streamtask.admin;

import cn.heycloudream.streamtask.dlq.DeadLetterReplayService;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/stream-task/admin")
public class StreamTaskAdminController {
    private final StreamTaskOverviewService overviewService;
    private final DeadLetterReplayService replayService;

    public StreamTaskAdminController(StreamTaskOverviewService overviewService, DeadLetterReplayService replayService) {
        this.overviewService = overviewService;
        this.replayService = replayService;
    }

    @GetMapping("/overview")
    public Object overview() {
        return overviewService.overview();
    }

    @GetMapping("/pending")
    public Object pending() {
        return overviewService.pending();
    }

    @GetMapping("/dlq")
    public Object dlq() {
        return overviewService.dlq();
    }

    @PostMapping("/dlq/{messageId}/replay")
    public Object replay(@PathVariable String messageId) {
        RecordId recordId = replayService.replay(messageId);
        return Map.of("messageId", recordId.getValue());
    }
}
