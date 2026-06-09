package cn.heycloudream.streamtask;

import cn.heycloudream.streamtask.api.StreamTaskHandler;
import cn.heycloudream.streamtask.consumer.StreamTaskHandlerRegistry;
import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamTaskHandlerRegistryTest {
    @Test
    void findsRegisteredHandler() {
        StreamTaskHandler handler = handler("demo.success");
        StreamTaskHandlerRegistry registry = new StreamTaskHandlerRegistry(List.of(handler));

        assertThat(registry.getRequired("demo.success")).isSameAs(handler);
    }

    @Test
    void rejectsDuplicateTaskType() {
        StreamTaskHandler first = handler("demo.success");
        StreamTaskHandler second = handler("demo.success");

        assertThatThrownBy(() -> new StreamTaskHandlerRegistry(List.of(first, second)))
                .isInstanceOf(StreamTaskException.class)
                .hasMessageContaining("Duplicate");
    }

    @Test
    void rejectsUnknownTaskType() {
        StreamTaskHandlerRegistry registry = new StreamTaskHandlerRegistry(List.of());

        assertThatThrownBy(() -> registry.getRequired("missing"))
                .isInstanceOf(StreamTaskException.class)
                .hasMessageContaining("No StreamTaskHandler");
    }

    private static StreamTaskHandler handler(String taskType) {
        return new StreamTaskHandler() {
            @Override
            public String taskType() {
                return taskType;
            }

            @Override
            public void handle(StreamTaskEnvelope task) {
            }
        };
    }
}
