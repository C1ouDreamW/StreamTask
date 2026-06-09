package cn.heycloudream.streamtask;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskEnvelopeValidator;
import cn.heycloudream.streamtask.support.StreamTaskException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamTaskEnvelopeValidatorTest {
    private final StreamTaskEnvelopeValidator validator = new StreamTaskEnvelopeValidator(16);

    @Test
    void acceptsValidEnvelope() {
        StreamTaskEnvelope task = StreamTaskEnvelope.create("demo.success", "biz-1", "{\"ok\":true}");

        assertThatCode(() -> validator.validate(task)).doesNotThrowAnyException();
    }

    @Test
    void rejectsBlankBusinessKey() {
        StreamTaskEnvelope task = StreamTaskEnvelope.create("demo.success", " ", "{}");

        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(StreamTaskException.class)
                .hasMessageContaining("businessKey");
    }

    @Test
    void rejectsOversizedPayload() {
        StreamTaskEnvelope task = StreamTaskEnvelope.create("demo.success", "biz-1", "0123456789abcdefg");

        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(StreamTaskException.class)
                .hasMessageContaining("payload exceeds");
    }

    @Test
    void rejectsIllegalTaskType() {
        StreamTaskEnvelope task = StreamTaskEnvelope.create("demo success", "biz-1", "{}");

        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(StreamTaskException.class)
                .hasMessageContaining("taskType");
    }
}
