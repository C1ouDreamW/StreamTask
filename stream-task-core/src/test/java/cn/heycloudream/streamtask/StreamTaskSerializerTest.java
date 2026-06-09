package cn.heycloudream.streamtask;

import cn.heycloudream.streamtask.model.StreamTaskEnvelope;
import cn.heycloudream.streamtask.support.StreamTaskSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamTaskSerializerTest {
    private final StreamTaskSerializer serializer = new StreamTaskSerializer(new ObjectMapper());

    @Test
    void roundTripsEnvelopeMap() {
        StreamTaskEnvelope task = StreamTaskEnvelope.create("demo.success", "biz-1", "{\"a\":1}");
        Map<Object, Object> raw = new LinkedHashMap<>(serializer.toMap(task));

        StreamTaskEnvelope decoded = serializer.fromMap(raw);

        assertThat(decoded.taskType()).isEqualTo("demo.success");
        assertThat(decoded.businessKey()).isEqualTo("biz-1");
        assertThat(decoded.payload()).isEqualTo("{\"a\":1}");
        assertThat(decoded.schemaVersion()).isEqualTo(1);
    }
}
