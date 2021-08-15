package io.appform.eventingester.server.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.eventingester.models.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import static io.appform.eventingester.server.Utils.configureMapper;

/**
 *
 */
@Slf4j
public class EventSerializer implements Serializer<Event> {
    private final ObjectMapper mapper = new ObjectMapper();

    public EventSerializer() {
        configureMapper(mapper);
    }

    @Override
    public byte[] serialize(String topic, Event data) {
        try {
            return mapper.writeValueAsBytes(data);
        }
        catch (JsonProcessingException e) {
            log.error("Error serializing " + data + ": ", e);
        }
        return new byte[0];
    }
}
