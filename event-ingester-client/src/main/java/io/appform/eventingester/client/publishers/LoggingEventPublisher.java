package io.appform.eventingester.client.publishers;

import io.appform.eventingester.client.EventPublisher;
import io.appform.eventingester.models.Event;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 *
 */
@Slf4j
public class LoggingEventPublisher implements EventPublisher {
    @Override
    public void publish(List<Event> events) {
        log.info("Events to be sent: {}", events);
    }

    @Override
    public void close() throws IOException {
        //Nothing to do here
    }
}
