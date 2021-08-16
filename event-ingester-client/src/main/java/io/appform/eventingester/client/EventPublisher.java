package io.appform.eventingester.client;

import io.appform.eventingester.models.Event;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

/**
 * Generic event publisher interface
 */
public interface EventPublisher extends Closeable {

    default void publish(final Event event) {
        publish(Collections.singletonList(event));
    }

    void publish(final List<Event> events);
}
