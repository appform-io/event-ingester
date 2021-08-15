package io.appform.eventingester.server;

import io.appform.eventingester.models.Event;

import java.util.List;

/**
 *
 */
public interface EventSink {
    boolean send(final List<Event> events);
}
