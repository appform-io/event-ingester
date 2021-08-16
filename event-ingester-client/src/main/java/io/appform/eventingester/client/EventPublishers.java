package io.appform.eventingester.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.appform.eventingester.client.publishers.LoggingEventPublisher;
import io.appform.eventingester.client.publishers.QueuedEventPublisher;
import io.appform.eventingester.client.publishers.SyncEventPublisher;
import lombok.experimental.UtilityClass;
import lombok.val;

/**
 * Create publishers as needed
 */
@UtilityClass
public class EventPublishers {
    public static EventPublisher create(final EventPublisherConfig config, final ObjectMapper mapper) {
        if(config.isDisabled()) {
            return new LoggingEventPublisher();
        }
        val syncPublisher = new SyncEventPublisher(config, mapper);
        if(!Strings.isNullOrEmpty(config.getQueuePath())) {
            return new QueuedEventPublisher(config, mapper, syncPublisher);
        }
        return syncPublisher;
    }
}
