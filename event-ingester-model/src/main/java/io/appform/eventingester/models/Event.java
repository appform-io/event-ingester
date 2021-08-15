package io.appform.eventingester.models;

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import java.util.Date;

/**
 * Core event object
 * @author shashank.g
 */
@Data
public class Event {

    @NotEmpty
    private String app;

    @NotEmpty
    private String eventType;

    @NotEmpty
    private String id;

    @NotEmpty
    private String groupingKey;
    private String partitionKey;
    private String eventSchemaVersion;

    private Date time = new Date();

    private String topic;

    private Object eventData;
}
