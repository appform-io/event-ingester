package io.appform.eventingester.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotEmpty;
import java.util.Date;

/**
 * Core event object
 * @author shashank.g
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Event {

    @NotEmpty
    private String app;

    @NotEmpty
    private String eventType;

    private String id;
    private Object eventData;

    private String topic;
    private String groupingKey;
    private String partitionKey;
    private String eventSchemaVersion;

    private Date time;


}
