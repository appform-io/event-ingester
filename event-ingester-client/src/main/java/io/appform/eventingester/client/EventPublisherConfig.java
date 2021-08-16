package io.appform.eventingester.client;

import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;

/**
 *
 */
@Data
public class EventPublisherConfig {
    @NotEmpty
    @Valid
    private String server;

    private String queuePath;

    @Min(10L)
    @Max(1024L)
    private int connections = 10;

    @Max(86400L)
    private int idleTimeOutSeconds = 30;

    @Max(86400000L)
    private int connectTimeoutMs = 10000;

    @Max(86400000L)
    private int opTimeoutMs = 10000;

    private boolean disabled;

}
