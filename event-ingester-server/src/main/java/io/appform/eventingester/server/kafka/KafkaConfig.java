package io.appform.eventingester.server.kafka;

import lombok.Data;

import javax.validation.constraints.NotEmpty;

/**
 *
 */
@Data
public class KafkaConfig {

    @NotEmpty
    private String bootstrapHosts;

    private String senderId;

    private int batchSize;
}
