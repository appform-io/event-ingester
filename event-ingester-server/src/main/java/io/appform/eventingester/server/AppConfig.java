package io.appform.eventingester.server;

import io.appform.eventingester.server.kafka.KafkaConfig;
import io.dropwizard.Configuration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
public class AppConfig extends Configuration {

    @NotNull
    @Valid
    private KafkaConfig kafka;
}
