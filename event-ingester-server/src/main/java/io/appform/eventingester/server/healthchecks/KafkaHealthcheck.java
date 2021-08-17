package io.appform.eventingester.server.healthchecks;

import io.appform.eventingester.server.kafka.KafkaConfig;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;

import javax.inject.Inject;
import java.util.Properties;

/**
 * Checks kafka
 */
public class KafkaHealthcheck extends NamedHealthCheck {
    private final AdminClient adminClient;

    @Inject
    public KafkaHealthcheck(KafkaConfig config) {
        val properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapHosts());
        this.adminClient = AdminClient.create(properties);
    }

    @Override
    public String getName() {
        return "kafka-check";
    }

    @Override
    protected Result check() throws Exception {
        val response = adminClient.describeCluster();
        return (response.nodes().get().isEmpty()
                || response.clusterId() == null
                || response.controller().get() == null)
               ? Result.unhealthy("Kafka cluster seems to be down")
               : Result.healthy();
    }
}
