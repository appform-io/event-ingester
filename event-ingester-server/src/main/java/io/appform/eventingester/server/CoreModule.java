package io.appform.eventingester.server;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.appform.eventingester.server.kafka.KafkaConfig;
import io.appform.eventingester.server.kafka.KafkaEventSink;

import javax.inject.Singleton;

/**
 *
 */

public class CoreModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EventSink.class).to(KafkaEventSink.class);
    }

    @Provides
    @Singleton
    public KafkaConfig kafkaConfig(final AppConfig appConfig) {
        return appConfig.getKafka();
    }
}
