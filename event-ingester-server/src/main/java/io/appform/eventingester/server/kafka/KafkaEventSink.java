package io.appform.eventingester.server.kafka;

import io.appform.eventingester.models.Event;
import io.appform.eventingester.server.EventSink;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 *
 */
@Slf4j
@Singleton
public class KafkaEventSink implements EventSink {
    private final KafkaProducer<String, Event> producer;

    @Inject
    public KafkaEventSink(final KafkaConfig kafkaConfig) {
        val config = new Properties();
        config.put(ProducerConfig.CLIENT_ID_CONFIG,
                   Objects.requireNonNullElse(kafkaConfig.getSenderId(), System.getenv("HOSTNAME")));
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapHosts());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG,
                   kafkaConfig.getBatchSize() != 0
                   ? kafkaConfig.getBatchSize()
                   : 1000);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class.getName());
        producer = new KafkaProducer<>(config);
    }

    @Override
    @SneakyThrows
    public boolean send(List<Event> events) {
        val groupedEvents = events.stream()
                .collect(Collectors.groupingBy(Event::getTopic));
        log.debug("Events to be sent to topics: {}", groupedEvents.keySet());
        val futures = groupedEvents.entrySet()
                .stream()
                .flatMap(groupedEvent -> groupedEvent.getValue()
                        .stream()
                        .map(event -> new ProducerRecord<>(
                                groupedEvent.getKey(),
                                Objects.requireNonNullElse(event.getPartitionKey(), UUID.randomUUID().toString()),
                                event)))
                .map(producer::send)
                .collect(Collectors.toUnmodifiableList());
        val pushed = (Long) futures.stream()
                .map(future -> {
                    try {
                        return future.get(2, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (TimeoutException | ExecutionException e) {
                        log.error("Error pushing message");
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .count();
        log.debug("Pushed {} events", pushed);
        return pushed == events.size();
    }
}
