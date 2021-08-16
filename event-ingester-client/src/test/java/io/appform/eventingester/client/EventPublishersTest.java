package io.appform.eventingester.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.appform.eventingester.models.Event;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 *
 */
class EventPublishersTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WireMockServer server = new WireMockServer(new WireMockConfiguration().dynamicPort());

    @BeforeEach
    void setup() {
        server.start();
    }

    @AfterEach
    void destroy() {
        server.stop();
        server.resetAll();
    }

    @Test
    @SneakyThrows
    void testSender() {
        server.stubFor(post(urlEqualTo("/events/v1/bulk"))
                               .willReturn(aResponse()
                                                   .withStatus(HttpStatus.SC_OK)
                                                   .withJsonBody(MAPPER.createObjectNode().put("status", true))));

        val event = Event.builder()
                .topic("test")
                .app("testapp")
                .eventType("TEST_EVENT")
                .eventData(Collections.singletonMap("name", "santanu"))
                .build();

        val config = new EventPublisherConfig();
        config.setServer(server.baseUrl());
        config.setQueuePath("/tmp/testqueue");

        try (val publisher = EventPublishers.create(config, MAPPER)) {
            IntStream.rangeClosed(1, 321)
                    .forEach(i -> publisher.publish(Event.builder()
                                                            .topic("test")
                                                            .app("testapp")
                                                            .eventType("TEST_EVENT")
                                                            .eventData(Collections.singletonMap("messageCount", i))
                                                            .build()));
        }
        catch (Throwable t) {
            Assertions.fail("Should not have encountered exception");
        }
    }

}