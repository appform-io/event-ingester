package io.appform.eventingester.server.resources;

import io.appform.eventingester.models.Event;
import io.appform.eventingester.server.EventSink;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

/**
 *
 */
@Slf4j
@Singleton
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/events")
public class Events {

    private final EventSink eventSink;

    @Inject
    public Events(EventSink eventSink) {
        this.eventSink = eventSink;
    }

    @POST
    @Path("/v1")
    public Response ingestEvents(@Valid final List<Event> events) {
        val status = eventSink.send(events);
        log.debug("Event ingestion status: {}", status);
        return Response.ok(Collections.singletonMap("status", status)).build();
    }
}
