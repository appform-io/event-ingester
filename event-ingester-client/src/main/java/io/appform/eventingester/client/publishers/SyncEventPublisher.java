package io.appform.eventingester.client.publishers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.eventingester.client.EventPublisher;
import io.appform.eventingester.client.EventPublisherConfig;
import io.appform.eventingester.client.EventSendingException;
import io.appform.eventingester.models.Event;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 *
 */
@Slf4j
public class SyncEventPublisher implements EventPublisher {
    private final EventPublisherConfig publisherConfig;
    private final ObjectMapper mapper;
    private final CloseableHttpClient client;

    public SyncEventPublisher(EventPublisherConfig publisherConfig, ObjectMapper mapper) {
        this.publisherConfig = publisherConfig;
        this.mapper = mapper;
        val connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(publisherConfig.getConnections());
        client = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(RequestConfig.copy(RequestConfig.DEFAULT)
                                                 .setConnectionRequestTimeout(publisherConfig.getConnectTimeoutMs())
                                                 .setConnectTimeout(publisherConfig.getConnectTimeoutMs())
                                                 .setSocketTimeout(publisherConfig.getOpTimeoutMs())
                                                 .build())
                .build();
    }

    @Override
    public void publish(List<Event> events) {
        val uri = URI.create(publisherConfig.getServer() + "/events/v1/bulk");
        val post = new HttpPost(uri);
        try {
            post.setEntity(new ByteArrayEntity(mapper.writeValueAsBytes(events)));
        }
        catch (JsonProcessingException e) {
            throw new EventSendingException("Error converting events to send to remote: " + uri, e);
        }
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        try(val response = client.execute(post)) {
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                val responseData = mapper.readTree(EntityUtils.toByteArray(response.getEntity()));
                if(responseData.has("status") && responseData.get("status").asBoolean()) {
                    log.debug("Published {} events to {}", events.size(), uri);
                }
                else {
                    log.debug("Events endpoint returned non successful status. Please retry sending");
                    throw new EventSendingException("Error status from remote: " + uri);
                }
            }
        }
        catch (IOException e) {
            throw new EventSendingException("Error sending events to remote: " + uri, e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
