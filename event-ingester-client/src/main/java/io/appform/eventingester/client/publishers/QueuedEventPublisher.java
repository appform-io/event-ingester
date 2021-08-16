package io.appform.eventingester.client.publishers;

import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.exception.RetriesExhaustedException;
import com.evanlennick.retry4j.exception.UnexpectedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import io.appform.eventingester.client.EventPublisher;
import io.appform.eventingester.client.EventPublisherConfig;
import io.appform.eventingester.models.Event;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
@Slf4j
public class QueuedEventPublisher implements EventPublisher {
    private static final int RETRIES = 5;
    private static final int MAX_PAYLOAD_SIZE = 2000000; //2MB

    private final MessageSenderThread messageSenderThread;
    private final ScheduledExecutorService scheduler;
    private final EventPublisherConfig config;
    private final ObjectMapper mapper;
    private final IBigQueue messageQueue;

    @SneakyThrows
    public QueuedEventPublisher(
            final EventPublisherConfig config,
            final ObjectMapper mapper,
            final SyncEventPublisher transport) {
        this.config = config;
        this.mapper = mapper;
        val path = Objects.requireNonNullElse(config.getQueuePath(), "/tmp/" + UUID.randomUUID());
        val perms = PosixFilePermissions.fromString("rwx------");
        val attr = PosixFilePermissions.asFileAttribute(perms);
        try {
            Files.createDirectories(Paths.get(path), attr);
        }
        catch (final FileAlreadyExistsException e) {
            log.warn("Queue path {} already exists, hence skipping creation", path);
        }
        this.messageQueue = new BigQueueImpl(path, "event-ingester");
        this.messageSenderThread = new MessageSenderThread(this,
                                                           transport,
                                                           messageQueue,
                                                           path,
                                                           mapper,
                                                           100);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.scheduler.scheduleWithFixedDelay(messageSenderThread, 0, 1, TimeUnit.SECONDS);
        this.scheduler.scheduleWithFixedDelay(new QueueCleaner(messageQueue, path), 0, 15, TimeUnit.SECONDS);
    }

    @Override
    public void publish(List<Event> events) {
        events.forEach(e -> {
            try {
                this.messageQueue.enqueue(mapper.writeValueAsBytes(e));
            }
            catch (IOException ex) {
                log.error("Error saving event " + e, ex);
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void close() throws IOException {
        log.info("Closing queued sender");
        val retryConfig = new RetryConfigBuilder()
                .withExponentialBackoff()
                .withDelayBetweenTries(1, ChronoUnit.SECONDS)
                .retryOnAnyException()
                .retryOnReturnValue(false)
                .withMaxNumberOfTries(RETRIES)
                .build();
        try {
            new CallExecutorBuilder<Void>()
                    .config(retryConfig)
                    .build()
                    .execute(messageQueue::isEmpty);
        }
        catch (RetriesExhaustedException e) {
            log.error("Retires exhausted, message queue still not empty. Events will be lost.");
        }
        catch (UnexpectedException e) {
            log.error("Unexpected exception: ", e);
        }
        try {
            new CallExecutorBuilder<Void>()
                    .config(retryConfig)
                    .build()
                    .execute(() -> !messageSenderThread.isRunning());
        }
        catch (RetriesExhaustedException e) {
            log.error("Retires exhausted, sender still active. Events might be lost.");
        }
        catch (UnexpectedException e) {
            log.error("Unexpected exception: ", e);
        }
        this.scheduler.shutdownNow();
        this.messageQueue.close();
        deleteDirectoryWalkTree(Path.of(config.getQueuePath()));
        log.info("Queue shutdown completed");
    }


    void deleteDirectoryWalkTree(Path path) throws IOException {
        val visitor = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(path, visitor);
    }

    private static final class MessageSenderThread implements Runnable {
        private final QueuedEventPublisher queuedPublished;
        private final ObjectMapper mapper;
        private final RetryConfig retryConfig;
        private final IBigQueue messageQueue;
        private final int batchSize;
        private final String path;
        private final SyncEventPublisher publisher;
        private final AtomicBoolean running = new AtomicBoolean(false);

        public MessageSenderThread(
                QueuedEventPublisher queuedPublished,
                SyncEventPublisher publisher,
                IBigQueue messageQueue,
                String path,
                ObjectMapper mapper,
                int batchSize) {
            this.queuedPublished = queuedPublished;
            this.messageQueue = messageQueue;
            this.path = path;
            this.mapper = mapper;
            this.batchSize = batchSize;
            this.publisher = publisher;
            this.retryConfig = new RetryConfigBuilder()
                    .withNoWaitBackoff()
                    .withMaxNumberOfTries(RETRIES)
                    .retryOnAnyException()
                    .build();
        }

        @Override
        public void run() {
            running.set(true);
            try {
                while (!messageQueue.isEmpty()) {
                    log.debug("Messages found in queue {}. Sender Invoked.", path);
                    val entries = prepareBatch();
                    if (entries.isEmpty()) {
                        log.trace("No events to send after filtering");
                    }
                    else {
                        if (trySendingEvents(entries)) {
                            log.trace("Events sent successfully");
                        }
                        else {
                            log.warn("Events could not be sent. Giving up for now. Will retry later");
                            break;
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("Error sending messages from queue", e);
            }
            running.set(false);
        }

        private List<Event> prepareBatch() throws IOException {
            val entries = Lists.<Event>newArrayListWithExpectedSize(batchSize);
            var sizeOfPayload = 0;
            var expectedDataSize = 0;
            while (entries.size() < batchSize
                    && expectedDataSize < MAX_PAYLOAD_SIZE) {
                byte[] data = messageQueue.dequeue();
                final int dataLength = null == data
                                       ? 0
                                       : (data.length + 24 + 8);
                if (0 == dataLength || dataLength > MAX_PAYLOAD_SIZE) {
                    log.warn("Ignoring invalid message in queue. Message size: {}", dataLength);
                    break;
                }
                // Check added to keep avoid payload size greater than 2 MB from being pushed in one batch calls
                expectedDataSize = sizeOfPayload + dataLength;
                if (expectedDataSize > MAX_PAYLOAD_SIZE) {
                    log.debug("Current size ({}) + event size({}) exceeded 2MB. Queuing back.",
                              sizeOfPayload, dataLength);
                    messageQueue.enqueue(data);
                }
                else {
                    sizeOfPayload = expectedDataSize;
                    entries.add(mapper.readValue(data, Event.class));
                }
            }
            return entries;
        }

        private boolean trySendingEvents(List<Event> entries) {
            val executorBuilder = new CallExecutorBuilder<Void>();
            executorBuilder.config(retryConfig);
            try {
                executorBuilder.build().execute(() -> {
                    publisher.publish(entries);
                    return null;
                });
            }
            catch (RetriesExhaustedException e) {
                log.error("Error sending events after {} retries. Enqueuing back.", RETRIES);
                queuedPublished.publish(entries);
                return false;
            }
            return true;
        }

        private boolean isRunning() {
            return running.get();
        }
    }

    private static final class QueueCleaner implements Runnable {
        private final IBigQueue messageQueue;
        private final String path;

        private QueueCleaner(IBigQueue messageQueue, String path) {
            this.messageQueue = messageQueue;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                val stopwatch = Stopwatch.createStarted();
                this.messageQueue.gc();
                log.info("GC on queue {} took {} ms", path, stopwatch.elapsed());
            }
            catch (Exception e) {
                log.error("GC on queue " + path + " failed.", e);
            }
        }
    }

}
