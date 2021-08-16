package io.appform.eventingester.client;

/**
 *
 */
public class EventSendingException extends RuntimeException {
    public EventSendingException(String message) {
        super(message);
    }

    public EventSendingException(String message, Throwable cause) {
        super(message, cause);
    }
}
