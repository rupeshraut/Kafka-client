package com.kafka.multidc.deadletter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for handling failed messages and routing them to dead letter queues.
 * Provides comprehensive error management and retry patterns.
 */
public interface DeadLetterQueueHandler {

    /**
     * Configuration for dead letter queue handling.
     */
    interface DeadLetterConfig {
        String getDeadLetterTopicSuffix();
        int getMaxRetryAttempts();
        boolean isEnabled();
        DeadLetterStrategy getStrategy();
    }

    /**
     * Strategy for handling dead letter messages.
     */
    enum DeadLetterStrategy {
        /** Send to a dedicated dead letter topic */
        TOPIC_BASED,
        /** Log the error and discard the message */
        LOG_AND_DISCARD,
        /** Store in an external system (database, file, etc.) */
        EXTERNAL_STORE,
        /** Custom handler implementation */
        CUSTOM
    }

    /**
     * Context information for dead letter processing.
     */
    interface DeadLetterContext {
        ConsumerRecord<?, ?> getOriginalRecord();
        Exception getFailureReason();
        int getRetryAttempt();
        String getDatacenterId();
        long getFirstFailureTimestamp();
        long getLastFailureTimestamp();
    }

    /**
     * Handle a failed message synchronously.
     *
     * @param context the dead letter context containing failure information
     * @return true if handling was successful, false otherwise
     */
    boolean handleFailedMessage(DeadLetterContext context);

    /**
     * Handle a failed message asynchronously.
     *
     * @param context the dead letter context containing failure information
     * @return future that completes when handling is done
     */
    CompletableFuture<Boolean> handleFailedMessageAsync(DeadLetterContext context);

    /**
     * Handle a failed message reactively.
     *
     * @param context the dead letter context containing failure information
     * @return mono that completes when handling is done
     */
    Mono<Boolean> handleFailedMessageReactive(DeadLetterContext context);

    /**
     * Check if a message should be retried or sent to dead letter queue.
     *
     * @param context the dead letter context containing failure information
     * @return true if the message should be retried, false if it should go to DLQ
     */
    boolean shouldRetry(DeadLetterContext context);

    /**
     * Create a dead letter topic name for the given original topic.
     *
     * @param originalTopic the original topic name
     * @return the dead letter topic name
     */
    String createDeadLetterTopicName(String originalTopic);

    /**
     * Get the current configuration.
     *
     * @return the dead letter configuration
     */
    DeadLetterConfig getConfiguration();

    /**
     * Update the configuration at runtime.
     *
     * @param config the new configuration
     */
    void updateConfiguration(DeadLetterConfig config);

    /**
     * Get metrics about dead letter queue operations.
     *
     * @return dead letter metrics
     */
    DeadLetterMetrics getMetrics();

    /**
     * Metrics for dead letter queue operations.
     */
    interface DeadLetterMetrics {
        long getTotalFailedMessages();
        long getTotalRetriedMessages();
        long getTotalDeadLetterMessages();
        double getFailureRate();
        double getRetrySuccessRate();
    }
}
