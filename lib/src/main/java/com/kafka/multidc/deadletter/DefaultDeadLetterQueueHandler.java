package com.kafka.multidc.deadletter;

import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import com.kafka.multidc.routing.DatacenterRouter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the Dead Letter Queue Handler.
 * Provides comprehensive error management and retry patterns with metrics.
 */
public class DefaultDeadLetterQueueHandler implements DeadLetterQueueHandler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDeadLetterQueueHandler.class);
    
    private static final String DLQ_ORIGINAL_TOPIC_HEADER = "dlq.original.topic";
    private static final String DLQ_ORIGINAL_PARTITION_HEADER = "dlq.original.partition";
    private static final String DLQ_ORIGINAL_OFFSET_HEADER = "dlq.original.offset";
    private static final String DLQ_FAILURE_REASON_HEADER = "dlq.failure.reason";
    private static final String DLQ_RETRY_ATTEMPT_HEADER = "dlq.retry.attempt";
    private static final String DLQ_FIRST_FAILURE_TIMESTAMP_HEADER = "dlq.first.failure.timestamp";
    private static final String DLQ_LAST_FAILURE_TIMESTAMP_HEADER = "dlq.last.failure.timestamp";
    private static final String DLQ_DATACENTER_ID_HEADER = "dlq.datacenter.id";

    private final KafkaConnectionPoolManager poolManager;
    @SuppressWarnings("unused") // May be used in future routing optimizations
    private final DatacenterRouter router;
    @SuppressWarnings("unused") // Used for metrics registration
    private final MeterRegistry meterRegistry;
    
    // Configuration
    private volatile DeadLetterConfig configuration;
    
    // Metrics
    private final Counter failedMessagesCounter;
    private final Counter retriedMessagesCounter;
    private final Counter deadLetterMessagesCounter;
    private final Timer deadLetterProcessingTimer;
    
    // Internal metrics
    private final AtomicLong totalFailedMessages = new AtomicLong(0);
    private final AtomicLong totalRetriedMessages = new AtomicLong(0);
    private final AtomicLong totalDeadLetterMessages = new AtomicLong(0);

    public DefaultDeadLetterQueueHandler(KafkaConnectionPoolManager poolManager,
                                       DatacenterRouter router,
                                       MeterRegistry meterRegistry,
                                       DeadLetterConfig configuration) {
        this.poolManager = poolManager;
        this.router = router;
        this.meterRegistry = meterRegistry;
        this.configuration = configuration;
        
        // Initialize metrics
        this.failedMessagesCounter = Counter.builder("kafka.deadletter.failed.messages")
            .description("Total number of failed messages")
            .register(meterRegistry);
        this.retriedMessagesCounter = Counter.builder("kafka.deadletter.retried.messages")
            .description("Total number of retried messages")
            .register(meterRegistry);
        this.deadLetterMessagesCounter = Counter.builder("kafka.deadletter.sent.messages")
            .description("Total number of messages sent to dead letter queue")
            .register(meterRegistry);
        this.deadLetterProcessingTimer = Timer.builder("kafka.deadletter.processing.time")
            .description("Time taken to process dead letter messages")
            .register(meterRegistry);
    }

    @Override
    public boolean handleFailedMessage(DeadLetterContext context) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            failedMessagesCounter.increment();
            totalFailedMessages.incrementAndGet();
            
            if (shouldRetry(context)) {
                logger.debug("Retrying message from topic: {}, attempt: {}", 
                    context.getOriginalRecord().topic(), context.getRetryAttempt());
                retriedMessagesCounter.increment();
                totalRetriedMessages.incrementAndGet();
                return true; // Indicate retry
            }
            
            return sendToDeadLetterQueue(context);
            
        } catch (Exception e) {
            logger.error("Error handling failed message", e);
            return false;
        } finally {
            sample.stop(deadLetterProcessingTimer);
        }
    }

    @Override
    public CompletableFuture<Boolean> handleFailedMessageAsync(DeadLetterContext context) {
        return CompletableFuture.supplyAsync(() -> handleFailedMessage(context));
    }

    @Override
    public Mono<Boolean> handleFailedMessageReactive(DeadLetterContext context) {
        return Mono.fromCallable(() -> handleFailedMessage(context));
    }

    @Override
    public boolean shouldRetry(DeadLetterContext context) {
        if (!configuration.isEnabled()) {
            return false;
        }
        
        return context.getRetryAttempt() < configuration.getMaxRetryAttempts();
    }

    @Override
    public String createDeadLetterTopicName(String originalTopic) {
        return originalTopic + configuration.getDeadLetterTopicSuffix();
    }

    @Override
    public DeadLetterConfig getConfiguration() {
        return configuration;
    }

    @Override
    public void updateConfiguration(DeadLetterConfig config) {
        logger.info("Updating dead letter queue configuration");
        this.configuration = config;
    }

    @Override
    public DeadLetterMetrics getMetrics() {
        return new DefaultDeadLetterMetrics();
    }

    private boolean sendToDeadLetterQueue(DeadLetterContext context) {
        switch (configuration.getStrategy()) {
            case TOPIC_BASED:
                return sendToDeadLetterTopic(context);
            case LOG_AND_DISCARD:
                return logAndDiscard(context);
            case EXTERNAL_STORE:
                return sendToExternalStore(context);
            case CUSTOM:
                return handleCustomStrategy(context);
            default:
                logger.warn("Unknown dead letter strategy: {}", configuration.getStrategy());
                return logAndDiscard(context);
        }
    }

    private boolean sendToDeadLetterTopic(DeadLetterContext context) {
        try {
            ConsumerRecord<?, ?> originalRecord = context.getOriginalRecord();
            String deadLetterTopic = createDeadLetterTopicName(originalRecord.topic());
            
            // Create headers with failure information
            Headers headers = new RecordHeaders();
            headers.add(DLQ_ORIGINAL_TOPIC_HEADER, originalRecord.topic().getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_ORIGINAL_PARTITION_HEADER, String.valueOf(originalRecord.partition()).getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_ORIGINAL_OFFSET_HEADER, String.valueOf(originalRecord.offset()).getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_FAILURE_REASON_HEADER, context.getFailureReason().getMessage().getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_RETRY_ATTEMPT_HEADER, String.valueOf(context.getRetryAttempt()).getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_FIRST_FAILURE_TIMESTAMP_HEADER, String.valueOf(context.getFirstFailureTimestamp()).getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_LAST_FAILURE_TIMESTAMP_HEADER, String.valueOf(context.getLastFailureTimestamp()).getBytes(StandardCharsets.UTF_8));
            headers.add(DLQ_DATACENTER_ID_HEADER, context.getDatacenterId().getBytes(StandardCharsets.UTF_8));
            
            // Copy original headers
            originalRecord.headers().forEach(header -> 
                headers.add("original." + header.key(), header.value()));
            
            // Create dead letter record
            ProducerRecord<Object, Object> deadLetterRecord = new ProducerRecord<>(
                deadLetterTopic,
                null, // Let Kafka assign partition
                originalRecord.key(),
                originalRecord.value(),
                headers
            );
            
            // Send to dead letter topic using the same datacenter
            KafkaProducer<Object, Object> producer = poolManager.getProducer(context.getDatacenterId());
            producer.send(deadLetterRecord).get(); // Synchronous send for reliability
            
            deadLetterMessagesCounter.increment();
            totalDeadLetterMessages.incrementAndGet();
            
            logger.info("Successfully sent message to dead letter queue. Original topic: {}, DLQ topic: {}, Retry attempts: {}", 
                originalRecord.topic(), deadLetterTopic, context.getRetryAttempt());
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to send message to dead letter topic", e);
            // Fallback to log and discard
            return logAndDiscard(context);
        }
    }

    private boolean logAndDiscard(DeadLetterContext context) {
        ConsumerRecord<?, ?> originalRecord = context.getOriginalRecord();
        logger.error("DEAD LETTER: Failed to process message after {} attempts. " +
                    "Topic: {}, Partition: {}, Offset: {}, Key: {}, Datacenter: {}, Error: {}", 
            context.getRetryAttempt(),
            originalRecord.topic(),
            originalRecord.partition(),
            originalRecord.offset(),
            originalRecord.key(),
            context.getDatacenterId(),
            context.getFailureReason().getMessage());
        
        deadLetterMessagesCounter.increment();
        totalDeadLetterMessages.incrementAndGet();
        
        return true; // Always succeed for log and discard
    }

    private boolean sendToExternalStore(DeadLetterContext context) {
        // This would be implemented based on specific external storage requirements
        // For now, fallback to logging
        logger.warn("External store strategy not implemented, falling back to log and discard");
        return logAndDiscard(context);
    }

    private boolean handleCustomStrategy(DeadLetterContext context) {
        // This would be implemented by extending this class or providing a custom handler
        logger.warn("Custom strategy not implemented, falling back to log and discard");
        return logAndDiscard(context);
    }

    /**
     * Default implementation of DeadLetterMetrics.
     */
    private class DefaultDeadLetterMetrics implements DeadLetterMetrics {
        
        @Override
        public long getTotalFailedMessages() {
            return totalFailedMessages.get();
        }

        @Override
        public long getTotalRetriedMessages() {
            return totalRetriedMessages.get();
        }

        @Override
        public long getTotalDeadLetterMessages() {
            return totalDeadLetterMessages.get();
        }

        @Override
        public double getFailureRate() {
            long total = getTotalFailedMessages();
            return total > 0 ? (double) getTotalDeadLetterMessages() / total : 0.0;
        }

        @Override
        public double getRetrySuccessRate() {
            long totalRetries = getTotalRetriedMessages();
            return totalRetries > 0 ? 1.0 - ((double) getTotalDeadLetterMessages() / totalRetries) : 1.0;
        }
    }
}
