package com.kafka.multidc.deadletter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Dead Letter Queue functionality.
 * Fast running tests without external dependencies.
 */
class DeadLetterQueueUnitTest {
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testDeadLetterQueueConfiguration() {
        DeadLetterQueueHandler.DeadLetterConfig config = DefaultDeadLetterConfig.builder()
            .deadLetterTopicSuffix(".error")
            .maxRetryAttempts(5)
            .enabled(true)
            .strategy(DeadLetterQueueHandler.DeadLetterStrategy.LOG_AND_DISCARD)
            .build();
        
        assertEquals(".error", config.getDeadLetterTopicSuffix());
        assertEquals(5, config.getMaxRetryAttempts());
        assertTrue(config.isEnabled());
        assertEquals(DeadLetterQueueHandler.DeadLetterStrategy.LOG_AND_DISCARD, config.getStrategy());
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testDeadLetterContextCreation() {
        ConsumerRecord<String, String> originalRecord = new ConsumerRecord<>(
            "test-topic", 0, 0L, "test-key", "test-value"
        );
        
        Exception failureReason = new RuntimeException("Processing failed");
        
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(originalRecord)
            .failureReason(failureReason)
            .retryAttempt(2)
            .datacenterId("test-dc")
            .build();
        
        assertEquals(originalRecord, context.getOriginalRecord());
        assertEquals(failureReason, context.getFailureReason());
        assertEquals(2, context.getRetryAttempt());
        assertEquals("test-dc", context.getDatacenterId());
        assertTrue(context.getFirstFailureTimestamp() > 0);
        assertTrue(context.getLastFailureTimestamp() > 0);
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testDeadLetterTopicNaming() {
        DefaultDeadLetterQueueHandler handler = createTestHandler();
        
        assertEquals("user-events.dlq", handler.createDeadLetterTopicName("user-events"));
        assertEquals("orders.dlq", handler.createDeadLetterTopicName("orders"));
        assertEquals("notifications.dlq", handler.createDeadLetterTopicName("notifications"));
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testRetryLogic() {
        DefaultDeadLetterQueueHandler handler = createTestHandler();
        
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        
        DeadLetterQueueHandler.DeadLetterContext context1 = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(1)
            .datacenterId("test-dc")
            .build();
        
        assertTrue(handler.shouldRetry(context1));
        
        DeadLetterQueueHandler.DeadLetterContext context2 = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(2)
            .datacenterId("test-dc")
            .build();
        
        assertFalse(handler.shouldRetry(context2));
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testMetricsCollection() {
        DefaultDeadLetterQueueHandler handler = createTestHandler();
        DeadLetterQueueHandler.DeadLetterMetrics metrics = handler.getMetrics();
        
        assertNotNull(metrics);
        assertEquals(0, metrics.getTotalFailedMessages());
        assertEquals(0, metrics.getTotalRetriedMessages());
        assertEquals(0, metrics.getTotalDeadLetterMessages());
        assertEquals(0.0, metrics.getFailureRate());
        assertEquals(1.0, metrics.getRetrySuccessRate());
    }
    
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testAsynchronousOperations() throws Exception {
        DefaultDeadLetterQueueHandler handler = createTestHandler();
        
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(1)
            .datacenterId("test-dc")
            .build();
        
        Boolean asyncResult = handler.handleFailedMessageAsync(context).get(5, TimeUnit.SECONDS);
        assertTrue(asyncResult);
        
        Boolean reactiveResult = handler.handleFailedMessageReactive(context).block(Duration.ofSeconds(5));
        assertTrue(reactiveResult);
    }
    
    private DefaultDeadLetterQueueHandler createTestHandler() {
        DeadLetterQueueHandler.DeadLetterConfig config = DefaultDeadLetterConfig.builder()
            .deadLetterTopicSuffix(".dlq")
            .maxRetryAttempts(2)
            .enabled(true)
            .strategy(DeadLetterQueueHandler.DeadLetterStrategy.LOG_AND_DISCARD)
            .build();
        
        return new DefaultDeadLetterQueueHandler(null, null, 
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), config);
    }
}
