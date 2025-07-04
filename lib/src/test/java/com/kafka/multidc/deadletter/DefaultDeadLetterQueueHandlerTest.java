package com.kafka.multidc.deadletter;

import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import com.kafka.multidc.routing.DatacenterRouter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DefaultDeadLetterQueueHandler.
 */
@ExtendWith(MockitoExtension.class)
class DefaultDeadLetterQueueHandlerTest {

    @Mock
    private KafkaConnectionPoolManager poolManager;

    @Mock
    private DatacenterRouter router;

    @Mock
    private KafkaProducer<Object, Object> producer;

    @Mock
    private Future<RecordMetadata> future;

    @Mock
    private RecordMetadata recordMetadata;

    private SimpleMeterRegistry meterRegistry;
    private DefaultDeadLetterQueueHandler handler;
    private DeadLetterQueueHandler.DeadLetterConfig config;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        config = DefaultDeadLetterConfig.builder()
            .deadLetterTopicSuffix(".dlq")
            .maxRetryAttempts(3)
            .enabled(true)
            .strategy(DeadLetterQueueHandler.DeadLetterStrategy.TOPIC_BASED)
            .build();

        handler = new DefaultDeadLetterQueueHandler(poolManager, router, meterRegistry, config);
    }

    @Test
    void testShouldRetryWhenUnderMaxAttempts() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(2)
            .datacenterId("dc1")
            .build();

        assertTrue(handler.shouldRetry(context));
    }

    @Test
    void testShouldNotRetryWhenMaxAttemptsReached() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(3)
            .datacenterId("dc1")
            .build();

        assertFalse(handler.shouldRetry(context));
    }

    @Test
    void testCreateDeadLetterTopicName() {
        String originalTopic = "my-topic";
        String expectedDlqTopic = "my-topic.dlq";

        assertEquals(expectedDlqTopic, handler.createDeadLetterTopicName(originalTopic));
    }

    @Test
    void testHandleFailedMessageWithRetry() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(1)
            .datacenterId("dc1")
            .build();

        boolean result = handler.handleFailedMessage(context);

        assertTrue(result); // Should retry
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.failed.messages").count());
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.retried.messages").count());
    }

    @Test
    void testHandleFailedMessageSendToDeadLetterQueue() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(3)
            .datacenterId("dc1")
            .build();

        when(poolManager.getProducer("dc1")).thenReturn(producer);
        when(producer.send(any())).thenReturn(future);
        when(future.get()).thenReturn(recordMetadata);

        boolean result = handler.handleFailedMessage(context);

        assertTrue(result);
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.failed.messages").count());
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.sent.messages").count());

        verify(producer).send(argThat(producerRecord -> 
            producerRecord.topic().equals("test-topic.dlq") &&
            producerRecord.key().equals("key") &&
            producerRecord.value().equals("value")
        ));
    }

    @Test
    void testHandleFailedMessageLogAndDiscardStrategy() {
        DeadLetterQueueHandler.DeadLetterConfig logDiscardConfig = DefaultDeadLetterConfig.builder()
            .strategy(DeadLetterQueueHandler.DeadLetterStrategy.LOG_AND_DISCARD)
            .maxRetryAttempts(3)
            .build();

        handler.updateConfiguration(logDiscardConfig);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(3)
            .datacenterId("dc1")
            .build();

        boolean result = handler.handleFailedMessage(context);

        assertTrue(result);
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.failed.messages").count());
        assertEquals(1.0, meterRegistry.counter("kafka.deadletter.sent.messages").count());
        
        // Verify no producer interaction for log and discard
        verifyNoInteractions(poolManager);
    }

    @Test
    void testGetMetrics() {
        DeadLetterQueueHandler.DeadLetterMetrics metrics = handler.getMetrics();

        assertNotNull(metrics);
        assertEquals(0, metrics.getTotalFailedMessages());
        assertEquals(0, metrics.getTotalRetriedMessages());
        assertEquals(0, metrics.getTotalDeadLetterMessages());
        assertEquals(0.0, metrics.getFailureRate());
        assertEquals(1.0, metrics.getRetrySuccessRate());
    }

    @Test
    void testHandleFailedMessageAsync() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(1)
            .datacenterId("dc1")
            .build();

        assertDoesNotThrow(() -> {
            Boolean result = handler.handleFailedMessageAsync(context).get();
            assertTrue(result);
        });
    }

    @Test
    void testHandleFailedMessageReactive() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        DeadLetterQueueHandler.DeadLetterContext context = DefaultDeadLetterContext.builder()
            .originalRecord(record)
            .failureReason(new RuntimeException("Test error"))
            .retryAttempt(1)
            .datacenterId("dc1")
            .build();

        Boolean result = handler.handleFailedMessageReactive(context).block();
        assertTrue(result);
    }

    @Test
    void testConfigurationUpdate() {
        DeadLetterQueueHandler.DeadLetterConfig newConfig = DefaultDeadLetterConfig.builder()
            .deadLetterTopicSuffix(".error")
            .maxRetryAttempts(5)
            .build();

        handler.updateConfiguration(newConfig);

        assertEquals(newConfig, handler.getConfiguration());
        assertEquals(".error", handler.getConfiguration().getDeadLetterTopicSuffix());
        assertEquals(5, handler.getConfiguration().getMaxRetryAttempts());
    }
}
