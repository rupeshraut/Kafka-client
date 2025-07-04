package com.kafka.multidc.tracing;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Real-world example demonstrating message tracing and correlation across multiple datacenters.
 * This is essential for debugging, monitoring, and maintaining data lineage in production systems.
 */
public class MessageTracingExample {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageTracingExample.class);
    
    // Standard headers for message tracing
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String TRACE_ID_HEADER = "X-Trace-ID";
    private static final String SPAN_ID_HEADER = "X-Span-ID";
    private static final String SOURCE_DATACENTER_HEADER = "X-Source-Datacenter";
    private static final String TIMESTAMP_HEADER = "X-Message-Timestamp";
    private static final String HOP_COUNT_HEADER = "X-Hop-Count";
    
    public static void main(String[] args) {
        // Configure client with tracing capabilities
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .bootstrapServers("localhost:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .bootstrapServers("localhost:9093")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)
            .healthCheckInterval(Duration.ofSeconds(30))
            .enableMetrics(true)
            .build();

        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            logger.info("🔍 Starting Message Tracing and Correlation Examples");
            
            // Demonstrate different tracing scenarios
            simpleMessageTracing(client);
            correlatedMessageFlow(client);
            crossDatacenterTracing(client);
            asyncTracingWithCallbacks(client);
            distributedTransactionTracing(client);
            
            logger.info("✅ All message tracing examples completed successfully!");
            
        } catch (Exception e) {
            logger.error("❌ Error running message tracing examples", e);
        }
    }
    
    /**
     * Basic message tracing with correlation IDs
     */
    private static void simpleMessageTracing(KafkaMultiDatacenterClient client) {
        logger.info("=== 🏷️ Simple Message Tracing ===");
        
        try {
            String correlationId = generateCorrelationId();
            String traceId = generateTraceId();
            
            // Set up logging context
            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            
            logger.info("📤 Sending traced message with correlation ID: {}", correlationId);
            
            // Create message with tracing headers
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "traced-events", 
                "user-123", 
                "User login event"
            );
            
            // Add tracing headers
            addTracingHeaders(record.headers(), correlationId, traceId, "us-east-1", 0);
            
            RecordMetadata metadata = client.producerSync().send(record);
            
            logger.info("✅ Message sent successfully - Partition: {}, Offset: {}, CorrelationId: {}", 
                       metadata.partition(), metadata.offset(), correlationId);
            
        } catch (Exception e) {
            logger.error("❌ Simple message tracing failed", e);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Demonstrate correlated message flow across multiple topics
     */
    private static void correlatedMessageFlow(KafkaMultiDatacenterClient client) {
        logger.info("=== 🔗 Correlated Message Flow ===");
        
        try {
            String correlationId = generateCorrelationId();
            String traceId = generateTraceId();
            
            // Set up logging context
            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            
            logger.info("🚀 Starting correlated flow with correlation ID: {}", correlationId);
            
            // Step 1: User action event
            sendTracedMessage(client, "user-actions", "user-456", 
                "User clicked purchase button", correlationId, traceId, 1);
            
            // Step 2: Order created event (same correlation chain)
            sendTracedMessage(client, "order-events", "order-789", 
                "Order created for user-456", correlationId, traceId, 2);
            
            // Step 3: Payment processing event (same correlation chain)
            sendTracedMessage(client, "payment-events", "payment-101", 
                "Payment processing started", correlationId, traceId, 3);
            
            // Step 4: Inventory check event (same correlation chain)
            sendTracedMessage(client, "inventory-events", "product-202", 
                "Inventory check for purchase", correlationId, traceId, 4);
            
            logger.info("✅ Correlated message flow completed - 4 messages sent with correlation ID: {}", correlationId);
            
        } catch (Exception e) {
            logger.error("❌ Correlated message flow failed", e);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Cross-datacenter tracing with hop counting
     */
    private static void crossDatacenterTracing(KafkaMultiDatacenterClient client) {
        logger.info("=== 🌐 Cross-Datacenter Tracing ===");
        
        try {
            String correlationId = generateCorrelationId();
            String traceId = generateTraceId();
            
            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            
            logger.info("🌍 Sending message across datacenters with correlation ID: {}", correlationId);
            
            // Simulate message moving through different datacenters
            for (String datacenter : List.of("us-east-1", "us-west-1")) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "cross-dc-events", 
                    "global-event-" + System.currentTimeMillis(), 
                    String.format("Event processed in datacenter: %s", datacenter)
                );
                
                // Add datacenter-specific tracing
                addTracingHeaders(record.headers(), correlationId, traceId, datacenter, 
                    datacenter.equals("us-east-1") ? 1 : 2);
                
                RecordMetadata metadata = client.producerSync().send(record);
                
                logger.info("📍 Message sent from datacenter: {} - Partition: {}, Offset: {}", 
                           datacenter, metadata.partition(), metadata.offset());
                
                // Simulate processing time
                Thread.sleep(100);
            }
            
            logger.info("✅ Cross-datacenter tracing completed for correlation ID: {}", correlationId);
            
        } catch (Exception e) {
            logger.error("❌ Cross-datacenter tracing failed", e);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Asynchronous tracing with callbacks for monitoring
     */
    private static void asyncTracingWithCallbacks(KafkaMultiDatacenterClient client) {
        logger.info("=== ⚡ Async Tracing with Callbacks ===");
        
        try {
            String correlationId = generateCorrelationId();
            String traceId = generateTraceId();
            
            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            
            logger.info("🔄 Starting async tracing with correlation ID: {}", correlationId);
            
            CompletableFuture<RecordMetadata> future = sendTracedMessageAsync(
                client, "async-traced-events", "async-key-" + System.currentTimeMillis(),
                "Async traced message", correlationId, traceId, 1
            );
            
            // Add tracing callbacks
            future.thenAccept(metadata -> {
                MDC.put("correlationId", correlationId);
                MDC.put("traceId", traceId);
                logger.info("✅ Async message delivered successfully - Partition: {}, Offset: {}, CorrelationId: {}", 
                           metadata.partition(), metadata.offset(), correlationId);
                MDC.clear();
            }).exceptionally(throwable -> {
                MDC.put("correlationId", correlationId);
                MDC.put("traceId", traceId);
                logger.error("❌ Async message delivery failed - CorrelationId: {}", correlationId, throwable);
                MDC.clear();
                return null;
            });
            
            // Wait for completion
            future.join();
            
        } catch (Exception e) {
            logger.error("❌ Async tracing with callbacks failed", e);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Distributed transaction tracing across multiple services
     */
    private static void distributedTransactionTracing(KafkaMultiDatacenterClient client) {
        logger.info("=== 🔄 Distributed Transaction Tracing ===");
        
        try {
            String correlationId = generateCorrelationId();
            String traceId = generateTraceId();
            String transactionId = "txn-" + UUID.randomUUID().toString().substring(0, 8);
            
            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            MDC.put("transactionId", transactionId);
            
            logger.info("💳 Starting distributed transaction with ID: {} and correlation ID: {}", 
                       transactionId, correlationId);
            
            // Transaction start event
            sendTransactionEvent(client, "transaction-events", transactionId, 
                "TRANSACTION_STARTED", correlationId, traceId, 1);
            
            // Service A processing
            sendTransactionEvent(client, "service-a-events", transactionId, 
                "SERVICE_A_PROCESSING", correlationId, traceId, 2);
            
            // Service B processing
            sendTransactionEvent(client, "service-b-events", transactionId, 
                "SERVICE_B_PROCESSING", correlationId, traceId, 3);
            
            // Service C processing
            sendTransactionEvent(client, "service-c-events", transactionId, 
                "SERVICE_C_PROCESSING", correlationId, traceId, 4);
            
            // Transaction completion
            sendTransactionEvent(client, "transaction-events", transactionId, 
                "TRANSACTION_COMPLETED", correlationId, traceId, 5);
            
            logger.info("✅ Distributed transaction completed - TransactionId: {}, CorrelationId: {}", 
                       transactionId, correlationId);
            
        } catch (Exception e) {
            logger.error("❌ Distributed transaction tracing failed", e);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Add comprehensive tracing headers to a message
     */
    private static void addTracingHeaders(Headers headers, String correlationId, String traceId, 
                                        String sourceDatacenter, int hopCount) {
        headers.add(CORRELATION_ID_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        headers.add(TRACE_ID_HEADER, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(SPAN_ID_HEADER, generateSpanId().getBytes(StandardCharsets.UTF_8));
        headers.add(SOURCE_DATACENTER_HEADER, sourceDatacenter.getBytes(StandardCharsets.UTF_8));
        headers.add(TIMESTAMP_HEADER, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        headers.add(HOP_COUNT_HEADER, String.valueOf(hopCount).getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Send a traced message synchronously
     */
    private static void sendTracedMessage(KafkaMultiDatacenterClient client, String topic, 
                                        String key, String value, String correlationId, 
                                        String traceId, int hopCount) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        addTracingHeaders(record.headers(), correlationId, traceId, "us-east-1", hopCount);
        
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("📤 Traced message sent to topic: {} - Key: {}, Partition: {}, Offset: {}", 
                   topic, key, metadata.partition(), metadata.offset());
    }
    
    /**
     * Send a traced message asynchronously
     */
    private static CompletableFuture<RecordMetadata> sendTracedMessageAsync(
            KafkaMultiDatacenterClient client, String topic, String key, String value, 
            String correlationId, String traceId, int hopCount) {
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        addTracingHeaders(record.headers(), correlationId, traceId, "us-east-1", hopCount);
        
        return client.producerAsync().sendAsync(record);
    }
    
    /**
     * Send a transaction event with additional transaction metadata
     */
    private static void sendTransactionEvent(KafkaMultiDatacenterClient client, String topic, 
                                           String transactionId, String eventType, 
                                           String correlationId, String traceId, int step) throws Exception {
        
        String eventData = String.format("{\"transactionId\":\"%s\",\"eventType\":\"%s\",\"step\":%d,\"timestamp\":\"%s\"}", 
                                       transactionId, eventType, step, Instant.now().toString());
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, transactionId, eventData);
        addTracingHeaders(record.headers(), correlationId, traceId, "us-east-1", step);
        
        // Add transaction-specific headers
        record.headers().add("X-Transaction-ID", transactionId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Event-Type", eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Transaction-Step", String.valueOf(step).getBytes(StandardCharsets.UTF_8));
        
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("🔄 Transaction event sent - Type: {}, Step: {}, Partition: {}, Offset: {}", 
                   eventType, step, metadata.partition(), metadata.offset());
    }
    
    /**
     * Generate a unique correlation ID
     */
    private static String generateCorrelationId() {
        return "corr-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Generate a unique trace ID
     */
    private static String generateTraceId() {
        return "trace-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Generate a unique span ID
     */
    private static String generateSpanId() {
        return "span-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Extract and log tracing information from consumed message
     */
    public static void logTracingInfo(ConsumerRecord<?, ?> record) {
        Headers headers = record.headers();
        
        String correlationId = extractHeader(headers, CORRELATION_ID_HEADER);
        String traceId = extractHeader(headers, TRACE_ID_HEADER);
        String spanId = extractHeader(headers, SPAN_ID_HEADER);
        String sourceDatacenter = extractHeader(headers, SOURCE_DATACENTER_HEADER);
        String timestamp = extractHeader(headers, TIMESTAMP_HEADER);
        String hopCount = extractHeader(headers, HOP_COUNT_HEADER);
        
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        if (traceId != null) {
            MDC.put("traceId", traceId);
        }
        
        logger.info("📥 Received traced message - Topic: {}, Key: {}, CorrelationId: {}, TraceId: {}, " +
                   "SpanId: {}, SourceDC: {}, Timestamp: {}, HopCount: {}", 
                   record.topic(), record.key(), correlationId, traceId, spanId, 
                   sourceDatacenter, timestamp, hopCount);
    }
    
    /**
     * Extract header value as string
     */
    private static String extractHeader(Headers headers, String headerName) {
        var header = headers.lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}
