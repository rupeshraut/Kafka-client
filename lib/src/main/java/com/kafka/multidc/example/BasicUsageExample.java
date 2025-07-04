package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Basic usage examples for the Kafka Multi-Datacenter Client.
 * Demonstrates simple producer and consumer operations across multiple datacenters.
 */
public class BasicUsageExample {
    
    private static final Logger logger = LoggerFactory.getLogger(BasicUsageExample.class);
    
    // Health monitoring and metrics
    private static final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private static final Counter successfulMessages = Counter.builder("kafka.messages.success")
        .description("Number of successfully sent messages")
        .register(meterRegistry);
    private static final Counter failedMessages = Counter.builder("kafka.messages.failed")
        .description("Number of failed message sends")
        .register(meterRegistry);
    private static final AtomicLong lastSuccessfulSend = new AtomicLong(System.currentTimeMillis());
    private static final AtomicInteger activeOperations = new AtomicInteger(0);
    
    // Circuit breaker for resilience
    private static final CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50) // 50% failure rate triggers circuit breaker
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .build()
    );
    private static final CircuitBreaker kafkaCircuitBreaker = circuitBreakerRegistry.circuitBreaker("kafka-client");
    
    private static final ScheduledExecutorService healthMonitor = Executors.newScheduledThreadPool(1);
    
    // Message deduplication and idempotency
    private static final ConcurrentMap<String, MessageMetadata> processedMessages = new ConcurrentHashMap<>();
    private static final Counter duplicateMessages = Counter.builder("kafka.messages.duplicate")
        .description("Number of duplicate messages detected and skipped")
        .register(meterRegistry);
    private static final Counter idempotentOperations = Counter.builder("kafka.operations.idempotent")
        .description("Number of idempotent operations performed")
        .register(meterRegistry);
    
    /**
     * Metadata for tracking processed messages
     */
    private static class MessageMetadata {
        final long timestamp;
        final String messageId;
        final String contentHash;
        final boolean processed;
        
        MessageMetadata(String messageId, String contentHash, boolean processed) {
            this.timestamp = System.currentTimeMillis();
            this.messageId = messageId;
            this.contentHash = contentHash;
            this.processed = processed;
        }
        
        boolean isExpired(long ttlMs) {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }
    }
    
    static {
        // Register health metrics
        Gauge.builder("kafka.last.success.seconds", () -> 
            (System.currentTimeMillis() - lastSuccessfulSend.get()) / 1000.0)
            .description("Seconds since last successful message send")
            .register(meterRegistry);
            
        Gauge.builder("kafka.operations.active", activeOperations::get)
            .description("Number of active Kafka operations")
            .register(meterRegistry);
            
        // Start health monitoring
        startHealthMonitoring();
        
        // Start deduplication cleanup
        startDeduplicationCleanup();
    }
    
    public static void main(String[] args) {
        // Configure multiple datacenters
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
            .connectionTimeout(Duration.ofSeconds(10))
            .enableMetrics(true)
            .build();

        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            logger.info("Starting Kafka Multi-Datacenter Client with Health Monitoring");
            logger.info("Circuit Breaker State: {}", kafkaCircuitBreaker.getState());
            
            // Run all basic examples with health monitoring and idempotency
            basicProducerExample(client);
            idempotentProducerExample(client);
            basicConsumerExample(client);
            asyncProducerExample(client);
            reactiveProducerExample(client);
            reactiveConsumerExample(client);
            
            // Demonstrate deduplication by sending duplicates
            demonstrateDeduplication(client);
            
            // Show final health report
            reportHealthMetrics();
            
            logger.info("All basic usage examples completed successfully!");
            
        } catch (Exception e) {
            logger.error("Error running basic usage examples", e);
            failedMessages.increment();
        } finally {
            healthMonitor.shutdown();
            try {
                if (!healthMonitor.awaitTermination(5, TimeUnit.SECONDS)) {
                    healthMonitor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                healthMonitor.shutdownNow();
            }
        }
    }
    
    /**
     * Basic synchronous producer example
     */
    private static void basicProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Basic Producer Example ===");
        
        try {
            // Send simple messages
            for (int i = 0; i < 5; i++) {
                String key = "user-" + i;
                String value = "event-data-" + i;
                
                ProducerRecord<String, String> record = new ProducerRecord<>("user-events", key, value);
                RecordMetadata metadata = client.producerSync().send(record);
                
                // Track success metrics
                successfulMessages.increment();
                lastSuccessfulSend.set(System.currentTimeMillis());
                
                logger.info("Message sent: key={}, partition={}, offset={}", 
                           key, metadata.partition(), metadata.offset());
            }
            
            // Send message with headers
            RecordHeaders headers = new RecordHeaders();
            headers.add("source", "basic-example".getBytes());
            headers.add("version", "1.0".getBytes());
            headers.add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
            
            ProducerRecord<String, String> recordWithHeaders = new ProducerRecord<>(
                "user-events", null, "header-user", "header-data", headers);
            RecordMetadata metadata = client.producerSync().send(recordWithHeaders);
            
            // Track success metrics
            successfulMessages.increment();
            lastSuccessfulSend.set(System.currentTimeMillis());
            
            logger.info("Message with headers sent: partition={}, offset={}", 
                       metadata.partition(), metadata.offset());
            
        } catch (Exception e) {
            logger.error("Basic producer example failed", e);
            failedMessages.increment();
        }
    }
    
    /**
     * Basic synchronous consumer example
     */
    private static void basicConsumerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Basic Consumer Example ===");
        
        try {
            // Subscribe to topics
            client.consumerSync().subscribe(List.of("user-events"));
            
            // Poll for messages (limited for example)
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<String, String> records = client.consumerSync().poll(Duration.ofSeconds(2));
                
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumed message: key={}, value={}, topic={}, partition={}, offset={}", 
                               record.key(), record.value(), record.topic(), 
                               record.partition(), record.offset());
                    
                    // Process the message
                    processMessage(record);
                }
                
                // Commit offsets
                client.consumerSync().commitSync();
                logger.info("Offsets committed for poll {}", i + 1);
            }
            
        } catch (Exception e) {
            logger.error("Basic consumer example failed", e);
        }
    }
    
    /**
     * Asynchronous producer example
     */
    private static void asyncProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Async Producer Example ===");
        
        try {
            // Send multiple messages asynchronously
            List<CompletableFuture<RecordMetadata>> futures = new java.util.ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                String key = "async-user-" + i;
                String value = "async-event-data-" + i;
                
                ProducerRecord<String, String> record = new ProducerRecord<>("async-events", key, value);
                CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
                
                future.thenAccept(metadata -> 
                    logger.info("Async message sent: key={}, partition={}, offset={}", 
                               key, metadata.partition(), metadata.offset())
                ).exceptionally(throwable -> {
                    logger.error("Failed to send async message: key={}", key, throwable);
                    return null;
                });
                
                futures.add(future);
            }
            
            // Wait for all messages to be sent
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> logger.info("All async messages sent successfully!"))
                .join();
            
        } catch (Exception e) {
            logger.error("Async producer example failed", e);
        }
    }
    
    /**
     * Reactive producer example with backpressure handling
     */
    private static void reactiveProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Producer Example ===");
        
        try {
            // Create a stream of messages
            Flux<String> messageStream = Flux.range(1, 20)
                .map(i -> "reactive-message-" + i);
            
            // Send with backpressure control
            messageStream
                .flatMap(message -> {
                    String key = "reactive-key-" + message.split("-")[2];
                    ProducerRecord<String, String> record = new ProducerRecord<>("reactive-topic", key, message);
                    return client.producerReactive().send(record);
                }, 5) // Control concurrency
                .doOnNext(metadata -> 
                    logger.info("Reactive message sent to partition: {}", metadata.partition()))
                .doOnError(error -> 
                    logger.error("Reactive send error", error))
                .doOnComplete(() -> 
                    logger.info("All reactive messages sent!"))
                .blockLast(); // Wait for completion in this example
            
        } catch (Exception e) {
            logger.error("Reactive producer example failed", e);
        }
    }
    
    /**
     * Reactive consumer example with stream processing
     */
    private static void reactiveConsumerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Consumer Example ===");
        
        try {
            // Subscribe and process records as a stream
            reactor.core.Disposable subscription = client.consumerReactive()
                .subscribe("reactive-topic")
                .take(10) // Take only 10 messages for demo
                .doOnNext(record -> {
                    logger.info("Reactive consumed: key={}, value={}, partition={}, offset={}", 
                               record.key(), record.value(), record.partition(), record.offset());
                    
                    // Your processing logic here
                    processReactiveMessageGeneric(record);
                })
                .doOnError(error -> logger.error("Reactive consumer error", error))
                .doOnComplete(() -> logger.info("Reactive consumer completed"))
                .subscribe();
            
            // Wait a bit for messages
            Thread.sleep(5000);
            subscription.dispose();
            
        } catch (Exception e) {
            logger.error("Reactive consumer example failed", e);
        }
    }
    
    /**
     * Start continuous health monitoring
     */
    private static void startHealthMonitoring() {
        healthMonitor.scheduleAtFixedRate(() -> {
            try {
                reportHealthMetrics();
            } catch (Exception e) {
                logger.error("Health monitoring failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * Report current health and performance metrics
     */
    private static void reportHealthMetrics() {
        long timeSinceLastSuccess = (System.currentTimeMillis() - lastSuccessfulSend.get()) / 1000;
        double successRate = calculateSuccessRate();
        
        logger.info("=== KAFKA CLIENT HEALTH REPORT ===");
        logger.info("Success Rate: {:.2f}%", successRate);
        logger.info("Messages - Success: {}, Failed: {}, Duplicates: {}", 
                   (long)successfulMessages.count(), (long)failedMessages.count(), (long)duplicateMessages.count());
        logger.info("Idempotent Operations: {}", (long)idempotentOperations.count());
        logger.info("Processed Messages Cache Size: {}", processedMessages.size());
        logger.info("Seconds Since Last Success: {}", timeSinceLastSuccess);
        logger.info("Circuit Breaker State: {}", kafkaCircuitBreaker.getState());
        logger.info("Active Operations: {}", activeOperations.get());
        
        // Alert on issues
        if (successRate < 95.0 && (successfulMessages.count() + failedMessages.count()) > 10) {
            logger.warn("🚨 LOW SUCCESS RATE ALERT: {}%", successRate);
        }
        if (timeSinceLastSuccess > 120) {
            logger.warn("🚨 NO RECENT SUCCESS ALERT: {}s since last success", timeSinceLastSuccess);
        }
        logger.info("=====================================");
    }
    
    /**
     * Calculate current success rate
     */
    private static double calculateSuccessRate() {
        double total = successfulMessages.count() + failedMessages.count();
        return total > 0 ? (successfulMessages.count() / total) * 100.0 : 100.0;
    }
    
    /**
     * Start deduplication cleanup task
     */
    private static void startDeduplicationCleanup() {
        healthMonitor.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredMessages();
            } catch (Exception e) {
                logger.error("Deduplication cleanup failed", e);
            }
        }, 60, 60, TimeUnit.SECONDS); // Cleanup every minute
    }
    
    /**
     * Clean up expired message metadata
     */
    private static void cleanupExpiredMessages() {
        long ttl = 3600000; // 1 hour TTL
        int removed = 0;
        
        for (var entry : processedMessages.entrySet()) {
            if (entry.getValue().isExpired(ttl)) {
                processedMessages.remove(entry.getKey());
                removed++;
            }
        }
        
        if (removed > 0) {
            logger.debug("Cleaned up {} expired message entries", removed);
        }
    }
    
    /**
     * Generate idempotent message ID from record content
     */
    private static String generateIdempotentId(String topic, String key, String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String content = topic + "|" + (key != null ? key : "") + "|" + value;
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash
            return String.valueOf((topic + key + value).hashCode());
        }
    }
    
    /**
     * Check if message has already been processed (deduplication)
     */
    private static boolean isMessageAlreadyProcessed(String messageId, String contentHash) {
        MessageMetadata existing = processedMessages.get(messageId);
        if (existing != null) {
            if (existing.contentHash.equals(contentHash)) {
                duplicateMessages.increment();
                logger.warn("Duplicate message detected and skipped: {}", messageId.substring(0, 8) + "...");
                return true;
            } else {
                logger.warn("Message ID collision detected with different content: {}", messageId.substring(0, 8) + "...");
            }
        }
        return false;
    }
    
    /**
     * Mark message as processed for deduplication
     */
    private static void markMessageProcessed(String messageId, String contentHash) {
        processedMessages.put(messageId, new MessageMetadata(messageId, contentHash, true));
    }
    
    /**
     * Send message with idempotency and deduplication support
     */
    private static RecordMetadata sendIdempotentMessage(KafkaMultiDatacenterClient client, 
                                                       String topic, String key, String value) throws Exception {
        // Generate idempotent message ID
        String messageId = generateIdempotentId(topic, key, value);
        String contentHash = generateIdempotentId("content", "", value);
        
        // Check for duplicates
        if (isMessageAlreadyProcessed(messageId, contentHash)) {
            logger.info("Skipping duplicate message: topic={}, key={}", topic, key);
            return null; // Return null to indicate duplicate
        }
        
        // Add idempotency headers
        RecordHeaders headers = new RecordHeaders();
        headers.add("message-id", messageId.getBytes());
        headers.add("content-hash", contentHash.getBytes());
        headers.add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        headers.add("client-id", "kafka-multidc-client".getBytes());
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, key, value, headers);
        
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            
            // Mark as processed after successful send
            markMessageProcessed(messageId, contentHash);
            idempotentOperations.increment();
            successfulMessages.increment();
            lastSuccessfulSend.set(System.currentTimeMillis());
            
            logger.info("Idempotent message sent: topic={}, key={}, messageId={}, partition={}, offset={}", 
                       topic, key, messageId.substring(0, 8) + "...", metadata.partition(), metadata.offset());
            
            return metadata;
        } catch (Exception e) {
            logger.error("Failed to send idempotent message: topic={}, key={}, messageId={}", 
                        topic, key, messageId.substring(0, 8) + "...", e);
            failedMessages.increment();
            throw e;
        }
    }
    
    /**
     * Process a consumed message with deduplication
     */
    private static void processMessage(ConsumerRecord<String, String> record) {
        logger.info("Processing message: {}", record.value());
        
        // Extract message ID from headers for deduplication
        String messageId = null;
        String contentHash = null;
        
        if (record.headers() != null) {
            var messageIdHeader = record.headers().lastHeader("message-id");
            var contentHashHeader = record.headers().lastHeader("content-hash");
            
            if (messageIdHeader != null) {
                messageId = new String(messageIdHeader.value());
            }
            if (contentHashHeader != null) {
                contentHash = new String(contentHashHeader.value());
            }
        }
        
        // If no message ID in headers, generate one from content
        if (messageId == null) {
            messageId = generateIdempotentId(record.topic(), record.key(), record.value());
            contentHash = generateIdempotentId("content", "", record.value());
        }
        
        // Check for duplicates
        if (isMessageAlreadyProcessed(messageId, contentHash)) {
            logger.info("Skipping duplicate message: topic={}, key={}, messageId={}", 
                       record.topic(), record.key(), messageId.substring(0, 8) + "...");
            return;
        }
        
        // Simulate processing time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Mark as processed and track success metrics
        markMessageProcessed(messageId, contentHash);
        idempotentOperations.increment();
        successfulMessages.increment();
        lastSuccessfulSend.set(System.currentTimeMillis());
        
        logger.info("Message processed successfully: key={}, messageId={}", 
                   record.key(), messageId.substring(0, 8) + "...");
    }
    
    /**
     * Process a reactive message (generic version) with deduplication
     */
    private static void processReactiveMessageGeneric(ConsumerRecord<Object, Object> record) {
        logger.info("Processing reactive message: key={}, value={}", record.key(), record.value());
        
        // Convert to string for processing
        String key = record.key() != null ? record.key().toString() : null;
        String value = record.value() != null ? record.value().toString() : "";
        
        // Generate message ID for deduplication
        String messageId = generateIdempotentId(record.topic(), key, value);
        String contentHash = generateIdempotentId("content", "", value);
        
        // Check for duplicates
        if (isMessageAlreadyProcessed(messageId, contentHash)) {
            logger.info("Skipping duplicate reactive message: topic={}, key={}, messageId={}", 
                       record.topic(), key, messageId.substring(0, 8) + "...");
            return;
        }
        
        // Track processing metrics
        markMessageProcessed(messageId, contentHash);
        idempotentOperations.increment();
        successfulMessages.increment();
        lastSuccessfulSend.set(System.currentTimeMillis());
        
        // Add your reactive processing logic here
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        logger.info("Reactive message processed: key={}, messageId={}", 
                   key, messageId.substring(0, 8) + "...");
    }
    
    /**
     * Idempotent producer example with deduplication
     */
    private static void idempotentProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Idempotent Producer Example ===");
        
        try {
            // Send messages with idempotency support
            for (int i = 0; i < 5; i++) {
                String key = "idempotent-user-" + i;
                String value = "idempotent-event-data-" + i;
                
                RecordMetadata metadata = sendIdempotentMessage(client, "idempotent-events", key, value);
                if (metadata != null) {
                    logger.info("New idempotent message sent: key={}, partition={}, offset={}", 
                               key, metadata.partition(), metadata.offset());
                } else {
                    logger.info("Duplicate idempotent message skipped: key={}", key);
                }
            }
            
        } catch (Exception e) {
            logger.error("Idempotent producer example failed", e);
        }
    }
    
    /**
     * Demonstrate deduplication by sending duplicate messages
     */
    private static void demonstrateDeduplication(KafkaMultiDatacenterClient client) {
        logger.info("=== Deduplication Demonstration ===");
        
        try {
            String key = "duplicate-test";
            String value = "test-message-for-deduplication";
            
            // Send the same message multiple times to demonstrate deduplication
            logger.info("Sending the same message 3 times to demonstrate deduplication:");
            
            for (int i = 1; i <= 3; i++) {
                logger.info("Attempt {} to send duplicate message:", i);
                RecordMetadata metadata = sendIdempotentMessage(client, "duplicate-test", key, value);
                
                if (metadata != null) {
                    logger.info("  ✓ Message sent: partition={}, offset={}", 
                               metadata.partition(), metadata.offset());
                } else {
                    logger.info("  ⚠ Duplicate detected and skipped");
                }
            }
            
            // Send a different message with the same key to show it's not blocked
            logger.info("Sending different message with same key:");
            RecordMetadata metadata = sendIdempotentMessage(client, "duplicate-test", key, "different-content");
            if (metadata != null) {
                logger.info("  ✓ New message sent: partition={}, offset={}", 
                           metadata.partition(), metadata.offset());
            }
            
        } catch (Exception e) {
            logger.error("Deduplication demonstration failed", e);
        }
    }
}
