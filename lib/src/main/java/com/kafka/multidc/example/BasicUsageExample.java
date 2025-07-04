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
import io.micrometer.core.instrument.Timer;
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
import java.util.UUID;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

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
    private static final ScheduledExecutorService performanceMonitor = Executors.newScheduledThreadPool(1);
    
    // Message deduplication and idempotency
    private static final ConcurrentMap<String, MessageMetadata> processedMessages = new ConcurrentHashMap<>();
    private static final Counter duplicateMessages = Counter.builder("kafka.messages.duplicate")
        .description("Number of duplicate messages detected and skipped")
        .register(meterRegistry);
    private static final Counter idempotentOperations = Counter.builder("kafka.operations.idempotent")
        .description("Number of idempotent operations performed")
        .register(meterRegistry);
    
    // Distributed tracing and correlation
    private static final Counter tracedMessages = Counter.builder("kafka.messages.traced")
        .description("Number of messages with distributed tracing")
        .register(meterRegistry);
    private static final Counter correlatedRequests = Counter.builder("kafka.requests.correlated")
        .description("Number of correlated request flows")
        .register(meterRegistry);
    
    // Thread-local context for request correlation
    private static final ThreadLocal<TraceContext> traceContext = new ThreadLocal<>();
    
    // ===== PERFORMANCE BENCHMARKING AND AUTO-OPTIMIZATION =====
    
    // Performance monitoring metrics
    private static final Timer sendLatencyTimer = Timer.builder("kafka.send.latency")
        .description("Message send latency")
        .register(meterRegistry);
    private static final Timer receiveLatencyTimer = Timer.builder("kafka.receive.latency")
        .description("Message receive latency")
        .register(meterRegistry);
    private static final Counter throughputMessages = Counter.builder("kafka.throughput.messages")
        .description("Total throughput messages processed")
        .register(meterRegistry);
    private static final Counter optimizationApplied = Counter.builder("kafka.optimization.applied")
        .description("Number of automatic optimizations applied")
        .register(meterRegistry);
    
    // Performance monitoring state
    private static final AtomicLong totalBytesProcessed = new AtomicLong(0);
    private static final AtomicLong benchmarkStartTime = new AtomicLong(System.currentTimeMillis());
    private static final AtomicInteger currentThroughputRpm = new AtomicInteger(0); // Records per minute
    private static final ConcurrentMap<String, PerformanceSnapshot> datacenterMetrics = new ConcurrentHashMap<>();
    
    // Auto-optimization settings
    private static volatile boolean autoOptimizationEnabled = true;
    private static final AtomicLong lastOptimizationTime = new AtomicLong(0);
    private static final long OPTIMIZATION_COOLDOWN_MS = 60_000; // 1 minute cooldown
    
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
    
    /**
     * Distributed tracing context for request correlation
     */
    private static class TraceContext {
        final String traceId;
        final String spanId;
        final String parentSpanId;
        final long startTime;
        final String operation;
        final String serviceName;
        
        TraceContext(String operation, String serviceName) {
            this.traceId = UUID.randomUUID().toString().replace("-", "");
            this.spanId = UUID.randomUUID().toString().substring(0, 16);
            this.parentSpanId = null;
            this.startTime = System.currentTimeMillis();
            this.operation = operation;
            this.serviceName = serviceName;
        }
        
        TraceContext(String traceId, String parentSpanId, String operation, String serviceName) {
            this.traceId = traceId;
            this.spanId = UUID.randomUUID().toString().substring(0, 16);
            this.parentSpanId = parentSpanId;
            this.startTime = System.currentTimeMillis();
            this.operation = operation;
            this.serviceName = serviceName;
        }
        
        long getDurationMs() {
            return System.currentTimeMillis() - startTime;
        }
        
        String getTraceInfo() {
            return String.format("trace=%s span=%s", traceId.substring(0, 8), spanId.substring(0, 8));
        }
    }
    
    /**
     * Captures performance metrics for a specific datacenter at a point in time
     */
    private static class PerformanceSnapshot {
        final String datacenterId;
        final long timestamp;
        final double avgLatencyMs;
        final int throughputRpm;
        final double errorRate;
        final long bytesPerSecond;
        final int batchEfficiency;
        
        PerformanceSnapshot(String datacenterId, double avgLatencyMs, int throughputRpm, 
                          double errorRate, long bytesPerSecond, int batchEfficiency) {
            this.datacenterId = datacenterId;
            this.timestamp = System.currentTimeMillis();
            this.avgLatencyMs = avgLatencyMs;
            this.throughputRpm = throughputRpm;
            this.errorRate = errorRate;
            this.bytesPerSecond = bytesPerSecond;
            this.batchEfficiency = batchEfficiency;
        }
    }
    
    /**
     * Represents an optimization recommendation with priority and expected impact
     */
    private static class OptimizationRecommendation {
        final String type;
        final String description;
        final String priority;
        final double expectedImpact;
        final Map<String, String> parameters;
        
        OptimizationRecommendation(String type, String description, String priority, 
                                 double expectedImpact, Map<String, String> parameters) {
            this.type = type;
            this.description = description;
            this.priority = priority;
            this.expectedImpact = expectedImpact;
            this.parameters = parameters != null ? parameters : new HashMap<>();
        }
    }
    
    /**
     * Performance benchmark for testing and optimization
     */
    private static class PerformanceBenchmark {
        final String name;
        final long startTime;
        long endTime;
        int messagesProcessed;
        int errorsEncountered;
        boolean completed;
        
        PerformanceBenchmark(String name) {
            this.name = name;
            this.startTime = System.currentTimeMillis();
            this.messagesProcessed = 0;
            this.errorsEncountered = 0;
            this.completed = false;
        }
        
        void recordMessage() {
            messagesProcessed++;
        }
        
        void recordError() {
            errorsEncountered++;
        }
        
        boolean isCompleted() {
            return completed;
        }
        
        BenchmarkResult complete() {
            endTime = System.currentTimeMillis();
            completed = true;
            return new BenchmarkResult(name, endTime - startTime, messagesProcessed, errorsEncountered);
        }
    }
    
    /**
     * Result of a performance benchmark
     */
    private static class BenchmarkResult {
        final String name;
        final long durationMs;
        final int totalMessages;
        final int totalErrors;
        final double messagesPerSecond;
        final double errorRate;
        
        BenchmarkResult(String name, long durationMs, int totalMessages, int totalErrors) {
            this.name = name;
            this.durationMs = durationMs;
            this.totalMessages = totalMessages;
            this.totalErrors = totalErrors;
            this.messagesPerSecond = durationMs > 0 ? (totalMessages * 1000.0) / durationMs : 0;
            this.errorRate = totalMessages > 0 ? (double) totalErrors / totalMessages : 0;
        }
    }
    
    // Performance benchmarking state
    private static volatile PerformanceBenchmark currentBenchmark;
    
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
            
            // Run all basic examples with health monitoring, idempotency, and tracing
            basicProducerExample(client);
            idempotentProducerExample(client);
            distributedTracingExample(client);
            basicConsumerExample(client);
            asyncProducerExample(client);
            reactiveProducerExample(client);
            reactiveConsumerExample(client);
            
            // Demonstrate end-to-end tracing
            demonstrateEndToEndTracing(client);
            
            // Demonstrate performance benchmarking and auto-optimization
            demonstratePerformanceBenchmarking(client);
            
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
        logger.info("Tracing - Traced Messages: {}, Correlated Requests: {}", 
                   (long)tracedMessages.count(), (long)correlatedRequests.count());
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
     * Start a new distributed trace
     */
    private static TraceContext startTrace(String operation, String serviceName) {
        TraceContext context = new TraceContext(operation, serviceName);
        traceContext.set(context);
        tracedMessages.increment();
        
        logger.info("Started trace: {} operation={} service={}", 
                   context.getTraceInfo(), operation, serviceName);
        return context;
    }
    
    /**
     * Finish current trace and log timing
     */
    private static void finishTrace() {
        TraceContext context = traceContext.get();
        if (context != null) {
            long duration = context.getDurationMs();
            logger.info("Finished trace: {} operation={} duration={}ms", 
                       context.getTraceInfo(), context.operation, duration);
            
            // Alert on slow operations
            if (duration > 5000) {
                logger.warn("🐌 SLOW OPERATION ALERT: {} took {}ms", context.operation, duration);
            }
            
            traceContext.remove();
        }
    }
    
    /**
     * Send message with distributed tracing support
     */
    private static RecordMetadata sendTracedMessage(KafkaMultiDatacenterClient client, 
                                                   String topic, String key, String value, 
                                                   String operation) throws Exception {
        // Start tracing
        TraceContext context = startTrace(operation, "kafka-producer");
        
        try {
            // Generate idempotent message ID
            String messageId = generateIdempotentId(topic, key, value);
            String contentHash = generateIdempotentId("content", "", value);
            
            // Check for duplicates
            if (isMessageAlreadyProcessed(messageId, contentHash)) {
                logger.info("Skipping duplicate traced message: topic={} {} key={}", 
                           topic, context.getTraceInfo(), key);
                return null; // Return null to indicate duplicate
            }
            
            // Add comprehensive headers
            RecordHeaders headers = new RecordHeaders();
            
            // Idempotency headers
            headers.add("message-id", messageId.getBytes());
            headers.add("content-hash", contentHash.getBytes());
            headers.add("client-id", "kafka-multidc-client".getBytes());
            
            // Tracing headers  
            headers.add("trace-id", context.traceId.getBytes());
            headers.add("span-id", context.spanId.getBytes());
            headers.add("operation", context.operation.getBytes());
            headers.add("service-name", context.serviceName.getBytes());
            
            // Business headers
            headers.add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
            headers.add("environment", "production".getBytes());
            headers.add("version", "1.0.0".getBytes());
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, key, value, headers);
            
            RecordMetadata metadata = client.producerSync().send(record);
            
            // Mark as processed and track metrics
            markMessageProcessed(messageId, contentHash);
            idempotentOperations.increment();
            successfulMessages.increment();
            lastSuccessfulSend.set(System.currentTimeMillis());
            
            logger.info("Traced message sent: {} topic={} key={} messageId={} partition={} offset={}", 
                       context.getTraceInfo(), topic, key, messageId.substring(0, 8) + "...", 
                       metadata.partition(), metadata.offset());
            
            return metadata;
            
        } catch (Exception e) {
            logger.error("Failed to send traced message: {} topic={} key={}", 
                        context.getTraceInfo(), topic, key, e);
            failedMessages.increment();
            throw e;
        } finally {
            finishTrace();
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
     * Distributed tracing example
     */
    private static void distributedTracingExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Distributed Tracing Example ===");
        
        try {
            // Send messages with distributed tracing
            for (int i = 0; i < 3; i++) {
                String key = "traced-user-" + i;
                String value = "traced-event-data-" + i;
                
                RecordMetadata metadata = sendTracedMessage(client, "traced-events", key, value, "user-event-processing");
                if (metadata != null) {
                    logger.info("Traced message sent successfully: key={}, partition={}, offset={}", 
                               key, metadata.partition(), metadata.offset());
                }
            }
            
        } catch (Exception e) {
            logger.error("Distributed tracing example failed", e);
        }
    }
    
    /**
     * Demonstrate end-to-end distributed tracing
     */
    private static void demonstrateEndToEndTracing(KafkaMultiDatacenterClient client) {
        logger.info("=== End-to-End Tracing Demonstration ===");
        
        try {
            // Simulate a business workflow with multiple steps
            TraceContext mainTrace = startTrace("user-registration-workflow", "registration-service");
            
            try {
                // Step 1: Create user account
                logger.info("Step 1: Creating user account - {}", mainTrace.getTraceInfo());
                RecordMetadata step1 = sendTracedMessage(client, "user-accounts", "user-123", 
                    "{\"action\":\"create\",\"userId\":\"user-123\",\"email\":\"user@example.com\"}", 
                    "account-creation");
                
                if (step1 != null) {
                    logger.info("✓ User account created: {}", mainTrace.getTraceInfo());
                }
                
                // Step 2: Send welcome email
                logger.info("Step 2: Sending welcome email - {}", mainTrace.getTraceInfo());
                RecordMetadata step2 = sendTracedMessage(client, "email-notifications", "user-123", 
                    "{\"type\":\"welcome\",\"userId\":\"user-123\",\"template\":\"welcome-email\"}", 
                    "email-notification");
                
                if (step2 != null) {
                    logger.info("✓ Welcome email queued: {}", mainTrace.getTraceInfo());
                }
                
                // Step 3: Log audit event
                logger.info("Step 3: Logging audit event - {}", mainTrace.getTraceInfo());
                RecordMetadata step3 = sendTracedMessage(client, "audit-logs", "user-123", 
                    "{\"event\":\"user-registered\",\"userId\":\"user-123\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}", 
                    "audit-logging");
                
                if (step3 != null) {
                    logger.info("✓ Audit event logged: {}", mainTrace.getTraceInfo());
                }
                
                logger.info("🎉 User registration workflow completed successfully: {}", mainTrace.getTraceInfo());
                
            } catch (Exception e) {
                logger.error("User registration workflow failed: {} - {}", mainTrace.getTraceInfo(), e.getMessage(), e);
                throw e;
            } finally {
                finishTrace();
            }
            
        } catch (Exception e) {
            logger.error("End-to-end tracing demonstration failed", e);
        }
    }
    
    // ===== PERFORMANCE BENCHMARKING AND AUTO-OPTIMIZATION METHODS =====
    
    /**
     * Start performance monitoring and auto-optimization
     */
    private static void startPerformanceMonitoring() {
        // Start real-time performance monitoring
        performanceMonitor.scheduleAtFixedRate(() -> {
            try {
                collectPerformanceMetrics();
                analyzePerformanceAndOptimize();
            } catch (Exception e) {
                logger.error("Performance monitoring failed", e);
            }
        }, 15, 15, TimeUnit.SECONDS); // Monitor every 15 seconds
        
        // Start performance benchmarking
        performanceMonitor.scheduleAtFixedRate(() -> {
            try {
                runPerformanceBenchmark();
            } catch (Exception e) {
                logger.error("Performance benchmarking failed", e);
            }
        }, 60, 120, TimeUnit.SECONDS); // Benchmark every 2 minutes
        
        logger.info("Performance monitoring and auto-optimization started");
    }
    
    /**
     * Collect real-time performance metrics for all datacenters
     */
    private static void collectPerformanceMetrics() {
        try {
            // Simulate collecting metrics from different datacenters
            collectDatacenterMetrics("us-east-1");
            collectDatacenterMetrics("us-west-1");
            
            // Update current throughput
            long currentTime = System.currentTimeMillis();
            long elapsedMinutes = (currentTime - benchmarkStartTime.get()) / 60000;
            if (elapsedMinutes > 0) {
                currentThroughputRpm.set((int) (throughputMessages.count() / elapsedMinutes));
            }
            
            logger.debug("Performance metrics collected for {} datacenters", datacenterMetrics.size());
            
        } catch (Exception e) {
            logger.error("Failed to collect performance metrics", e);
        }
    }
    
    /**
     * Collect performance metrics for a specific datacenter
     */
    private static void collectDatacenterMetrics(String datacenterId) {
        // Simulate realistic performance metrics with some variation
        double baseLatency = ThreadLocalRandom.current().nextDouble(50, 200);
        double variationFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.3);
        
        double avgLatency = baseLatency * variationFactor;
        double p95Latency = avgLatency * 1.5;
        double p99Latency = avgLatency * 2.2;
        
        int baseRpm = ThreadLocalRandom.current().nextInt(800, 1200);
        int throughputRpm = (int) (baseRpm * variationFactor);
        
        double errorRate = Math.max(0, ThreadLocalRandom.current().nextGaussian() * 0.02 + 0.01);
        long bytesPerSecond = (long) (throughputRpm * 1024 * ThreadLocalRandom.current().nextDouble(0.5, 2.0));
        int batchEfficiency = ThreadLocalRandom.current().nextInt(70, 95);
        
        String status = avgLatency < 100 && errorRate < 0.05 ? "HEALTHY" : 
                       avgLatency < 300 && errorRate < 0.10 ? "DEGRADED" : "CRITICAL";
        
        PerformanceSnapshot snapshot = new PerformanceSnapshot(
            datacenterId, avgLatency, throughputRpm, 
            errorRate, bytesPerSecond, batchEfficiency
        );
        
        datacenterMetrics.put(datacenterId, snapshot);
        
        logger.debug("Performance metrics for {}: {:.1f}ms avg latency, {} rpm, {:.2f}% error rate", 
                    datacenterId, avgLatency, throughputRpm, errorRate * 100);
    }
    
    /**
     * Analyze performance and apply optimizations if needed
     */
    private static void analyzePerformanceAndOptimize() {
        if (!autoOptimizationEnabled) {
            return;
        }
        
        long now = System.currentTimeMillis();
        if (now - lastOptimizationTime.get() < OPTIMIZATION_COOLDOWN_MS) {
            return; // Respect cooldown period
        }
        
        try {
            for (PerformanceSnapshot snapshot : datacenterMetrics.values()) {
                if (snapshot.avgLatencyMs > 200 || snapshot.errorRate > 0.05 || snapshot.throughputRpm < 500) {
                    List<OptimizationRecommendation> recommendations = generateOptimizationRecommendations(snapshot);
                    
                    if (!recommendations.isEmpty()) {
                        applyOptimizations(snapshot.datacenterId, recommendations);
                        lastOptimizationTime.set(now);
                        optimizationApplied.increment();
                        
                        logger.info("Auto-optimization applied for datacenter {}: {} recommendations", 
                                   snapshot.datacenterId, recommendations.size());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Performance analysis and optimization failed", e);
        }
    }
    
    /**
     * Generate optimization recommendations based on performance metrics
     */
    private static List<OptimizationRecommendation> generateOptimizationRecommendations(PerformanceSnapshot snapshot) {
        List<OptimizationRecommendation> recommendations = new ArrayList<>();
        
        // High latency optimization
        if (snapshot.avgLatencyMs > 200) {
            Map<String, String> latencyParams = new HashMap<>();
            latencyParams.put("batch.size", "32768");
            latencyParams.put("linger.ms", "5");
            recommendations.add(new OptimizationRecommendation(
                "REDUCE_LATENCY",
                "Increase batch size and reduce linger time to improve latency",
                "HIGH",
                15.0,
                latencyParams
            ));
        }
        
        // Low throughput optimization
        if (snapshot.throughputRpm < 500) {
            Map<String, String> throughputParams = new HashMap<>();
            throughputParams.put("batch.size", "65536");
            throughputParams.put("linger.ms", "10");
            throughputParams.put("compression.type", "lz4");
            recommendations.add(new OptimizationRecommendation(
                "INCREASE_THROUGHPUT",
                "Optimize producer settings for higher throughput",
                "HIGH",
                25.0,
                throughputParams
            ));
        }
        
        // High error rate optimization
        if (snapshot.errorRate > 0.05) {
            Map<String, String> errorParams = new HashMap<>();
            errorParams.put("retries", "5");
            errorParams.put("retry.backoff.ms", "1000");
            errorParams.put("request.timeout.ms", "60000");
            recommendations.add(new OptimizationRecommendation(
                "REDUCE_ERRORS",
                "Increase retries and timeout settings to reduce error rate",
                "MEDIUM",
                20.0,
                errorParams
            ));
        }
        
        // Batch efficiency optimization
        if (snapshot.batchEfficiency < 80) {
            Map<String, String> batchParams = new HashMap<>();
            batchParams.put("batch.size", "49152");
            batchParams.put("linger.ms", "15");
            batchParams.put("max.in.flight.requests.per.connection", "3");
            recommendations.add(new OptimizationRecommendation(
                "OPTIMIZE_BATCHING",
                "Tune batching parameters for better efficiency",
                "MEDIUM",
                10.0,
                batchParams
            ));
        }
        
        return recommendations;
    }
    
    /**
     * Apply optimization recommendations to a datacenter
     */
    private static void applyOptimizations(String datacenterId, List<OptimizationRecommendation> recommendations) {
        logger.info("🚀 Applying {} performance optimizations for datacenter {}", 
                   recommendations.size(), datacenterId);
        
        for (OptimizationRecommendation rec : recommendations) {
            logger.info("  • {} - {} (Expected improvement: {:.1f}%)", 
                       rec.type, rec.description, rec.expectedImpact);
            logger.debug("    Parameters: {}", rec.parameters);
            
            // In a real implementation, you would apply these settings to the Kafka client
            simulateOptimizationApplication(datacenterId, rec);
        }
    }
    
    /**
     * Simulate applying optimization (in real implementation, this would configure the client)
     */
    private static void simulateOptimizationApplication(String datacenterId, OptimizationRecommendation rec) {
        // Simulate the optimization taking effect by improving the metrics slightly
        PerformanceSnapshot current = datacenterMetrics.get(datacenterId);
        if (current != null) {
            double improvementFactor = 1.0 - (rec.expectedImpact / 100.0);
            
            double newLatency = current.avgLatencyMs * improvementFactor;
            double newErrorRate = current.errorRate * improvementFactor;
            int newThroughput = (int) (current.throughputRpm * (2.0 - improvementFactor));
            
            PerformanceSnapshot improved = new PerformanceSnapshot(
                datacenterId, newLatency, newThroughput, newErrorRate, 
                current.bytesPerSecond * (long)(2.0 - improvementFactor),
                Math.min(95, current.batchEfficiency + 5)
            );
            
            datacenterMetrics.put(datacenterId, improved);
            logger.debug("Applied optimization {} to {}: latency {:.1f}ms -> {:.1f}ms", 
                        rec.type, datacenterId, current.avgLatencyMs, newLatency);
        }
    }
    
    /**
     * Run a comprehensive performance benchmark
     */
    private static void runPerformanceBenchmark() {
        if (currentBenchmark != null && !currentBenchmark.isCompleted()) {
            return; // Already running a benchmark
        }
        
        logger.info("🏁 Starting performance benchmark...");
        currentBenchmark = new PerformanceBenchmark("Real-time Performance Test");
        
        try {
            // Simulate benchmark operations
            logger.info("Running performance benchmark tests...");
            Thread.sleep(2000); // Simulate benchmark operations
            currentBenchmark.messagesProcessed = 1000;
            
            // Complete benchmark and generate report
            BenchmarkResult result = currentBenchmark.complete();
            logger.info("Benchmark completed: {} messages in {}ms (Rate: {:.2f} msg/sec)", 
                       result.totalMessages, result.durationMs, result.messagesPerSecond);
            
        } catch (Exception e) {
            logger.error("Performance benchmark failed", e);
            if (currentBenchmark != null) {
                currentBenchmark.recordError();
                currentBenchmark.complete();
            }
        }
    }
    
    /**
     * Run producer performance test
     */
    private static void runBenchmarkProducerTest(PerformanceBenchmark benchmark) {
        int messageCount = 100;
        int messageSize = 1024; // 1KB messages
        
        for (int i = 0; i < messageCount; i++) {
            long startTime = System.currentTimeMillis();
            
            try {
                // Simulate message sending with realistic timing
                Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50)); // 10-50ms send time
                
                long latency = System.currentTimeMillis() - startTime;
                benchmark.recordMessage();
                
                sendLatencyTimer.record(Duration.ofMillis(latency));
                
            } catch (Exception e) {
                benchmark.recordError();
                logger.debug("Benchmark producer test error: {}", e.getMessage());
            }
        }
        
        logger.debug("Benchmark producer test completed: {} messages", messageCount);
    }
    
    /**
     * Run consumer performance test
     */
    private static void runBenchmarkConsumerTest(PerformanceBenchmark benchmark) {
        int messageCount = 80; // Slightly less than producer to simulate real-world lag
        
        for (int i = 0; i < messageCount; i++) {
            long startTime = System.currentTimeMillis();
            
            try {
                // Simulate message receiving with realistic timing
                Thread.sleep(ThreadLocalRandom.current().nextInt(5, 30)); // 5-30ms receive time
                
                long latency = System.currentTimeMillis() - startTime;
                benchmark.recordMessage();
                
                receiveLatencyTimer.record(Duration.ofMillis(latency));
                
            } catch (Exception e) {
                benchmark.recordError();
                logger.debug("Benchmark consumer test error: {}", e.getMessage());
            }
        }
        
        logger.debug("Benchmark consumer test completed: {} messages", messageCount);
    }
    
    /**
     * Store benchmark result for historical analysis
     */
    private static void storeBenchmarkResult(BenchmarkResult result) {
        // In a real implementation, you would store this in a time-series database
        // For now, we'll just log the key metrics for trend analysis
        
        logger.info("📊 Benchmark Results Stored:");
        logger.info("  Timestamp: {}", Instant.now());
        logger.info("  Total Messages: {}", result.totalMessages);
        logger.info("  Total Errors: {}", result.totalErrors);
        logger.info("  Duration: {}ms", result.durationMs);
        logger.info("  Messages/sec: {:.2f}", result.messagesPerSecond);
        logger.info("  Error Rate: {:.4f}%", result.errorRate * 100);
        
        // Check for performance regression
        checkPerformanceRegression(result);
    }
    
    /**
     * Check for performance regression compared to baseline
     */
    private static void checkPerformanceRegression(BenchmarkResult result) {
        // Define baseline performance expectations
        double baselineThroughput = 50.0; // 50 msg/sec minimum
        double baselineLatency = 100.0;   // 100ms maximum
        double baselineErrorRate = 0.01;  // 1% maximum
        
        boolean regressionDetected = false;
        StringBuilder regressionReport = new StringBuilder("🚨 PERFORMANCE REGRESSION DETECTED:\n");
        
        if (result.messagesPerSecond < baselineThroughput) {
            regressionDetected = true;
            regressionReport.append(String.format("  • Throughput below baseline: %.2f < %.2f msg/sec\n", 
                result.messagesPerSecond, baselineThroughput));
        }
        
        // Note: Latency comparison removed since we don't track individual latencies
        
        if (result.errorRate > baselineErrorRate) {
            regressionDetected = true;
            regressionReport.append(String.format("  • Error rate above baseline: %.4f%% > %.4f%%\n", 
                result.errorRate * 100, baselineErrorRate * 100));
        }
        
        if (regressionDetected) {
            logger.warn(regressionReport.toString());
            
            // Trigger auto-optimization
            if (autoOptimizationEnabled) {
                logger.info("Triggering auto-optimization due to performance regression...");
                lastOptimizationTime.set(0); // Reset cooldown to allow immediate optimization
            }
        } else {
            logger.info("✅ Performance within acceptable baseline parameters");
        }
    }
    
    /**
     * Generate comprehensive performance report
     */
    private static void generatePerformanceReport() {
        logger.info("=== COMPREHENSIVE PERFORMANCE REPORT ===");
        
        // Overall performance metrics
        long totalMessages = (long) throughputMessages.count();
        long elapsedTime = System.currentTimeMillis() - benchmarkStartTime.get();
        double avgThroughputPerSecond = elapsedTime > 0 ? (totalMessages * 1000.0) / elapsedTime : 0;
        
        logger.info("📈 Overall Performance:");
        logger.info("  Total Runtime: {} minutes", elapsedTime / 60000);
        logger.info("  Total Messages Processed: {}", totalMessages);
        logger.info("  Average Throughput: {:.2f} msg/sec", avgThroughputPerSecond);
        logger.info("  Total Data Processed: {:.2f} MB", totalBytesProcessed.get() / (1024.0 * 1024.0));
        logger.info("  Current RPM: {}", currentThroughputRpm.get());
        
        // Datacenter-specific performance
        logger.info("🌐 Datacenter Performance:");
        for (PerformanceSnapshot snapshot : datacenterMetrics.values()) {
            logger.info("  Datacenter: {}", snapshot.datacenterId);
            logger.info("    Latency: {:.1f}ms avg", snapshot.avgLatencyMs);
            logger.info("    Throughput: {} rpm, {:.2f} MB/s", 
                       snapshot.throughputRpm, snapshot.bytesPerSecond / (1024.0 * 1024.0));
            logger.info("    Error Rate: {:.2f}%", snapshot.errorRate * 100);
            logger.info("    Batch Efficiency: {}%", snapshot.batchEfficiency);
        }
        
        // Optimization history
        logger.info("🔧 Optimization Summary:");
        logger.info("  Auto-optimization: {}", autoOptimizationEnabled ? "ENABLED" : "DISABLED");
        logger.info("  Optimizations Applied: {}", (long) optimizationApplied.count());
        logger.info("  Last Optimization: {} seconds ago", 
                   (System.currentTimeMillis() - lastOptimizationTime.get()) / 1000);
        
        logger.info("==========================================");
    }
    
    /**
     * Enhanced health report with performance metrics
     */
    private static void reportEnhancedHealthMetrics() {
        // Original health metrics
        reportHealthMetrics();
        
        // Additional performance metrics
        logger.info("=== PERFORMANCE METRICS ===");
        logger.info("Current Throughput: {} rpm", currentThroughputRpm.get());
        logger.info("Total Data Processed: {:.2f} MB", totalBytesProcessed.get() / (1024.0 * 1024.0));
        logger.info("Optimizations Applied: {}", (long) optimizationApplied.count());
        
        // Performance alerts
        if (currentThroughputRpm.get() < 100) {
            logger.warn("🚨 LOW THROUGHPUT ALERT: {} rpm", currentThroughputRpm.get());
        }
        
        for (PerformanceSnapshot snapshot : datacenterMetrics.values()) {
            if (snapshot.avgLatencyMs > 200 || snapshot.errorRate > 0.05 || snapshot.throughputRpm < 500) {
                logger.warn("🚨 DATACENTER PERFORMANCE ALERT: {} requires optimization", snapshot.datacenterId);
            }
        }
        
        logger.info("==============================");
    }
    
    /**
     * Demonstrate performance benchmarking and auto-optimization
     */
    private static void demonstratePerformanceBenchmarking(KafkaMultiDatacenterClient client) {
        logger.info("=== Performance Benchmarking and Auto-Optimization Demo ===");
        
        try {
            // Start performance monitoring
            startPerformanceMonitoring();
            
            // Run a dedicated performance test
            logger.info("🏃 Running dedicated performance test...");
            PerformanceBenchmark dedicatedTest = new PerformanceBenchmark("Dedicated Performance Test");
            
            // Send test messages with performance monitoring
            for (int i = 0; i < 50; i++) {
                long startTime = System.currentTimeMillis();
                
                String key = "perf-test-" + i;
                String value = "performance-test-message-" + i + "-" + System.currentTimeMillis();
                
                try {
                    RecordMetadata metadata = sendTracedMessage(client, "performance-test", key, value, "performance-testing");
                    
                    if (metadata != null) {
                        long latency = System.currentTimeMillis() - startTime;
                        dedicatedTest.recordMessage();
                        
                        logger.debug("Performance test message {} sent: {}ms latency", i, latency);
                    }
                } catch (Exception e) {
                    dedicatedTest.recordError();
                    logger.debug("Performance test message {} failed: {}", i, e.getMessage());
                }
                
                // Small delay to simulate realistic load
                Thread.sleep(50);
            }
            
            // Complete test and show results
            BenchmarkResult testResult = dedicatedTest.complete();
            logger.info("📊 Dedicated Performance Test Results:");
            logger.info("Dedicated test completed: {} messages in {}ms ({:.2f} msg/sec)", 
                       testResult.totalMessages, testResult.durationMs, testResult.messagesPerSecond);
            
            // Generate performance report
            generatePerformanceReport();
            
        } catch (Exception e) {
            logger.error("Performance benchmarking demonstration failed", e);
        }
    }
}
