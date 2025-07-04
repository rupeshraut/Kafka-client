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
            
            // Run all basic examples with health monitoring
            basicProducerExample(client);
            basicConsumerExample(client);
            asyncProducerExample(client);
            reactiveProducerExample(client);
            reactiveConsumerExample(client);
            
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
        logger.info("Messages - Success: {}, Failed: {}", 
                   (long)successfulMessages.count(), (long)failedMessages.count());
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
     * Process a consumed message
     */
    private static void processMessage(ConsumerRecord<String, String> record) {
        logger.info("Processing message: {}", record.value());
        
        // Simulate processing time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Track success metrics
        successfulMessages.increment();
        lastSuccessfulSend.set(System.currentTimeMillis());
        
        logger.info("Message processed successfully: {}", record.key());
    }
    
    /**
     * Process a reactive message (generic version)
     */
    private static void processReactiveMessageGeneric(ConsumerRecord<Object, Object> record) {
        logger.info("Processing reactive message: key={}, value={}", record.key(), record.value());
        
        // Track processing metrics
        successfulMessages.increment();
        lastSuccessfulSend.set(System.currentTimeMillis());
        
        // Add your reactive processing logic here
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
