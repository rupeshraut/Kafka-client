package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Example demonstrating resilience patterns in the Kafka Multi-Datacenter Client.
 * Shows circuit breaker, retry mechanisms, health-aware routing, and fallback strategies.
 */
public class ResilienceExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ResilienceExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Resilience Example ===");
        
        try {
            // Create configuration with resilience patterns
            KafkaDatacenterConfiguration config = createResilienceConfiguration();
            
            // Build the client with resilience features enabled
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Resilient client created successfully");
                
                // Demonstrate health-aware routing
                demonstrateHealthAwareRouting(client);
                
                // Demonstrate automatic failover
                demonstrateAutomaticFailover(client);
                
                // Demonstrate circuit breaker simulation
                demonstrateCircuitBreakerBehavior(client);
                
                // Demonstrate retry mechanisms
                demonstrateRetryMechanisms(client);
                
                logger.info("Resilience patterns example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Resilience example failed", e);
        }
    }
    
    /**
     * Create configuration with resilience patterns enabled.
     */
    private static KafkaDatacenterConfiguration createResilienceConfiguration() {
        logger.info("Creating resilience configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                // Primary datacenter with higher priority
                KafkaDatacenterEndpoint.builder()
                    .id("resilient-primary")
                    .region("us-east-1")
                    .bootstrapServers("kafka-primary-1.company.com:9092,kafka-primary-2.company.com:9092")
                    .priority(1)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(20)
                    .minConnections(5)
                    .build(),
                // Secondary datacenter for failover
                KafkaDatacenterEndpoint.builder()
                    .id("resilient-secondary")
                    .region("us-west-2")
                    .bootstrapServers("kafka-secondary-1.company.com:9092,kafka-secondary-2.company.com:9092")
                    .priority(2)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(15)
                    .minConnections(3)
                    .build(),
                // Tertiary datacenter for disaster recovery
                KafkaDatacenterEndpoint.builder()
                    .id("resilient-tertiary")
                    .region("eu-west-1")
                    .bootstrapServers("kafka-tertiary-1.company.com:9092,kafka-tertiary-2.company.com:9092")
                    .priority(3)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(45))
                    .maxConnections(10)
                    .minConnections(2)
                    .build()
            ))
            .localDatacenter("resilient-primary")
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)
            .healthCheckInterval(Duration.ofSeconds(15))
            .connectionTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofMinutes(1))
            .enableMetrics(true)
            .metricsPrefix("resilience.kafka.multidc")
            .build();
    }
    
    /**
     * Demonstrate health-aware routing that automatically routes to healthy datacenters.
     */
    private static void demonstrateHealthAwareRouting(KafkaMultiDatacenterClient client) {
        logger.info("=== Health-Aware Routing Demo ===");
        
        try {
            // Monitor current datacenter health
            var healthFuture = client.checkDatacenterHealth();
            var datacenters = healthFuture.get();
            
            logger.info("Current datacenter health status:");
            datacenters.forEach(dc -> 
                logger.info("  {} ({}): Priority {}, Healthy: {}", 
                           dc.getId(), dc.getRegion(), dc.getPriority(), dc.isHealthy()));
            
            // Send messages with health-aware routing
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "resilience-health-test",
                    "health-key-" + i,
                    "{\"messageId\":" + i + ",\"test\":\"health-aware-routing\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("Message {} routed to partition {} at offset {} (health-aware)", 
                           i, metadata.partition(), metadata.offset());
            }
            
            logger.info("✅ Health-aware routing completed - messages automatically routed to healthy datacenters");
            
        } catch (Exception e) {
            logger.error("❌ Health-aware routing demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate automatic failover when primary datacenter becomes unavailable.
     */
    private static void demonstrateAutomaticFailover(KafkaMultiDatacenterClient client) {
        logger.info("=== Automatic Failover Demo ===");
        
        try {
            // Subscribe to health changes to monitor failover
            client.subscribeToHealthChanges((datacenter, healthy) -> {
                logger.info("🔔 Failover Alert: Datacenter {} is now {} - automatic routing adjustment", 
                           datacenter.getId(), healthy ? "HEALTHY" : "UNHEALTHY");
            });
            
            // Send messages with automatic failover capability
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "resilience-failover-test",
                    "failover-key-" + i,
                    "{\"messageId\":" + i + ",\"test\":\"automatic-failover\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                try {
                    RecordMetadata metadata = client.producerSync().send(record);
                    logger.info("Failover message {} sent to partition {} at offset {}", 
                               i, metadata.partition(), metadata.offset());
                } catch (Exception e) {
                    logger.warn("Failover message {} failed, client will retry to other datacenters: {}", 
                               i, e.getMessage());
                }
                
                // Add some delay to simulate real-world timing
                Thread.sleep(100);
            }
            
            logger.info("✅ Automatic failover demonstration completed");
            
        } catch (Exception e) {
            logger.error("❌ Automatic failover demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate circuit breaker behavior under failure conditions.
     */
    private static void demonstrateCircuitBreakerBehavior(KafkaMultiDatacenterClient client) {
        logger.info("=== Circuit Breaker Behavior Demo ===");
        
        try {
            logger.info("Simulating high failure rate to trigger circuit breaker...");
            
            // Send messages that might trigger circuit breaker
            for (int i = 0; i < 20; i++) {
                final int messageId = i; // Make effectively final for lambda usage
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "resilience-circuit-test",
                    "circuit-key-" + messageId,
                    "{\"messageId\":" + messageId + ",\"test\":\"circuit-breaker\",\"highLoad\":true,\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                try {
                    // Use async to not block on failures
                    CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
                    
                    future.thenAccept(metadata -> 
                        logger.info("Circuit test message {} succeeded: partition {} offset {}", 
                                   messageId, metadata.partition(), metadata.offset()))
                          .exceptionally(error -> {
                              logger.warn("Circuit test message {} failed (may trigger circuit breaker): {}", 
                                         messageId, error.getMessage());
                              return null;
                          });
                    
                    // Add slight delay to simulate load
                    Thread.sleep(50);
                    
                } catch (Exception e) {
                    logger.warn("Circuit breaker may be OPEN for message {}: {}", i, e.getMessage());
                }
            }
            
            // Wait for async operations to complete
            Thread.sleep(2000);
            
            // Test circuit breaker recovery
            logger.info("Testing circuit breaker recovery after failure period...");
            Thread.sleep(5000); // Wait for potential circuit breaker recovery
            
            ProducerRecord<String, String> recoveryRecord = new ProducerRecord<>(
                "resilience-circuit-recovery",
                "recovery-key",
                "{\"test\":\"circuit-recovery\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
            );
            
            RecordMetadata recoveryMetadata = client.producerSync().send(recoveryRecord);
            logger.info("✅ Circuit breaker recovery successful: partition {} offset {}", 
                       recoveryMetadata.partition(), recoveryMetadata.offset());
            
        } catch (Exception e) {
            logger.error("❌ Circuit breaker demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate retry mechanisms with exponential backoff.
     */
    private static void demonstrateRetryMechanisms(KafkaMultiDatacenterClient client) {
        logger.info("=== Retry Mechanisms Demo ===");
        
        try {
            // Send messages that demonstrate retry behavior
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "resilience-retry-test",
                    "retry-key-" + i,
                    "{\"messageId\":" + i + ",\"test\":\"retry-mechanisms\",\"retryable\":true,\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                try {
                    // Send with timeout to demonstrate retry on timeout
                    RecordMetadata metadata = client.producerSync().send(record, Duration.ofSeconds(5));
                    logger.info("Retry test message {} sent successfully: partition {} offset {}", 
                               i, metadata.partition(), metadata.offset());
                    
                } catch (Exception e) {
                    logger.warn("Retry test message {} failed after retries: {}", i, e.getMessage());
                }
                
                // Space out the messages
                Thread.sleep(200);
            }
            
            // Demonstrate batch retry
            logger.info("Testing batch retry mechanisms...");
            
            List<ProducerRecord<String, String>> batchRecords = List.of(
                new ProducerRecord<>("resilience-batch-retry", "batch-1", "{\"batch\":1,\"test\":\"batch-retry\"}"),
                new ProducerRecord<>("resilience-batch-retry", "batch-2", "{\"batch\":2,\"test\":\"batch-retry\"}"),
                new ProducerRecord<>("resilience-batch-retry", "batch-3", "{\"batch\":3,\"test\":\"batch-retry\"}")
            );
            
            try {
                List<RecordMetadata> batchResults = client.producerSync().sendBatch(batchRecords);
                logger.info("✅ Batch retry test successful: {} messages sent", batchResults.size());
                batchResults.forEach(metadata -> 
                    logger.info("  Batch message sent to partition {} at offset {}", 
                               metadata.partition(), metadata.offset()));
            } catch (Exception e) {
                logger.warn("Batch retry test failed after retries: {}", e.getMessage());
            }
            
            logger.info("✅ Retry mechanisms demonstration completed");
            
        } catch (Exception e) {
            logger.error("❌ Retry mechanisms demonstration failed", e);
        }
    }
}
