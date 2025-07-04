package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive example demonstrating producer and consumer partitioning strategies
 * with the Kafka Multi-Datacenter Client.
 */
public class PartitioningStrategiesExample {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitioningStrategiesExample.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private static final AtomicInteger messageCounter = new AtomicInteger(0);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Partitioning Strategies Example ===");
        
        try {
            // Create configuration for partitioning demonstration
            KafkaDatacenterConfiguration config = createPartitioningConfiguration();
            
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Partitioning client created successfully");
                
                // Demonstrate producer partitioning strategies
                demonstrateProducerPartitioning(client);
                
                // Wait for messages to be produced
                Thread.sleep(5000);
                
                // Demonstrate consumer partitioning strategies
                demonstrateConsumerPartitioning(client);
                
                // Run advanced partitioning scenarios
                demonstrateAdvancedPartitioning(client);
                
                // Wait for demonstration to complete
                Thread.sleep(15000);
                
                logger.info("Partitioning strategies example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Partitioning example failed", e);
        } finally {
            scheduler.shutdown();
        }
    }
    
    /**
     * Create configuration optimized for partitioning demonstrations.
     */
    private static KafkaDatacenterConfiguration createPartitioningConfiguration() {
        logger.info("Creating partitioning configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("partition-east")
                    .region("us-east-1")
                    .bootstrapServers("kafka-east-1.company.com:9092,kafka-east-2.company.com:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(25)
                    .minConnections(5)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("partition-west")
                    .region("us-west-2")
                    .bootstrapServers("kafka-west-1.company.com:9092,kafka-west-2.company.com:9092")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(20)
                    .minConnections(3)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("partition-eu")
                    .region("eu-central-1")
                    .bootstrapServers("kafka-eu-1.company.com:9092,kafka-eu-2.company.com:9092")
                    .priority(3)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(15))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(15)
                    .minConnections(2)
                    .build()
            ))
            .localDatacenter("partition-east")
            .routingStrategy(RoutingStrategy.PRIMARY_PREFERRED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(15))
            .requestTimeout(Duration.ofMinutes(2))
            .enableMetrics(true)
            .metricsPrefix("partitioning.kafka.multidc")
            .build();
    }
    
    /**
     * Demonstrate various producer partitioning strategies.
     */
    private static void demonstrateProducerPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("=== Producer Partitioning Strategies Demo ===");
        
        try {
            // 1. Round-Robin Partitioning
            demonstrateRoundRobinPartitioning(client);
            
            // 2. Key-Hash Partitioning
            demonstrateKeyHashPartitioning(client);
            
            // 3. Geographic Partitioning
            demonstrateGeographicPartitioning(client);
            
            // 4. Time-Based Partitioning
            demonstrateTimeBasedPartitioning(client);
            
            // 5. Sticky Partitioning
            demonstrateStickyPartitioning(client);
            
            // 6. Custom Partitioning
            demonstrateCustomPartitioning(client);
            
        } catch (Exception e) {
            logger.error("Producer partitioning demonstration failed", e);
        }
    }
    
    private static void demonstrateRoundRobinPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("🔄 Round-Robin Partitioning Strategy");
        
        try {
            // Note: In a real implementation, this would be configured on the producer
            // For demonstration, we'll simulate the behavior
            
            for (int i = 0; i < 6; i++) {
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-round-robin",
                    null, // No key for round-robin
                    Map.of(
                        "messageId", "rr-" + i,
                        "strategy", "round-robin",
                        "timestamp", Instant.now().toString(),
                        "expectedPartition", i % 3 // Assuming 3 partitions
                    )
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Round-robin message {} sent to partition {} (expected rotation)", 
                           i, metadata.partition());
            }
            
        } catch (Exception e) {
            logger.error("Round-robin partitioning failed", e);
        }
    }
    
    private static void demonstrateKeyHashPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("🔑 Key-Hash Partitioning Strategy");
        
        try {
            String[] userIds = {"user-alice", "user-bob", "user-charlie", "user-diana", "user-eve"};
            
            for (String userId : userIds) {
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-key-hash",
                    userId, // Key for consistent hashing
                    Map.of(
                        "userId", userId,
                        "strategy", "key-hash",
                        "action", "login",
                        "timestamp", Instant.now().toString()
                    )
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Key-hash message for {} sent to partition {} (consistent)", 
                           userId, metadata.partition());
            }
            
            // Send duplicate keys to verify consistency
            logger.info("Verifying key-hash consistency...");
            for (String userId : Arrays.copyOf(userIds, 3)) {
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-key-hash",
                    userId,
                    Map.of(
                        "userId", userId,
                        "strategy", "key-hash",
                        "action", "logout",
                        "timestamp", Instant.now().toString()
                    )
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("🔄 Consistency check: {} again sent to partition {}", 
                           userId, metadata.partition());
            }
            
        } catch (Exception e) {
            logger.error("Key-hash partitioning failed", e);
        }
    }
    
    private static void demonstrateGeographicPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("🌍 Geographic Partitioning Strategy");
        
        try {
            String[] regions = {"us-east", "us-west", "eu", "asia"};
            
            for (int i = 0; i < regions.length; i++) {
                String region = regions[i];
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-geographic",
                    region + "-user-" + i,
                    Map.of(
                        "region", region,
                        "strategy", "geographic",
                        "orderId", "order-" + (1000 + i),
                        "timestamp", Instant.now().toString()
                    )
                );
                
                // Add region header for geographic partitioning
                record.headers().add(new RecordHeader("region", region.getBytes(StandardCharsets.UTF_8)));
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Geographic message from {} sent to partition {} (region-aware)", 
                           region, metadata.partition());
            }
            
        } catch (Exception e) {
            logger.error("Geographic partitioning failed", e);
        }
    }
    
    private static void demonstrateTimeBasedPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("⏰ Time-Based Partitioning Strategy");
        
        try {
            // Simulate messages from different time windows
            long baseTime = System.currentTimeMillis();
            long[] timeOffsets = {0, 3600000, 7200000, 10800000}; // 0, 1h, 2h, 3h
            
            for (int i = 0; i < timeOffsets.length; i++) {
                long timestamp = baseTime + timeOffsets[i];
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-time-based",
                    null, // partition - let partitioner decide
                    timestamp, // timestamp
                    "time-" + i, // key
                    Map.of( // value
                        "eventId", "event-" + i,
                        "strategy", "time-based",
                        "eventTime", Instant.ofEpochMilli(timestamp).toString(),
                        "timeWindow", i
                    )
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Time-based message (window {}) sent to partition {} at time {}", 
                           i, metadata.partition(), Instant.ofEpochMilli(timestamp));
            }
            
        } catch (Exception e) {
            logger.error("Time-based partitioning failed", e);
        }
    }
    
    private static void demonstrateStickyPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("📌 Sticky Partitioning Strategy");
        
        try {
            // Simulate sticky partitioning behavior
            logger.info("Sending batch of messages with sticky partitioning...");
            
            for (int batch = 0; batch < 3; batch++) {
                logger.info("Batch {} (sticky to same partition until batch complete):", batch + 1);
                
                for (int i = 0; i < 4; i++) {
                    ProducerRecord<String, Object> record = new ProducerRecord<>(
                        "partitioning-sticky",
                        null, // No key for sticky behavior
                        Map.of(
                            "batchId", batch,
                            "messageId", i,
                            "strategy", "sticky",
                            "timestamp", Instant.now().toString()
                        )
                    );
                    
                    RecordMetadata metadata = client.producerSync().send(record);
                    logger.info("  📌 Sticky batch {} message {} sent to partition {}", 
                               batch + 1, i, metadata.partition());
                }
                
                // Small delay between batches to demonstrate stickiness
                Thread.sleep(100);
            }
            
        } catch (Exception e) {
            logger.error("Sticky partitioning failed", e);
        }
    }
    
    private static void demonstrateCustomPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("🎯 Custom Partitioning Strategy");
        
        try {
            // Demonstrate custom business logic partitioning
            String[] customerTiers = {"premium", "standard", "basic"};
            
            for (int i = 0; i < customerTiers.length; i++) {
                String tier = customerTiers[i];
                
                // Custom logic: premium -> partition 0, standard -> partition 1, basic -> partition 2
                Integer customPartition = switch (tier) {
                    case "premium" -> 0;
                    case "standard" -> 1;
                    case "basic" -> 2;
                    default -> null;
                };
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-custom",
                    customPartition, // Explicit partition
                    "customer-" + i,
                    Map.of(
                        "customerId", "customer-" + i,
                        "tier", tier,
                        "strategy", "custom",
                        "businessPriority", tier.equals("premium") ? "high" : "normal",
                        "timestamp", Instant.now().toString()
                    )
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Custom partitioned {} customer sent to partition {} (business logic)", 
                           tier, metadata.partition());
            }
            
        } catch (Exception e) {
            logger.error("Custom partitioning failed", e);
        }
    }
    
    /**
     * Demonstrate consumer partitioning strategies.
     */
    private static void demonstrateConsumerPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("=== Consumer Partitioning Strategies Demo ===");
        
        try {
            // Demonstrate different consumer assignment strategies
            demonstrateConsumerAssignmentStrategies(client);
            
            // Demonstrate partition rebalancing
            demonstratePartitionRebalancing(client);
            
        } catch (Exception e) {
            logger.error("Consumer partitioning demonstration failed", e);
        }
    }
    
    private static void demonstrateConsumerAssignmentStrategies(KafkaMultiDatacenterClient client) {
        logger.info("📋 Consumer Partition Assignment Strategies");
        
        try {
            // Create multiple consumer threads to demonstrate assignment
            List<Thread> consumerThreads = new ArrayList<>();
            
            for (int i = 0; i < 3; i++) {
                final int consumerIndex = i;
                Thread consumerThread = new Thread(() -> {
                    try {
                        String consumerGroup = "partitioning-demo-group";
                        
                        // Subscribe to partitioned topics
                        client.consumerSync().subscribe(List.of(
                            "partitioning-round-robin",
                            "partitioning-key-hash",
                            "partitioning-geographic"
                        ));
                        
                        logger.info("🔍 Consumer {} joined group {}", consumerIndex, consumerGroup);
                        
                        // Poll for messages and log partition assignments
                        for (int poll = 0; poll < 3; poll++) {
                            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(2));
                            
                            if (records.count() > 0) {
                                Set<Integer> assignedPartitions = new HashSet<>();
                                
                                for (ConsumerRecord<String, Object> record : records) {
                                    assignedPartitions.add(record.partition());
                                    
                                    if (record.value() instanceof Map) {
                                        @SuppressWarnings("unchecked")
                                        Map<String, Object> data = (Map<String, Object>) record.value();
                                        logger.info("📨 Consumer {} consumed: topic={}, partition={}, strategy={}", 
                                                   consumerIndex, record.topic(), record.partition(), 
                                                   data.get("strategy"));
                                    }
                                }
                                
                                logger.info("📊 Consumer {} assigned partitions: {}", 
                                           consumerIndex, assignedPartitions);
                            }
                            
                            client.consumerSync().commitSync();
                        }
                        
                    } catch (Exception e) {
                        logger.error("Consumer {} failed", consumerIndex, e);
                    }
                });
                
                consumerThread.setName("PartitionConsumer-" + i);
                consumerThread.setDaemon(true);
                consumerThreads.add(consumerThread);
                consumerThread.start();
            }
            
            // Let consumers run for a while
            Thread.sleep(8000);
            
            logger.info("✅ Consumer partition assignment demonstration completed");
            
        } catch (Exception e) {
            logger.error("Consumer assignment strategies failed", e);
        }
    }
    
    private static void demonstratePartitionRebalancing(KafkaMultiDatacenterClient client) {
        logger.info("⚖️ Partition Rebalancing Demonstration");
        
        try {
            logger.info("Simulating partition rebalancing scenarios...");
            
            // This would demonstrate:
            // 1. Consumer joining/leaving group
            // 2. Partition reassignment
            // 3. Sticky assignment behavior
            // 4. Datacenter-aware assignment
            
            logger.info("Rebalancing scenarios:");
            logger.info("1. 🔄 Consumer group expansion - new consumers join");
            logger.info("2. ⚡ Consumer failure - partitions reassigned");
            logger.info("3. 📍 Datacenter-aware assignment - locality preference");
            logger.info("4. 📌 Sticky assignment - minimize partition movement");
            
            // Note: Full implementation would require coordination with actual Kafka consumer groups
            // This is a conceptual demonstration
            
        } catch (Exception e) {
            logger.error("Partition rebalancing demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate advanced partitioning scenarios.
     */
    private static void demonstrateAdvancedPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("=== Advanced Partitioning Scenarios Demo ===");
        
        try {
            // 1. Multi-datacenter partition affinity
            demonstrateDatacenterPartitionAffinity(client);
            
            // 2. Dynamic partition management
            demonstrateDynamicPartitioning(client);
            
            // 3. Performance-optimized partitioning
            demonstratePerformancePartitioning(client);
            
        } catch (Exception e) {
            logger.error("Advanced partitioning demonstration failed", e);
        }
    }
    
    private static void demonstrateDatacenterPartitionAffinity(KafkaMultiDatacenterClient client) {
        logger.info("🌐 Datacenter Partition Affinity");
        
        try {
            Map<String, String> datacenterMapping = Map.of(
                "us-east", "partition-east",
                "us-west", "partition-west", 
                "eu", "partition-eu"
            );
            
            for (Map.Entry<String, String> entry : datacenterMapping.entrySet()) {
                String region = entry.getKey();
                String datacenter = entry.getValue();
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "partitioning-datacenter-affinity",
                    region + "-affinity-key",
                    Map.of(
                        "region", region,
                        "datacenter", datacenter,
                        "strategy", "datacenter-affinity",
                        "orderData", "region-specific-order-" + messageCounter.incrementAndGet(),
                        "timestamp", Instant.now().toString()
                    )
                );
                
                // Add datacenter preference header
                record.headers().add(new RecordHeader("preferred-datacenter", datacenter.getBytes(StandardCharsets.UTF_8)));
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("✅ Datacenter affinity message for {} sent to partition {} (datacenter: {})", 
                           region, metadata.partition(), datacenter);
            }
            
        } catch (Exception e) {
            logger.error("Datacenter partition affinity failed", e);
        }
    }
    
    private static void demonstrateDynamicPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("🔄 Dynamic Partitioning Management");
        
        try {
            // Simulate dynamic partitioning based on load
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    int loadLevel = (int) (Math.random() * 100);
                    String partitionStrategy = loadLevel > 80 ? "load-balanced" : 
                                             loadLevel > 50 ? "round-robin" : "sticky";
                    
                    ProducerRecord<String, Object> record = new ProducerRecord<>(
                        "partitioning-dynamic",
                        "dynamic-" + System.currentTimeMillis(),
                        Map.of(
                            "loadLevel", loadLevel,
                            "selectedStrategy", partitionStrategy,
                            "strategy", "dynamic",
                            "adaptiveRouting", true,
                            "timestamp", Instant.now().toString()
                        )
                    );
                    
                    RecordMetadata metadata = client.producerSync().send(record);
                    logger.info("⚡ Dynamic partitioning: load={}%, strategy={}, partition={}", 
                               loadLevel, partitionStrategy, metadata.partition());
                    
                } catch (Exception e) {
                    logger.warn("Dynamic partitioning iteration failed: {}", e.getMessage());
                }
            }, 0, 2, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            logger.error("Dynamic partitioning failed", e);
        }
    }
    
    private static void demonstratePerformancePartitioning(KafkaMultiDatacenterClient client) {
        logger.info("⚡ Performance-Optimized Partitioning");
        
        try {
            // Demonstrate high-throughput partitioning
            CompletableFuture<Void> performanceTest = CompletableFuture.runAsync(() -> {
                try {
                    logger.info("Starting high-throughput partitioning test...");
                    
                    long startTime = System.currentTimeMillis();
                    int messageCount = 100;
                    
                    for (int i = 0; i < messageCount; i++) {
                        final int messageIndex = i; // Make effectively final for lambda
                        ProducerRecord<String, Object> record = new ProducerRecord<>(
                            "partitioning-performance",
                            "perf-key-" + (messageIndex % 10), // Limited key space for batching
                            Map.of(
                                "messageId", messageIndex,
                                "strategy", "performance-optimized",
                                "batchOptimized", true,
                                "timestamp", Instant.now().toString()
                            )
                        );
                        
                        client.producerAsync().sendAsync(record)
                            .thenAccept(metadata -> {
                                if (messageIndex % 20 == 0) {
                                    logger.debug("Performance message {} sent to partition {}", 
                                                messageIndex, metadata.partition());
                                }
                            });
                    }
                    
                    long endTime = System.currentTimeMillis();
                    double throughput = (messageCount * 1000.0) / (endTime - startTime);
                    
                    logger.info("✅ Performance test completed: {} messages in {}ms ({:.2f} msg/sec)", 
                               messageCount, endTime - startTime, throughput);
                    
                } catch (Exception e) {
                    logger.error("Performance partitioning test failed", e);
                }
            });
            
            // Wait for completion
            performanceTest.get(10, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            logger.error("Performance partitioning failed", e);
        }
    }
}
