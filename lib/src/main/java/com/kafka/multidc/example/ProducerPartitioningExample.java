package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.partitioning.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive example demonstrating producer partitioning strategies
 * with the Kafka Multi-Datacenter Client.
 * 
 * This example shows how to:
 * 1. Configure and use built-in partitioning strategies
 * 2. Implement custom partitioning logic
 * 3. Use the ProducerPartitioningManager API
 * 4. Monitor partitioning behavior and statistics
 */
public class ProducerPartitioningExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerPartitioningExample.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Producer Partitioning Strategies Example ===");
        
        try {
            // Create configuration for partitioning demonstration
            KafkaDatacenterConfiguration config = createPartitioningConfiguration();
            
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Multi-datacenter client created successfully");
                
                // 1. Demonstrate built-in partitioning strategies
                demonstrateBuiltInStrategies();
                
                // 2. Demonstrate custom partitioning
                demonstrateCustomPartitioning();
                
                // 3. Demonstrate partitioning with geographic headers
                demonstrateGeographicPartitioning(client);
                
                // 4. Demonstrate time-based partitioning
                demonstrateTimeBasedPartitioning(client);
                
                // 5. Demonstrate partitioning statistics and monitoring
                demonstratePartitioningMonitoring();
                
                logger.info("Producer partitioning strategies example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Producer partitioning example failed", e);
        } finally {
            scheduler.shutdown();
        }
    }
    
    /**
     * Create configuration optimized for partitioning demonstrations.
     */
    private static KafkaDatacenterConfiguration createPartitioningConfiguration() {
        // Create multi-datacenter configuration
        KafkaDatacenterEndpoint primaryEndpoint = KafkaDatacenterEndpoint.builder()
            .id("us-east-1")
            .region("us-east-1")
            .bootstrapServers("localhost:9092")
            .build();
        
        KafkaDatacenterEndpoint secondaryEndpoint = KafkaDatacenterEndpoint.builder()
            .id("us-west-2")
            .region("us-west-2")
            .bootstrapServers("localhost:9093")
            .build();
        
        // Configure producer and consumer (using default configs)
        ProducerConfig producerConfig = ProducerConfig.defaultConfig();
        ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(primaryEndpoint)
            .addDatacenter(secondaryEndpoint)
            .localDatacenter("us-east-1")
            .producerConfig(producerConfig)
            .consumerConfig(consumerConfig)
            .build();
    }
    
    /**
     * Demonstrate built-in partitioning strategies with ProducerPartitioningManager.
     */
    private static void demonstrateBuiltInStrategies() {
        logger.info("\n=== Built-in Partitioning Strategies ===");
        
        // Create ProducerPartitioningManager
        ProducerPartitioningManager partitioningManager = new ProducerPartitioningManager();
        
        // Demonstrate each built-in strategy
        demonstrateRoundRobinStrategy(partitioningManager);
        demonstrateKeyHashStrategy(partitioningManager);
        demonstrateModulusStrategy(partitioningManager);
        demonstrateRandomStrategy(partitioningManager);
        demonstrateStickyStrategy(partitioningManager);
        demonstrateLoadBalancedStrategy(partitioningManager);
    }
    
    private static void demonstrateRoundRobinStrategy(ProducerPartitioningManager manager) {
        logger.info("🔄 Round-Robin Partitioning Strategy");
        
        // Configure round-robin strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.ROUND_ROBIN);
        
        // Simulate partition determination for multiple messages
        int numPartitions = 3;
        for (int i = 0; i < 9; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                null, // No key for round-robin
                Map.of("messageId", "rr-" + i, "index", i)
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Message {} -> Partition {}", i, partition);
        }
        
        // Show statistics
        Map<String, Object> stats = manager.getPartitioningStats();
        logger.info("Strategy: {}, Statistics: {}", manager.getCurrentStrategyName(), stats);
    }
    
    private static void demonstrateKeyHashStrategy(ProducerPartitioningManager manager) {
        logger.info("🔑 Key-Hash Partitioning Strategy");
        
        // Configure key-hash strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.KEY_HASH);
        
        // Simulate partition determination for messages with keys
        int numPartitions = 3;
        String[] keys = {"user1", "user2", "user3", "user1", "user2", "user4"};
        
        for (int i = 0; i < keys.length; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                keys[i],
                Map.of("messageId", "kh-" + i, "userId", keys[i])
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Key '{}' -> Partition {} (consistent: {})", 
                      keys[i], partition, "consistent hashing ensures same key -> same partition");
        }
    }
    
    private static void demonstrateModulusStrategy(ProducerPartitioningManager manager) {
        logger.info("⚡ Modulus Partitioning Strategy");
        
        // Configure modulus strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.MODULUS);
        
        // Simulate partition determination for messages with keys
        int numPartitions = 3;
        String[] keys = {"user1", "user2", "user3", "user1", "user2", "user4", "product-100", "product-200"};
        
        for (int i = 0; i < keys.length; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                keys[i],
                Map.of("messageId", "mod-" + i, "keyId", keys[i])
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            int expectedPartition = Math.abs(keys[i].hashCode()) % numPartitions;
            logger.info("Key '{}' (hash: {}) -> Partition {} (expected: {})", 
                      keys[i], keys[i].hashCode(), partition, expectedPartition);
        }
        
        // Show consistency by re-testing the same keys
        logger.info("🔄 Verifying consistency - re-testing same keys:");
        for (String key : Arrays.asList("user1", "user2", "product-100")) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic", key, Map.of("retestKey", key)
            );
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Key '{}' -> Partition {} (should be consistent)", key, partition);
        }
    }
    
    private static void demonstrateRandomStrategy(ProducerPartitioningManager manager) {
        logger.info("🎲 Random Partitioning Strategy");
        
        // Configure random strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.RANDOM);
        
        // Track partition distribution
        Map<Integer, Integer> partitionCounts = new HashMap<>();
        int numPartitions = 3;
        int numMessages = 15;
        
        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                "random-key-" + i,
                Map.of("messageId", "rand-" + i)
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            partitionCounts.merge(partition, 1, Integer::sum);
        }
        
        logger.info("Random distribution across {} messages:", numMessages);
        partitionCounts.forEach((partition, count) -> 
            logger.info("Partition {}: {} messages ({:.1f}%)", 
                      partition, count, (count * 100.0) / numMessages));
    }
    
    private static void demonstrateStickyStrategy(ProducerPartitioningManager manager) {
        logger.info("📌 Sticky Partitioning Strategy");
        
        // Configure sticky strategy with custom batch threshold
        Map<String, Object> config = Map.of("batch.threshold", 5);
        manager.setPartitioningStrategy(ProducerPartitioningType.STICKY, config);
        
        // Simulate batch processing
        int numPartitions = 3;
        for (int i = 0; i < 12; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                null,
                Map.of("messageId", "sticky-" + i, "batchItem", i)
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Message {} -> Partition {} (sticky until batch full)", i, partition);
        }
    }
    
    private static void demonstrateLoadBalancedStrategy(ProducerPartitioningManager manager) {
        logger.info("⚖️ Load-Balanced Partitioning Strategy");
        
        // Configure load-balanced strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.LOAD_BALANCED);
        
        // Simulate load-aware partitioning
        int numPartitions = 4;
        for (int i = 0; i < 8; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic",
                "lb-key-" + i,
                Map.of("messageId", "lb-" + i, "priority", i % 3)
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Message {} -> Partition {} (load-balanced)", i, partition);
        }
    }
    
    /**
     * Demonstrate custom partitioning strategies.
     */
    private static void demonstrateCustomPartitioning() {
        logger.info("\n=== Custom Partitioning Strategies ===");
        
        ProducerPartitioningManager manager = new ProducerPartitioningManager();
        
        // 1. Business Logic Partitioning
        demonstrateBusinessLogicPartitioning(manager);
        
        // 2. Multi-Tenant Partitioning
        demonstrateMultiTenantPartitioning(manager);
    }
    
    private static void demonstrateBusinessLogicPartitioning(ProducerPartitioningManager manager) {
        logger.info("🏢 Business Logic Partitioning");
        
        // Create a custom strategy based on message priority
        ProducerPartitioningStrategy<String, Object> priorityStrategy = 
            new ProducerPartitioningStrategy<String, Object>() {
                @Override
                public Integer partition(ProducerRecord<String, Object> record, int numPartitions) {
                    if (record.value() instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> value = (Map<String, Object>) record.value();
                        Object priority = value.get("priority");
                        
                        if (priority instanceof String) {
                            switch ((String) priority) {
                                case "HIGH":
                                    return 0; // High priority messages to partition 0
                                case "MEDIUM":
                                    return 1; // Medium priority to partition 1
                                case "LOW":
                                    return numPartitions - 1; // Low priority to last partition
                                default:
                                    return null; // Use default partitioning
                            }
                        }
                    }
                    return null;
                }
                
                @Override
                public String getStrategyName() {
                    return "priority-based";
                }
            };
        
        // Register and use custom strategy
        manager.registerCustomStrategy("priority-based", priorityStrategy);
        manager.setCustomPartitioningStrategy("priority-based");
        
        // Test with different priority messages
        String[] priorities = {"HIGH", "MEDIUM", "LOW", "HIGH", "MEDIUM"};
        int numPartitions = 3;
        
        for (int i = 0; i < priorities.length; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "business-topic",
                "msg-" + i,
                Map.of("messageId", i, "priority", priorities[i], "content", "Business data")
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Priority '{}' message -> Partition {}", priorities[i], partition);
        }
    }
    
    private static void demonstrateMultiTenantPartitioning(ProducerPartitioningManager manager) {
        logger.info("🏢 Multi-Tenant Partitioning");
        
        // Create tenant-based partitioning strategy
        ProducerPartitioningStrategy<String, Object> tenantStrategy = 
            new ProducerPartitioningStrategy<String, Object>() {
                private final Map<String, Integer> tenantPartitions = new HashMap<>();
                
                @Override
                public Integer partition(ProducerRecord<String, Object> record, int numPartitions) {
                    if (record.key() != null && record.key().startsWith("tenant-")) {
                        String tenantId = record.key();
                        
                        // Assign tenant to partition based on hash, but keep it consistent
                        return tenantPartitions.computeIfAbsent(tenantId, 
                            key -> Math.abs(key.hashCode()) % numPartitions);
                    }
                    return null;
                }
                
                @Override
                public String getStrategyName() {
                    return "multi-tenant";
                }
            };
        
        manager.registerCustomStrategy("multi-tenant", tenantStrategy);
        manager.setCustomPartitioningStrategy("multi-tenant");
        
        // Test with different tenants
        String[] tenants = {"tenant-alpha", "tenant-beta", "tenant-gamma", "tenant-alpha", "tenant-delta"};
        int numPartitions = 4;
        
        for (int i = 0; i < tenants.length; i++) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "multi-tenant-topic",
                tenants[i],
                Map.of("messageId", i, "tenantData", "Data for " + tenants[i])
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            logger.info("Tenant '{}' -> Partition {} (tenant isolation)", tenants[i], partition);
        }
    }
    
    /**
     * Demonstrate geographic partitioning using message headers.
     */
    private static void demonstrateGeographicPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("\n=== Geographic Partitioning ===");
        
        try {
            // Create messages with geographic headers
            List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
            
            String[] regions = {"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"};
            
            for (int i = 0; i < 8; i++) {
                String region = regions[i % regions.length];
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "geographic-topic",
                    null,
                    Map.of(
                        "messageId", "geo-" + i,
                        "region", region,
                        "timestamp", Instant.now().toString(),
                        "data", "Geographic data from " + region
                    )
                );
                
                // Add geographic header for partitioning
                record.headers().add(new RecordHeader("region", region.getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("datacenter", region.getBytes(StandardCharsets.UTF_8)));
                
                CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
                futures.add(future);
                
                logger.info("Sending message from region '{}' with geographic headers", region);
            }
            
            // Wait for all messages to be sent
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    logger.info("All geographic messages sent successfully");
                    futures.forEach(future -> {
                        try {
                            RecordMetadata metadata = future.get();
                            logger.info("Geographic message sent to partition {}, offset {}", 
                                      metadata.partition(), metadata.offset());
                        } catch (Exception e) {
                            logger.error("Failed to get metadata", e);
                        }
                    });
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to send geographic messages", throwable);
                    return null;
                });
            
            Thread.sleep(2000); // Wait for async operations
            
        } catch (Exception e) {
            logger.error("Geographic partitioning demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate time-based partitioning.
     */
    private static void demonstrateTimeBasedPartitioning(KafkaMultiDatacenterClient client) {
        logger.info("\n=== Time-Based Partitioning ===");
        
        try {
            // Create messages with different timestamps
            List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
            
            Instant baseTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
            
            for (int i = 0; i < 6; i++) {
                Instant messageTime = baseTime.plus(i * 15, ChronoUnit.MINUTES);
                
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "time-based-topic",
                    String.valueOf(messageTime.toEpochMilli()), // Use timestamp as string key for time-based partitioning
                    Map.of(
                        "messageId", "time-" + i,
                        "timestamp", messageTime.toString(),
                        "timeSlot", i,
                        "data", "Time-based data at " + messageTime
                    )
                );
                
                // Add timestamp header
                record.headers().add(new RecordHeader("message-time", 
                    String.valueOf(messageTime.toEpochMilli()).getBytes(StandardCharsets.UTF_8)));
                
                CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
                futures.add(future);
                
                logger.info("Sending time-based message for time slot {} ({})", i, messageTime);
            }
            
            // Wait for completion
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> logger.info("All time-based messages sent successfully"))
                .get(5, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            logger.error("Time-based partitioning demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate partitioning statistics and monitoring.
     */
    private static void demonstratePartitioningMonitoring() {
        logger.info("\n=== Partitioning Statistics and Monitoring ===");
        
        ProducerPartitioningManager manager = new ProducerPartitioningManager();
        
        // Test different strategies and collect statistics
        ProducerPartitioningType[] strategies = {
            ProducerPartitioningType.ROUND_ROBIN,
            ProducerPartitioningType.KEY_HASH,
            ProducerPartitioningType.MODULUS,
            ProducerPartitioningType.RANDOM,
            ProducerPartitioningType.STICKY
        };
        
        for (ProducerPartitioningType strategy : strategies) {
            logger.info("📊 Testing strategy: {}", strategy);
            
            manager.setPartitioningStrategy(strategy);
            
            // Generate test data
            Map<Integer, Integer> partitionDistribution = new HashMap<>();
            int numPartitions = 3;
            int numMessages = 30;
            
            for (int i = 0; i < numMessages; i++) {
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                    "monitoring-topic",
                    "key-" + (i % 10), // Limited key space for hash testing
                    Map.of("messageId", "monitor-" + i, "strategyTest", strategy.toString())
                );
                
                Integer partition = manager.determinePartition(record, numPartitions);
                if (partition != null) {
                    partitionDistribution.merge(partition, 1, Integer::sum);
                }
            }
            
            // Show distribution statistics
            logger.info("Partition distribution for {} strategy:", strategy);
            partitionDistribution.forEach((partition, count) -> 
                logger.info("  Partition {}: {} messages ({:.1f}%)", 
                          partition, count, (count * 100.0) / numMessages));
            
            // Show strategy statistics
            Map<String, Object> stats = manager.getPartitioningStats();
            logger.info("Strategy statistics: {}", stats);
            
            // Calculate distribution metrics
            double mean = partitionDistribution.values().stream()
                .mapToInt(Integer::intValue)
                .average()
                .orElse(0.0);
            
            double variance = partitionDistribution.values().stream()
                .mapToDouble(count -> Math.pow(count - mean, 2))
                .average()
                .orElse(0.0);
            
            logger.info("Distribution metrics - Mean: {:.2f}, Variance: {:.2f}", mean, variance);
            logger.info("---");
        }
    }
}
