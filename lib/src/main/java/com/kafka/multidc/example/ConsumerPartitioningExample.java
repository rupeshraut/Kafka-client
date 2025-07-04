package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.partitioning.ConsumerPartitioningStrategy;
import com.kafka.multidc.partitioning.impl.BuiltInConsumerPartitioningStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Comprehensive example demonstrating consumer partitioning strategies
 * with the Kafka Multi-Datacenter Client.
 * 
 * This example shows how to:
 * 1. Use built-in consumer partitioning strategies
 * 2. Implement custom consumer assignment logic
 * 3. Handle multi-datacenter consumer coordination
 * 4. Monitor consumer partition assignments
 */
public class ConsumerPartitioningExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumerPartitioningExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Consumer Partitioning Strategies Example ===");
        
        try {
            // Create configuration for consumer partitioning demonstration
            KafkaDatacenterConfiguration config = createConsumerPartitioningConfiguration();
            
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Multi-datacenter consumer client created successfully");
                
                // 1. Demonstrate built-in consumer partitioning strategies
                demonstrateBuiltInConsumerStrategies();
                
                // 2. Demonstrate datacenter-aware partitioning
                demonstrateDatacenterAwarePartitioning();
                
                // 3. Demonstrate load-balanced consumer assignment
                demonstrateLoadBalancedAssignment();
                
                // 4. Demonstrate sticky consumer assignment
                demonstrateStickyAssignment();
                
                // 5. Demonstrate priority-based assignment
                demonstratePriorityBasedAssignment();
                
                // 6. Demonstrate custom consumer partitioning
                demonstrateCustomConsumerPartitioning();
                
                // 7. Test consumer consumption with partition awareness
                demonstratePartitionAwareConsumption(client);
                
                logger.info("Consumer partitioning strategies example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Consumer partitioning example failed", e);
        }
    }
    
    /**
     * Create configuration optimized for consumer partitioning demonstrations.
     */
    private static KafkaDatacenterConfiguration createConsumerPartitioningConfiguration() {
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
        
        KafkaDatacenterEndpoint europeEndpoint = KafkaDatacenterEndpoint.builder()
            .id("eu-west-1")
            .region("eu-west-1")
            .bootstrapServers("localhost:9094")
            .build();
        
        // Configure producer and consumer (using default configs)
        ProducerConfig producerConfig = ProducerConfig.defaultConfig();
        ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(primaryEndpoint)
            .addDatacenter(secondaryEndpoint)
            .addDatacenter(europeEndpoint)
            .localDatacenter("us-east-1")
            .producerConfig(producerConfig)
            .consumerConfig(consumerConfig)
            .build();
    }
    
    /**
     * Demonstrate built-in consumer partitioning strategies.
     */
    private static void demonstrateBuiltInConsumerStrategies() {
        logger.info("\n=== Built-in Consumer Partitioning Strategies ===");
        
        // 1. Datacenter-Aware Range Strategy
        demonstrateDatacenterAwareRangeStrategy();
        
        // 2. Load-Balanced Assignment Strategy
        demonstrateLoadBalancedStrategy();
        
        // 3. Datacenter-Aware Sticky Strategy
        demonstrateDatacenterAwareStickyStrategy();
        
        // 4. Priority-Based Assignment Strategy
        demonstratePriorityBasedStrategy();
    }
    
    private static void demonstrateDatacenterAwareRangeStrategy() {
        logger.info("🌍 Datacenter-Aware Range Assignment Strategy");
        
        ConsumerPartitioningStrategy strategy = new BuiltInConsumerPartitioningStrategies.DatacenterAwareRangeStrategy();
        
        // Simulate multiple consumers across datacenters
        List<String> allConsumers = Arrays.asList(
            "consumer-us-east-1-1", "consumer-us-east-1-2",
            "consumer-us-west-2-1", "consumer-us-west-2-2",
            "consumer-eu-west-1-1"
        );
        
        // Create topic partitions
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < 12; i++) {
            topicPartitions.add(new TopicPartition("consumer-partitioning-topic", i));
        }
        
        // Create datacenter information
        Map<String, String> datacenterInfo = Map.of(
            "consumer-us-east-1-1", "us-east-1",
            "consumer-us-east-1-2", "us-east-1",
            "consumer-us-west-2-1", "us-west-2",
            "consumer-us-west-2-2", "us-west-2",
            "consumer-eu-west-1-1", "eu-west-1"
        );
        
        // Test assignment for each consumer
        for (String consumerId : allConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "test-group", allConsumers, topicPartitions, datacenterInfo
            );
            
            logger.info("Consumer '{}' (datacenter: {}) assigned partitions: {}", 
                      consumerId, 
                      datacenterInfo.get(consumerId),
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
        
        logger.info("Strategy Name: {}, Supports Incremental Rebalancing: {}", 
                  strategy.getStrategyName(), strategy.supportsIncrementalRebalancing());
    }
    
    private static void demonstrateLoadBalancedStrategy() {
        logger.info("⚖️ Load-Balanced Assignment Strategy");
        
        ConsumerPartitioningStrategy strategy = new BuiltInConsumerPartitioningStrategies.LoadBalancedAssignmentStrategy();
        
        // Configure strategy with load balancing parameters
        Map<String, Object> config = Map.of(
            "load.balancing.enabled", true,
            "max.partitions.per.consumer", 4,
            "load.threshold", 0.8
        );
        strategy.configure(config);
        
        // Simulate consumers with different capacities
        List<String> allConsumers = Arrays.asList(
            "high-capacity-consumer-1", "high-capacity-consumer-2",
            "medium-capacity-consumer-1", "low-capacity-consumer-1"
        );
        
        // Create topic partitions
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            topicPartitions.add(new TopicPartition("load-balanced-topic", i));
        }
        
        // Create consumer capacity info (simulated as datacenter info)
        Map<String, String> capacityInfo = Map.of(
            "high-capacity-consumer-1", "high-capacity",
            "high-capacity-consumer-2", "high-capacity",
            "medium-capacity-consumer-1", "medium-capacity",
            "low-capacity-consumer-1", "low-capacity"
        );
        
        // Test load-balanced assignment
        for (String consumerId : allConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "load-balanced-group", allConsumers, topicPartitions, capacityInfo
            );
            
            logger.info("Consumer '{}' (capacity: {}) assigned {} partitions: {}", 
                      consumerId, 
                      capacityInfo.get(consumerId),
                      assignedPartitions.size(),
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    private static void demonstrateDatacenterAwareStickyStrategy() {
        logger.info("📌 Datacenter-Aware Sticky Assignment Strategy");
        
        ConsumerPartitioningStrategy strategy = new BuiltInConsumerPartitioningStrategies.DatacenterAwareStickyStrategy();
        
        // Configure sticky parameters
        Map<String, Object> config = Map.of(
            "sticky.assignment.enabled", true,
            "datacenter.affinity.weight", 0.7,
            "partition.stickiness.factor", 0.9
        );
        strategy.configure(config);
        
        List<String> allConsumers = Arrays.asList(
            "sticky-consumer-us-1", "sticky-consumer-us-2", "sticky-consumer-eu-1"
        );
        
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < 9; i++) {
            topicPartitions.add(new TopicPartition("sticky-topic", i));
        }
        
        Map<String, String> datacenterInfo = Map.of(
            "sticky-consumer-us-1", "us-east-1",
            "sticky-consumer-us-2", "us-east-1",
            "sticky-consumer-eu-1", "eu-west-1"
        );
        
        // Show initial assignment
        logger.info("Initial sticky assignment:");
        for (String consumerId : allConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "sticky-group", allConsumers, topicPartitions, datacenterInfo
            );
            
            logger.info("Consumer '{}' initially assigned: {}", 
                      consumerId,
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
        
        // Simulate consumer leaving and rejoining (sticky behavior)
        List<String> reducedConsumers = Arrays.asList("sticky-consumer-us-1", "sticky-consumer-eu-1");
        
        logger.info("After consumer-us-2 leaves (sticky reassignment):");
        for (String consumerId : reducedConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "sticky-group", reducedConsumers, topicPartitions, datacenterInfo
            );
            
            logger.info("Consumer '{}' reassigned: {}", 
                      consumerId,
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    private static void demonstratePriorityBasedStrategy() {
        logger.info("🎯 Priority-Based Assignment Strategy");
        
        ConsumerPartitioningStrategy strategy = new BuiltInConsumerPartitioningStrategies.PriorityBasedAssignmentStrategy();
        
        // Configure priority settings
        Map<String, Object> config = Map.of(
            "priority.assignment.enabled", true,
            "high.priority.weight", 2.0,
            "medium.priority.weight", 1.5,
            "low.priority.weight", 1.0
        );
        strategy.configure(config);
        
        List<String> allConsumers = Arrays.asList(
            "high-priority-consumer-1", "high-priority-consumer-2",
            "medium-priority-consumer-1", "low-priority-consumer-1"
        );
        
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < 8; i++) {
            topicPartitions.add(new TopicPartition("priority-topic", i));
        }
        
        // Priority information (using datacenterInfo parameter)
        Map<String, String> priorityInfo = Map.of(
            "high-priority-consumer-1", "high",
            "high-priority-consumer-2", "high",
            "medium-priority-consumer-1", "medium",
            "low-priority-consumer-1", "low"
        );
        
        for (String consumerId : allConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "priority-group", allConsumers, topicPartitions, priorityInfo
            );
            
            logger.info("Consumer '{}' (priority: {}) assigned {} partitions: {}", 
                      consumerId, 
                      priorityInfo.get(consumerId),
                      assignedPartitions.size(),
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    /**
     * Demonstrate datacenter-aware partitioning scenarios.
     */
    private static void demonstrateDatacenterAwarePartitioning() {
        logger.info("\n=== Datacenter-Aware Partitioning Scenarios ===");
        
        // Scenario 1: Cross-datacenter failover
        demonstrateCrossDatacenterFailover();
        
        // Scenario 2: Locality-aware assignment
        demonstrateLocalityAwareAssignment();
    }
    
    private static void demonstrateCrossDatacenterFailover() {
        logger.info("🔄 Cross-Datacenter Failover Scenario");
        
        ConsumerPartitioningStrategy strategy = new BuiltInConsumerPartitioningStrategies.DatacenterAwareRangeStrategy();
        
        // Initial setup with consumers in multiple datacenters
        List<String> initialConsumers = Arrays.asList(
            "consumer-us-east-1", "consumer-us-west-2", "consumer-eu-west-1"
        );
        
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            topicPartitions.add(new TopicPartition("failover-topic", i));
        }
        
        Map<String, String> datacenterInfo = Map.of(
            "consumer-us-east-1", "us-east-1",
            "consumer-us-west-2", "us-west-2",
            "consumer-eu-west-1", "eu-west-1"
        );
        
        logger.info("Initial assignment before failover:");
        for (String consumerId : initialConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "failover-group", initialConsumers, topicPartitions, datacenterInfo
            );
            
            logger.info("Consumer '{}' assigned: {}", 
                      consumerId,
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
        
        // Simulate datacenter failure (us-east-1 goes down)
        List<String> failoverConsumers = Arrays.asList("consumer-us-west-2", "consumer-eu-west-1");
        
        logger.info("After us-east-1 datacenter failure:");
        for (String consumerId : failoverConsumers) {
            Set<TopicPartition> assignedPartitions = strategy.assignPartitions(
                consumerId, "failover-group", failoverConsumers, topicPartitions, datacenterInfo
            );
            
            logger.info("Consumer '{}' reassigned: {}", 
                      consumerId,
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    private static void demonstrateLocalityAwareAssignment() {
        logger.info("📍 Locality-Aware Assignment");
        
        // Custom locality-aware strategy
        ConsumerPartitioningStrategy localityStrategy = new ConsumerPartitioningStrategy() {
            @Override
            public Set<TopicPartition> assignPartitions(String consumerId, String consumerGroup, 
                    List<String> allConsumers, Set<TopicPartition> topicPartitions, 
                    Map<String, String> datacenterInfo) {
                
                String consumerDatacenter = datacenterInfo.get(consumerId);
                if (consumerDatacenter == null) {
                    return Set.of(); // No assignment if datacenter unknown
                }
                
                // Prefer partitions that are "local" to the consumer's datacenter
                // This is a simplified example - in reality, you'd have partition-to-datacenter mapping
                List<TopicPartition> sortedPartitions = new ArrayList<>(topicPartitions);
                sortedPartitions.sort(Comparator.comparing(TopicPartition::partition));
                
                Set<TopicPartition> assignment = new HashSet<>();
                int consumerIndex = allConsumers.indexOf(consumerId);
                int totalConsumers = allConsumers.size();
                int partitionsPerConsumer = sortedPartitions.size() / totalConsumers;
                int extraPartitions = sortedPartitions.size() % totalConsumers;
                
                int startIndex = consumerIndex * partitionsPerConsumer + Math.min(consumerIndex, extraPartitions);
                int endIndex = startIndex + partitionsPerConsumer + (consumerIndex < extraPartitions ? 1 : 0);
                
                for (int i = startIndex; i < endIndex && i < sortedPartitions.size(); i++) {
                    assignment.add(sortedPartitions.get(i));
                }
                
                return assignment;
            }
            
            @Override
            public String getStrategyName() {
                return "locality-aware";
            }
        };
        
        List<String> consumers = Arrays.asList(
            "local-consumer-east", "local-consumer-west", "local-consumer-europe"
        );
        
        Set<TopicPartition> partitions = new HashSet<>();
        for (int i = 0; i < 9; i++) {
            partitions.add(new TopicPartition("locality-topic", i));
        }
        
        Map<String, String> locationInfo = Map.of(
            "local-consumer-east", "us-east-1",
            "local-consumer-west", "us-west-2",
            "local-consumer-europe", "eu-west-1"
        );
        
        for (String consumerId : consumers) {
            Set<TopicPartition> assignedPartitions = localityStrategy.assignPartitions(
                consumerId, "locality-group", consumers, partitions, locationInfo
            );
            
            logger.info("Consumer '{}' (location: {}) locally assigned: {}", 
                      consumerId, 
                      locationInfo.get(consumerId),
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    /**
     * Additional demonstration methods.
     */
    private static void demonstrateLoadBalancedAssignment() {
        logger.info("\n=== Load-Balanced Assignment ===");
        // Implementation details for load balancing scenarios
        logger.info("Load balancing considers consumer processing capacity and current load");
    }
    
    private static void demonstrateStickyAssignment() {
        logger.info("\n=== Sticky Assignment ===");
        // Implementation details for sticky assignment
        logger.info("Sticky assignment minimizes partition movement during rebalancing");
    }
    
    private static void demonstratePriorityBasedAssignment() {
        logger.info("\n=== Priority-Based Assignment ===");
        // Implementation details for priority-based assignment
        logger.info("Priority-based assignment allocates more partitions to higher priority consumers");
    }
    
    private static void demonstrateCustomConsumerPartitioning() {
        logger.info("\n=== Custom Consumer Partitioning ===");
        
        // Example: Business-specific consumer assignment
        ConsumerPartitioningStrategy businessStrategy = new ConsumerPartitioningStrategy() {
            @Override
            public Set<TopicPartition> assignPartitions(String consumerId, String consumerGroup, 
                    List<String> allConsumers, Set<TopicPartition> topicPartitions, 
                    Map<String, String> datacenterInfo) {
                
                // Custom business logic for partition assignment
                Set<TopicPartition> assignment = new HashSet<>();
                
                if (consumerId.contains("critical")) {
                    // Critical consumers get priority partitions (0, 1, 2)
                    topicPartitions.stream()
                        .filter(tp -> tp.partition() < 3)
                        .forEach(assignment::add);
                } else if (consumerId.contains("batch")) {
                    // Batch consumers get higher-numbered partitions
                    topicPartitions.stream()
                        .filter(tp -> tp.partition() >= 3)
                        .forEach(assignment::add);
                }
                
                return assignment;
            }
            
            @Override
            public String getStrategyName() {
                return "business-critical";
            }
            
            @Override
            public boolean supportsIncrementalRebalancing() {
                return true;
            }
        };
        
        List<String> businessConsumers = Arrays.asList(
            "critical-consumer-1", "batch-consumer-1", "critical-consumer-2"
        );
        
        Set<TopicPartition> businessPartitions = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            businessPartitions.add(new TopicPartition("business-topic", i));
        }
        
        for (String consumerId : businessConsumers) {
            Set<TopicPartition> assignedPartitions = businessStrategy.assignPartitions(
                consumerId, "business-group", businessConsumers, businessPartitions, Map.of()
            );
            
            logger.info("Business consumer '{}' assigned: {}", 
                      consumerId,
                      assignedPartitions.stream()
                          .map(tp -> tp.partition())
                          .sorted()
                          .collect(Collectors.toList()));
        }
    }
    
    /**
     * Demonstrate partition-aware consumption patterns.
     */
    private static void demonstratePartitionAwareConsumption(KafkaMultiDatacenterClient client) {
        logger.info("\n=== Partition-Aware Consumption ===");
        
        try {
            // Get consumer operations
            var consumerOps = client.consumerSync();
            
            // Subscribe to topics for partition-aware consumption
            consumerOps.subscribe(Arrays.asList("consumer-partitioning-topic", "partition-aware-topic"));
            
            logger.info("Subscribed to topics for partition-aware consumption");
            
            // Consume with partition awareness
            int pollCount = 0;
            int maxPolls = 3;
            
            while (pollCount < maxPolls) {
                ConsumerRecords<String, Object> records = consumerOps.poll(Duration.ofMillis(1000));
                
                if (!records.isEmpty()) {
                    logger.info("Polled {} records from {} partitions", 
                              records.count(), records.partitions().size());
                    
                    // Process records by partition
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
                        
                        logger.info("Processing {} records from partition {} of topic '{}'", 
                                  partitionRecords.size(), 
                                  partition.partition(), 
                                  partition.topic());
                        
                        // Partition-specific processing logic
                        processPartitionRecords(partition, partitionRecords);
                    }
                    
                    // Commit offsets after processing
                    consumerOps.commitSync();
                    logger.info("Committed offsets for processed partitions");
                }
                
                pollCount++;
                Thread.sleep(500);
            }
            
        } catch (Exception e) {
            logger.error("Partition-aware consumption demonstration failed", e);
        }
    }
    
    private static void processPartitionRecords(TopicPartition partition, 
            List<ConsumerRecord<String, Object>> records) {
        
        // Example partition-specific processing
        for (ConsumerRecord<String, Object> record : records) {
            logger.info("Processing record from partition {} - Key: {}, Offset: {}, Timestamp: {}", 
                      partition.partition(), 
                      record.key(), 
                      record.offset(), 
                      record.timestamp());
            
            // Simulate partition-specific business logic
            if (partition.partition() % 2 == 0) {
                // Even partitions: priority processing
                logger.debug("Priority processing for partition {}", partition.partition());
            } else {
                // Odd partitions: batch processing
                logger.debug("Batch processing for partition {}", partition.partition());
            }
        }
        
        logger.info("Completed processing {} records from partition {}", 
                  records.size(), partition.partition());
    }
}
