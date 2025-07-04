package com.kafka.multidc.partitioning.impl;

import com.kafka.multidc.partitioning.ConsumerPartitioningStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Built-in consumer partitioning strategies implementations.
 */
public class BuiltInConsumerPartitioningStrategies {
    
    private static final Logger logger = LoggerFactory.getLogger(BuiltInConsumerPartitioningStrategies.class);
    
    /**
     * Datacenter-aware range assignment strategy.
     */
    public static class DatacenterAwareRangeStrategy implements ConsumerPartitioningStrategy {
        
        @Override
        public Set<TopicPartition> assignPartitions(
            String consumerId,
            String consumerGroup,
            List<String> allConsumers,
            Set<TopicPartition> topicPartitions,
            Map<String, String> datacenterInfo
        ) {
            String consumerDatacenter = datacenterInfo.get(consumerId);
            if (consumerDatacenter == null) {
                return assignRangePartitions(consumerId, allConsumers, topicPartitions);
            }
            
            // Group consumers by datacenter
            Map<String, List<String>> consumersByDatacenter = allConsumers.stream()
                .collect(Collectors.groupingBy(
                    consumer -> datacenterInfo.getOrDefault(consumer, "unknown")
                ));
            
            // Group partitions by topic
            Map<String, List<TopicPartition>> partitionsByTopic = topicPartitions.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic));
            
            Set<TopicPartition> assignedPartitions = new HashSet<>();
            
            for (Map.Entry<String, List<TopicPartition>> topicEntry : partitionsByTopic.entrySet()) {
                List<TopicPartition> partitions = topicEntry.getValue();
                partitions.sort(Comparator.comparing(TopicPartition::partition));
                
                // Assign partitions preferring local datacenter consumers
                List<String> localConsumers = consumersByDatacenter.getOrDefault(consumerDatacenter, Collections.emptyList());
                int consumerIndex = localConsumers.indexOf(consumerId);
                
                if (consumerIndex >= 0) {
                    // Assign range of partitions to local consumers first
                    int partitionsPerConsumer = partitions.size() / localConsumers.size();
                    int remainder = partitions.size() % localConsumers.size();
                    
                    int startIndex = consumerIndex * partitionsPerConsumer + Math.min(consumerIndex, remainder);
                    int endIndex = startIndex + partitionsPerConsumer + (consumerIndex < remainder ? 1 : 0);
                    
                    for (int i = startIndex; i < endIndex && i < partitions.size(); i++) {
                        assignedPartitions.add(partitions.get(i));
                    }
                }
            }
            
            logger.debug("Datacenter-aware range assignment for consumer {} in datacenter {}: {} partitions",
                        consumerId, consumerDatacenter, assignedPartitions.size());
            
            return assignedPartitions;
        }
        
        private Set<TopicPartition> assignRangePartitions(String consumerId, List<String> allConsumers, Set<TopicPartition> topicPartitions) {
            List<String> sortedConsumers = new ArrayList<>(allConsumers);
            sortedConsumers.sort(String::compareTo);
            
            int consumerIndex = sortedConsumers.indexOf(consumerId);
            if (consumerIndex == -1) {
                return Set.of();
            }
            
            Map<String, List<TopicPartition>> partitionsByTopic = topicPartitions.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic));
            
            Set<TopicPartition> assignedPartitions = new HashSet<>();
            
            for (List<TopicPartition> partitions : partitionsByTopic.values()) {
                partitions.sort(Comparator.comparing(TopicPartition::partition));
                
                int partitionsPerConsumer = partitions.size() / sortedConsumers.size();
                int remainder = partitions.size() % sortedConsumers.size();
                
                int startIndex = consumerIndex * partitionsPerConsumer + Math.min(consumerIndex, remainder);
                int endIndex = startIndex + partitionsPerConsumer + (consumerIndex < remainder ? 1 : 0);
                
                for (int i = startIndex; i < endIndex && i < partitions.size(); i++) {
                    assignedPartitions.add(partitions.get(i));
                }
            }
            
            return assignedPartitions;
        }
        
        @Override
        public String getStrategyName() {
            return "datacenter-aware-range";
        }
    }
    
    /**
     * Load-balanced partition assignment strategy.
     */
    public static class LoadBalancedAssignmentStrategy implements ConsumerPartitioningStrategy {
        private Map<String, Integer> partitionLoadMap = new HashMap<>();
        
        @Override
        public Set<TopicPartition> assignPartitions(
            String consumerId,
            String consumerGroup,
            List<String> allConsumers,
            Set<TopicPartition> topicPartitions,
            Map<String, String> datacenterInfo
        ) {
            List<String> sortedConsumers = new ArrayList<>(allConsumers);
            sortedConsumers.sort(String::compareTo);
            
            int consumerIndex = sortedConsumers.indexOf(consumerId);
            if (consumerIndex == -1) {
                return Set.of();
            }
            
            // Sort partitions by load (ascending)
            List<TopicPartition> sortedPartitions = new ArrayList<>(topicPartitions);
            sortedPartitions.sort((p1, p2) -> {
                int load1 = partitionLoadMap.getOrDefault(p1.toString(), 0);
                int load2 = partitionLoadMap.getOrDefault(p2.toString(), 0);
                return Integer.compare(load1, load2);
            });
            
            Set<TopicPartition> assignedPartitions = new HashSet<>();
            
            // Round-robin assignment starting from consumer index
            for (int i = 0; i < sortedPartitions.size(); i++) {
                if (i % sortedConsumers.size() == consumerIndex) {
                    assignedPartitions.add(sortedPartitions.get(i));
                }
            }
            
            logger.debug("Load-balanced assignment for consumer {}: {} partitions",
                        consumerId, assignedPartitions.size());
            
            return assignedPartitions;
        }
        
        @Override
        public String getStrategyName() {
            return "load-balanced";
        }
        
        @Override
        public void configure(Map<String, Object> properties) {
            @SuppressWarnings("unchecked")
            Map<String, Integer> loadMap = (Map<String, Integer>) properties.get("partition.load.map");
            if (loadMap != null) {
                this.partitionLoadMap = new HashMap<>(loadMap);
            }
        }
    }
    
    /**
     * Sticky assignment strategy with datacenter awareness.
     */
    public static class DatacenterAwareStickyStrategy implements ConsumerPartitioningStrategy {
        private Map<String, Set<TopicPartition>> previousAssignments = new HashMap<>();
        
        @Override
        public Set<TopicPartition> assignPartitions(
            String consumerId,
            String consumerGroup,
            List<String> allConsumers,
            Set<TopicPartition> topicPartitions,
            Map<String, String> datacenterInfo
        ) {
            String consumerDatacenter = datacenterInfo.get(consumerId);
            
            // Start with previous assignment if available
            Set<TopicPartition> assignedPartitions = new HashSet<>(
                previousAssignments.getOrDefault(consumerId, Set.of())
            );
            
            // Remove partitions that no longer exist
            assignedPartitions.retainAll(topicPartitions);
            
            // Calculate target partition count per consumer
            int targetPartitionsPerConsumer = topicPartitions.size() / allConsumers.size();
            int remainder = topicPartitions.size() % allConsumers.size();
            
            List<String> sortedConsumers = new ArrayList<>(allConsumers);
            sortedConsumers.sort(String::compareTo);
            int consumerIndex = sortedConsumers.indexOf(consumerId);
            
            int targetPartitions = targetPartitionsPerConsumer + (consumerIndex < remainder ? 1 : 0);
            
            // If we have too many partitions, remove the excess
            if (assignedPartitions.size() > targetPartitions) {
                List<TopicPartition> excess = new ArrayList<>(assignedPartitions);
                excess.sort(Comparator.comparing(tp -> tp.topic() + "-" + tp.partition()));
                
                for (int i = 0; i < assignedPartitions.size() - targetPartitions; i++) {
                    assignedPartitions.remove(excess.get(i));
                }
            }
            
            // If we need more partitions, add them with datacenter preference
            if (assignedPartitions.size() < targetPartitions) {
                Set<TopicPartition> unassigned = new HashSet<>(topicPartitions);
                unassigned.removeAll(getAllPreviousAssignments());
                
                List<TopicPartition> candidates = new ArrayList<>(unassigned);
                
                // Prefer partitions in same datacenter if available
                if (consumerDatacenter != null) {
                    candidates.sort((p1, p2) -> {
                        boolean p1SameDatacenter = isPartitionInDatacenter(p1, consumerDatacenter);
                        boolean p2SameDatacenter = isPartitionInDatacenter(p2, consumerDatacenter);
                        
                        if (p1SameDatacenter && !p2SameDatacenter) return -1;
                        if (!p1SameDatacenter && p2SameDatacenter) return 1;
                        return p1.toString().compareTo(p2.toString());
                    });
                }
                
                int needed = targetPartitions - assignedPartitions.size();
                for (int i = 0; i < Math.min(needed, candidates.size()); i++) {
                    assignedPartitions.add(candidates.get(i));
                }
            }
            
            // Update previous assignments
            previousAssignments.put(consumerId, new HashSet<>(assignedPartitions));
            
            logger.debug("Datacenter-aware sticky assignment for consumer {} in datacenter {}: {} partitions",
                        consumerId, consumerDatacenter, assignedPartitions.size());
            
            return assignedPartitions;
        }
        
        private Set<TopicPartition> getAllPreviousAssignments() {
            return previousAssignments.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        }
        
        private boolean isPartitionInDatacenter(TopicPartition partition, String datacenter) {
            // Simple heuristic: check if topic name contains datacenter identifier
            return partition.topic().toLowerCase().contains(datacenter.toLowerCase());
        }
        
        @Override
        public String getStrategyName() {
            return "datacenter-aware-sticky";
        }
        
        @Override
        public boolean supportsIncrementalRebalancing() {
            return true;
        }
    }
    
    /**
     * Priority-based assignment strategy.
     */
    public static class PriorityBasedAssignmentStrategy implements ConsumerPartitioningStrategy {
        private Map<String, Integer> consumerPriorities = new HashMap<>();
        
        @Override
        public Set<TopicPartition> assignPartitions(
            String consumerId,
            String consumerGroup,
            List<String> allConsumers,
            Set<TopicPartition> topicPartitions,
            Map<String, String> datacenterInfo
        ) {
            // Sort consumers by priority (higher priority gets more partitions)
            List<String> sortedConsumers = allConsumers.stream()
                .sorted((c1, c2) -> {
                    int p1 = consumerPriorities.getOrDefault(c1, 0);
                    int p2 = consumerPriorities.getOrDefault(c2, 0);
                    if (p1 != p2) {
                        return Integer.compare(p2, p1); // Higher priority first
                    }
                    return c1.compareTo(c2); // Stable sort
                })
                .collect(Collectors.toList());
            
            int consumerIndex = sortedConsumers.indexOf(consumerId);
            if (consumerIndex == -1) {
                return Set.of();
            }
            
            // Calculate weighted partition distribution
            int totalPriority = allConsumers.stream()
                .mapToInt(c -> consumerPriorities.getOrDefault(c, 1))
                .sum();
            
            int consumerPriority = consumerPriorities.getOrDefault(consumerId, 1);
            int targetPartitions = (topicPartitions.size() * consumerPriority) / totalPriority;
            
            List<TopicPartition> sortedPartitions = new ArrayList<>(topicPartitions);
            sortedPartitions.sort(Comparator.comparing(tp -> tp.topic() + "-" + tp.partition()));
            
            Set<TopicPartition> assignedPartitions = new HashSet<>();
            
            // Assign partitions based on priority
            int startIndex = 0;
            for (int i = 0; i < consumerIndex; i++) {
                String consumer = sortedConsumers.get(i);
                int priority = consumerPriorities.getOrDefault(consumer, 1);
                int partitions = (topicPartitions.size() * priority) / totalPriority;
                startIndex += partitions;
            }
            
            int endIndex = Math.min(startIndex + targetPartitions, sortedPartitions.size());
            for (int i = startIndex; i < endIndex; i++) {
                assignedPartitions.add(sortedPartitions.get(i));
            }
            
            logger.debug("Priority-based assignment for consumer {} (priority {}): {} partitions",
                        consumerId, consumerPriority, assignedPartitions.size());
            
            return assignedPartitions;
        }
        
        @Override
        public String getStrategyName() {
            return "priority-based";
        }
        
        @Override
        public void configure(Map<String, Object> properties) {
            @SuppressWarnings("unchecked")
            Map<String, Integer> priorities = (Map<String, Integer>) properties.get("consumer.priorities");
            if (priorities != null) {
                this.consumerPriorities = new HashMap<>(priorities);
            }
        }
    }
}
