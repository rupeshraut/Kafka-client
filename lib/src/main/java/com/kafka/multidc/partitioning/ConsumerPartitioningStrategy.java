package com.kafka.multidc.partitioning;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for custom consumer partition assignment strategies.
 * Extends the basic partition assignment with multi-datacenter awareness.
 */
public interface ConsumerPartitioningStrategy {
    
    /**
     * Assign partitions to a consumer.
     *
     * @param consumerId Consumer identifier
     * @param consumerGroup Consumer group ID
     * @param allConsumers All consumers in the group
     * @param topicPartitions Available topic partitions
     * @param datacenterInfo Datacenter information for locality-aware assignment
     * @return Set of assigned partitions
     */
    Set<TopicPartition> assignPartitions(
        String consumerId,
        String consumerGroup,
        List<String> allConsumers,
        Set<TopicPartition> topicPartitions,
        Map<String, String> datacenterInfo
    );
    
    /**
     * Get the strategy name.
     *
     * @return Strategy name
     */
    String getStrategyName();
    
    /**
     * Configure the strategy.
     *
     * @param properties Configuration properties
     */
    default void configure(Map<String, Object> properties) {
        // Default: no configuration needed
    }
    
    /**
     * Check if the strategy supports incremental rebalancing.
     *
     * @return true if supports incremental rebalancing
     */
    default boolean supportsIncrementalRebalancing() {
        return false;
    }
    
    /**
     * Handle partition revocation for incremental rebalancing.
     *
     * @param consumerId Consumer identifier
     * @param revokedPartitions Partitions being revoked
     * @param remainingPartitions Partitions that will remain assigned
     * @return Additional partitions to revoke for optimal rebalancing
     */
    default Set<TopicPartition> handlePartitionRevocation(
        String consumerId,
        Set<TopicPartition> revokedPartitions,
        Set<TopicPartition> remainingPartitions
    ) {
        return Set.of(); // Default: no additional revocations
    }
}
