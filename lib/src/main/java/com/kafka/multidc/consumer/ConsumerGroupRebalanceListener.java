package com.kafka.multidc.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * Consumer group rebalance listener for handling rebalance events.
 */
public interface ConsumerGroupRebalanceListener {
    
    /**
     * Called when partitions are revoked from the consumer.
     */
    void onPartitionsRevoked(Set<TopicPartition> partitions);
    
    /**
     * Called when partitions are assigned to the consumer.
     */
    void onPartitionsAssigned(Set<TopicPartition> partitions);
    
    /**
     * Called when partitions are lost (unexpected revocation).
     */
    default void onPartitionsLost(Set<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}
