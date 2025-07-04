package com.kafka.multidc.consumer;

/**
 * Partition assignment strategy enumeration.
 */
public enum PartitionAssignmentStrategy {
    
    /**
     * Range assignment strategy - assigns consecutive partitions to consumers.
     */
    RANGE,
    
    /**
     * Round-robin assignment strategy - distributes partitions evenly across consumers.
     */
    ROUND_ROBIN,
    
    /**
     * Sticky assignment strategy - minimizes partition movement during rebalances.
     */
    STICKY,
    
    /**
     * Cooperative sticky assignment strategy - enables incremental rebalancing.
     */
    COOPERATIVE_STICKY,
    
    /**
     * Custom assignment strategy.
     */
    CUSTOM
}
