package com.kafka.multidc.consumer;

/**
 * Metrics interface for consumer group operations.
 */
public interface ConsumerGroupMetrics {
    
    /**
     * Get the number of successful operations.
     */
    long getOperationsSucceeded();
    
    /**
     * Get the number of failed operations.
     */
    long getOperationsFailed();
    
    /**
     * Get the number of consumer groups deleted.
     */
    long getGroupsDeleted();
    
    /**
     * Get the number of partitions assigned.
     */
    long getPartitionsAssigned();
    
    /**
     * Get the number of partitions paused.
     */
    long getPartitionsPaused();
    
    /**
     * Get the number of partitions resumed.
     */
    long getPartitionsResumed();
    
    /**
     * Get the number of seek operations performed.
     */
    long getSeekOperations();
    
    /**
     * Get the number of rebalance events.
     */
    long getRebalanceEvents();
    
    /**
     * Get the operation success rate.
     */
    double getOperationSuccessRate();
    
    /**
     * Get the number of active consumer groups.
     */
    int getActiveConsumerGroups();
}
