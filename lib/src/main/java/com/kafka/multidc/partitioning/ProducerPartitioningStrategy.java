package com.kafka.multidc.partitioning;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Interface for custom producer partitioning strategies.
 * Allows implementing custom logic for determining which partition a message should be sent to.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface ProducerPartitioningStrategy<K, V> {
    
    /**
     * Determine the partition for a given record.
     *
     * @param record The producer record
     * @param numPartitions The total number of partitions for the topic
     * @return The partition number (0-based) or null for default partitioning
     */
    Integer partition(ProducerRecord<K, V> record, int numPartitions);
    
    /**
     * Get the strategy name for identification and metrics.
     *
     * @return Strategy name
     */
    String getStrategyName();
    
    /**
     * Configure the strategy with custom properties.
     *
     * @param properties Configuration properties
     */
    default void configure(java.util.Map<String, Object> properties) {
        // Default: no configuration needed
    }
}
