package com.kafka.multidc.partitioning;

/**
 * Enumeration of built-in producer partitioning strategies.
 */
public enum ProducerPartitioningType {
    
    /**
     * Default Kafka partitioning - uses key hash or round-robin if no key.
     */
    DEFAULT,
    
    /**
     * Round-robin partitioning - distributes messages evenly across partitions.
     */
    ROUND_ROBIN,
    
    /**
     * Key-hash partitioning - uses consistent hashing of the key.
     */
    KEY_HASH,
    
    /**
     * Modulus partitioning - uses simple modulus operation on key hash.
     */
    MODULUS,
    
    /**
     * Random partitioning - randomly selects partition for each message.
     */
    RANDOM,
    
    /**
     * Sticky partitioning - sends to same partition until batch is full.
     */
    STICKY,
    
    /**
     * Geographic partitioning - partition based on geographic regions.
     */
    GEOGRAPHIC,
    
    /**
     * Time-based partitioning - partition based on timestamp.
     */
    TIME_BASED,
    
    /**
     * Load-balanced partitioning - considers partition load metrics.
     */
    LOAD_BALANCED,
    
    /**
     * Custom partitioning strategy.
     */
    CUSTOM
}
