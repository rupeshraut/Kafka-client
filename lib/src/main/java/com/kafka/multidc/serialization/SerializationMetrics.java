package com.kafka.multidc.serialization;

import java.time.Duration;

/**
 * Metrics for serialization operations and performance.
 */
public interface SerializationMetrics {
    
    /**
     * Get the total number of serialization operations performed.
     */
    long getSerializationCount();
    
    /**
     * Get the total number of deserialization operations performed.
     */
    long getDeserializationCount();
    
    /**
     * Get the total number of serialization errors.
     */
    long getSerializationErrors();
    
    /**
     * Get the total number of deserialization errors.
     */
    long getDeserializationErrors();
    
    /**
     * Get the total bytes serialized.
     */
    long getTotalSerializedBytes();
    
    /**
     * Get the total bytes deserialized.
     */
    long getTotalDeserializedBytes();
    
    /**
     * Get the average time taken for serialization operations.
     */
    Duration getAverageSerializationTime();
    
    /**
     * Get the average time taken for deserialization operations.
     */
    Duration getAverageDeserializationTime();
    
    /**
     * Get the success rate for serialization operations.
     */
    double getSerializationSuccessRate();
    
    /**
     * Get the success rate for deserialization operations.
     */
    double getDeserializationSuccessRate();
    
    /**
     * Get the number of registered schemas.
     */
    long getRegisteredSchemas();
}
