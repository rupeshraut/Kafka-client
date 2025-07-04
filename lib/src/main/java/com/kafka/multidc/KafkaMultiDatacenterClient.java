package com.kafka.multidc;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.model.DatacenterInfo;
import com.kafka.multidc.model.DatacenterHealthListener;
import com.kafka.multidc.operations.producer.KafkaProducerOperations;
import com.kafka.multidc.operations.consumer.KafkaConsumerOperations;
import com.kafka.multidc.pool.KafkaConnectionMetrics;
import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import reactor.core.Disposable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Main interface for the Kafka Multi-Datacenter Client.
 * Provides enterprise-grade Kafka producer and consumer operations
 * with intelligent routing, health monitoring, and comprehensive resilience.
 */
public interface KafkaMultiDatacenterClient extends AutoCloseable {
    
    // ===== Producer Operations =====
    
    /**
     * Get synchronous producer operations.
     *
     * @return producer operations for synchronous usage
     */
    KafkaProducerOperations.Sync producerSync();
    
    /**
     * Get asynchronous producer operations.
     *
     * @return producer operations for asynchronous usage
     */
    KafkaProducerOperations.Async producerAsync();
    
    /**
     * Get reactive producer operations.
     *
     * @return producer operations for reactive usage
     */
    KafkaProducerOperations.Reactive producerReactive();
    
    // ===== Consumer Operations =====
    
    /**
     * Get synchronous consumer operations.
     *
     * @return consumer operations for synchronous usage
     */
    KafkaConsumerOperations.Sync consumerSync();
    
    /**
     * Get asynchronous consumer operations.
     *
     * @return consumer operations for asynchronous usage
     */
    KafkaConsumerOperations.Async consumerAsync();
    
    /**
     * Get reactive consumer operations.
     *
     * @return consumer operations for reactive usage
     */
    KafkaConsumerOperations.Reactive consumerReactive();
    
    // ===== Datacenter Management =====
    
    /**
     * Get information about all configured datacenters.
     *
     * @return list of datacenter information
     */
    List<DatacenterInfo> getDatacenters();
    
    /**
     * Get information about the local (preferred) datacenter.
     *
     * @return local datacenter information
     */
    DatacenterInfo getLocalDatacenter();
    
    /**
     * Check the health of all datacenters.
     *
     * @return future with health status of all datacenters
     */
    CompletableFuture<List<DatacenterInfo>> checkDatacenterHealth();
    
    /**
     * Refresh datacenter routing information.
     *
     * @return future that completes when refresh is done
     */
    CompletableFuture<Void> refreshDatacenterInfo();
    
    /**
     * Subscribe to datacenter health change events.
     *
     * @param listener listener for health change events
     * @return disposable to unsubscribe
     */
    Disposable subscribeToHealthChanges(DatacenterHealthListener listener);
    
    // ===== Configuration =====
    
    /**
     * Get the current configuration.
     *
     * @return datacenter configuration
     */
    KafkaDatacenterConfiguration getConfiguration();
    
    // ===== Connection Pool Management =====
    
    /**
     * Get connection metrics for a specific datacenter.
     *
     * @param datacenterId datacenter identifier
     * @return connection metrics
     */
    KafkaConnectionMetrics getConnectionMetrics(String datacenterId);
    
    /**
     * Get aggregated connection metrics across all datacenters.
     *
     * @return aggregated metrics
     */
    KafkaConnectionPoolManager.AggregatedMetrics getAggregatedConnectionMetrics();
    
    /**
     * Check if all connection pools are healthy.
     *
     * @return true if all pools are healthy
     */
    boolean areAllConnectionPoolsHealthy();
    
    /**
     * Get health status of all connection pools.
     *
     * @return map of datacenter ID to health status
     */
    Map<String, Boolean> getConnectionPoolHealth();
    
    /**
     * Warm up all connection pools to reduce cold start latency.
     *
     * @return future that completes when warmup is done
     */
    CompletableFuture<Void> warmUpConnections();
    
    /**
     * Perform maintenance on all connection pools.
     */
    void maintainAllConnectionPools();
    
    /**
     * Drain a specific connection pool.
     *
     * @param datacenterId datacenter identifier
     */
    void drainConnectionPool(String datacenterId);
    
    /**
     * Reset connection metrics for a specific datacenter.
     *
     * @param datacenterId datacenter identifier
     */
    void resetConnectionMetrics(String datacenterId);
    
    /**
     * Get all connection metrics.
     *
     * @return map of datacenter ID to connection metrics
     */
    Map<String, KafkaConnectionMetrics> getAllConnectionMetrics();
    
    // ===== Schema Registry Operations =====
    
    /**
     * Check schema registry health across all datacenters.
     *
     * @return future with schema registry health status
     */
    CompletableFuture<Map<String, Boolean>> checkSchemaRegistryHealth();
    
    /**
     * Refresh schema registry client connections.
     *
     * @return future that completes when refresh is done
     */
    CompletableFuture<Void> refreshSchemaRegistryConnections();
    
    // ===== Lifecycle Management =====
    
    /**
     * Check if the client is closed.
     *
     * @return true if the client is closed
     */
    boolean isClosed();
    
    /**
     * Close the client and release all resources.
     */
    @Override
    void close();
}
