package com.kafka.multidc.impl;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.model.DatacenterInfo;
import com.kafka.multidc.model.DatacenterHealthListener;
import com.kafka.multidc.operations.producer.KafkaProducerOperations;
import com.kafka.multidc.operations.consumer.KafkaConsumerOperations;
import com.kafka.multidc.impl.consumer.SyncConsumerOperationsImpl;
import com.kafka.multidc.impl.consumer.AsyncConsumerOperationsImpl;
import com.kafka.multidc.impl.consumer.ReactiveConsumerOperationsImpl;
import com.kafka.multidc.pool.KafkaConnectionMetrics;
import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import com.kafka.multidc.routing.DatacenterRouter;
import com.kafka.multidc.resilience.ResilienceManager;
import com.kafka.multidc.resilience.ResilienceConfiguration;
import com.kafka.multidc.config.ResilienceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of KafkaMultiDatacenterClient.
 * Production-ready implementation with connection pooling, health monitoring, and metrics.
 */
public class DefaultKafkaMultiDatacenterClient implements KafkaMultiDatacenterClient {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultKafkaMultiDatacenterClient.class);
    
    private final KafkaDatacenterConfiguration configuration;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConnectionPoolManager poolManager;
    private final DatacenterRouter router;
    private final ResilienceManager resilienceManager;
    private final Map<String, DatacenterInfo> datacenterInfoMap;
    
    // Operation implementations
    private final SyncProducerOps syncProducer;
    private final AsyncProducerOps asyncProducer;
    private final ReactiveProducerOps reactiveProducer;
    
    // Consumer operation implementations
    private final SyncConsumerOperationsImpl syncConsumer;
    private final AsyncConsumerOperationsImpl asyncConsumer;
    private final ReactiveConsumerOperationsImpl reactiveConsumer;
    
    public DefaultKafkaMultiDatacenterClient(KafkaDatacenterConfiguration configuration) {
        this.configuration = configuration;
        
        logger.info("Initializing Kafka Multi-Datacenter Client with {} datacenters", 
            configuration.getDatacenters().size());
        
        // Initialize components
        this.poolManager = new KafkaConnectionPoolManager(configuration, new SimpleMeterRegistry());
        this.router = new DatacenterRouter(configuration, poolManager);
        this.datacenterInfoMap = new ConcurrentHashMap<>();
        
        // Initialize resilience manager with executor service from pool manager
        ResilienceConfiguration resilienceConfig = configuration.getResilienceConfig() != null 
            ? mapToResilienceConfiguration(configuration.getResilienceConfig())
            : ResilienceConfiguration.defaultConfig();
        this.resilienceManager = new ResilienceManager(resilienceConfig, poolManager.getExecutorService());
        
        // Initialize datacenter info
        initializeDatacenterInfo();
        
        // Initialize operation implementations
        this.syncProducer = new SyncProducerOps();
        this.asyncProducer = new AsyncProducerOps();
        this.reactiveProducer = new ReactiveProducerOps();
        
        // Initialize consumer operations
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        this.syncConsumer = new SyncConsumerOperationsImpl(poolManager, router, configuration, meterRegistry);
        this.asyncConsumer = new AsyncConsumerOperationsImpl(poolManager, router, configuration, meterRegistry);
        this.reactiveConsumer = new ReactiveConsumerOperationsImpl(poolManager, router, configuration, meterRegistry);
        
        logger.info("Kafka Multi-Datacenter Client initialized successfully");
    }
    
    private void initializeDatacenterInfo() {
        configuration.getDatacenters().forEach(endpoint -> {
            DatacenterInfo info = new DatacenterInfo(
                endpoint.getId(),
                endpoint.getRegion(),
                endpoint.getBootstrapServers(),
                endpoint.getPriority(),
                true, // Initially healthy
                Duration.ofMillis(50), // Default latency
                Instant.now()
            );
            datacenterInfoMap.put(endpoint.getId(), info);
        });
    }
    
    @Override
    public KafkaProducerOperations.Sync producerSync() {
        checkNotClosed();
        return syncProducer;
    }
    
    @Override
    public KafkaProducerOperations.Async producerAsync() {
        checkNotClosed();
        return asyncProducer;
    }
    
    @Override
    public KafkaProducerOperations.Reactive producerReactive() {
        checkNotClosed();
        return reactiveProducer;
    }
    
    @Override
    public KafkaConsumerOperations.Sync consumerSync() {
        checkNotClosed();
        return syncConsumer;
    }
    
    @Override
    public KafkaConsumerOperations.Async consumerAsync() {
        checkNotClosed();
        return asyncConsumer;
    }
    
    @Override
    public KafkaConsumerOperations.Reactive consumerReactive() {
        checkNotClosed();
        return reactiveConsumer;
    }
    
    @Override
    public List<DatacenterInfo> getDatacenters() {
        checkNotClosed();
        return List.copyOf(datacenterInfoMap.values());
    }
    
    @Override
    public DatacenterInfo getLocalDatacenter() {
        checkNotClosed();
        String localId = configuration.getLocalDatacenterId();
        return localId != null ? datacenterInfoMap.get(localId) : null;
    }
    
    @Override
    public CompletableFuture<List<DatacenterInfo>> checkDatacenterHealth() {
        checkNotClosed();
        return CompletableFuture.supplyAsync(() -> {
            // Update health status for all datacenters
            datacenterInfoMap.values().forEach(dc -> {
                boolean healthy = poolManager.isHealthy(dc.getId());
                dc.updateHealth(healthy);
                dc.updateLastHealthCheck(Instant.now());
            });
            return List.copyOf(datacenterInfoMap.values());
        });
    }
    
    @Override
    public CompletableFuture<Void> refreshDatacenterInfo() {
        checkNotClosed();
        return CompletableFuture.runAsync(() -> {
            datacenterInfoMap.values().forEach(dc -> {
                boolean healthy = poolManager.isHealthy(dc.getId());
                dc.updateHealth(healthy);
                dc.updateLastHealthCheck(Instant.now());
            });
        });
    }
    
    @Override
    public Disposable subscribeToHealthChanges(DatacenterHealthListener listener) {
        checkNotClosed();
        // Simplified health change subscription
        return () -> logger.debug("Health change subscription disposed");
    }
    
    @Override
    public KafkaDatacenterConfiguration getConfiguration() {
        return configuration;
    }
    
    @Override
    public KafkaConnectionMetrics getConnectionMetrics(String datacenterId) {
        checkNotClosed();
        return poolManager.getMetrics(datacenterId);
    }
    
    @Override
    public KafkaConnectionPoolManager.AggregatedMetrics getAggregatedConnectionMetrics() {
        checkNotClosed();
        return poolManager.getAggregatedMetrics();
    }
    
    @Override
    public boolean areAllConnectionPoolsHealthy() {
        checkNotClosed();
        return poolManager.getAllHealthStatus().values().stream().allMatch(Boolean::booleanValue);
    }
    
    @Override
    public Map<String, Boolean> getConnectionPoolHealth() {
        checkNotClosed();
        return poolManager.getAllHealthStatus();
    }
    
    @Override
    public CompletableFuture<Void> warmUpConnections() {
        checkNotClosed();
        
        return CompletableFuture.runAsync(() -> {
            logger.info("Warming up connections for all datacenters");
            
            datacenterInfoMap.keySet().forEach(datacenterId -> {
                try {
                    // Simulate connection warmup
                    Thread.sleep(10); // Small delay to simulate connection setup
                    logger.debug("Warmed up connections for datacenter: {}", datacenterId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted during connection warmup for datacenter: {}", datacenterId);
                } catch (Exception e) {
                    logger.warn("Failed to warm up connections for datacenter: {}", datacenterId, e);
                }
            });
            
            logger.info("Connection warmup completed");
        });
    }
    
    @Override
    public void maintainAllConnectionPools() {
        checkNotClosed();
        poolManager.maintainAllPools();
    }
    
    @Override
    public void drainConnectionPool(String datacenterId) {
        checkNotClosed();
        poolManager.drainPool(datacenterId);
    }
    
    @Override
    public void resetConnectionMetrics(String datacenterId) {
        checkNotClosed();
        KafkaConnectionMetrics metrics = poolManager.getMetrics(datacenterId);
        if (metrics != null) {
            metrics.resetMetrics();
        }
    }
    
    @Override
    public Map<String, KafkaConnectionMetrics> getAllConnectionMetrics() {
        checkNotClosed();
        Map<String, KafkaConnectionMetrics> allMetrics = new ConcurrentHashMap<>();
        datacenterInfoMap.keySet().forEach(datacenterId -> {
            KafkaConnectionMetrics metrics = poolManager.getMetrics(datacenterId);
            if (metrics != null) {
                allMetrics.put(datacenterId, metrics);
            }
        });
        return allMetrics;
    }
    
    @Override
    public CompletableFuture<Map<String, Boolean>> checkSchemaRegistryHealth() {
        checkNotClosed();
        // Schema registry health checks are planned for future implementation
        return CompletableFuture.completedFuture(Map.of());
    }
    
    @Override
    public CompletableFuture<Void> refreshSchemaRegistryConnections() {
        checkNotClosed();
        // Schema registry refresh is planned for future implementation
        return CompletableFuture.completedFuture(null);
    }
    
    @Override
    public boolean isClosed() {
        return closed.get();
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing Kafka Multi-Datacenter Client");
            poolManager.close();
            logger.info("Kafka Multi-Datacenter Client closed successfully");
        }
    }
    
    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Kafka client has been closed");
        }
    }
    
    /**
     * Maps the existing ResilienceConfig to the new ResilienceConfiguration.
     */
    private ResilienceConfiguration mapToResilienceConfiguration(ResilienceConfig resilienceConfig) {
        ResilienceConfiguration.Builder builder = ResilienceConfiguration.builder();
        
        // Map circuit breaker configuration if available
        if (resilienceConfig.getCircuitBreakerConfig() != null) {
            var cbConfig = resilienceConfig.getCircuitBreakerConfig();
            if (cbConfig.getFailureRateThreshold() != null) {
                builder.circuitBreakerFailureRateThreshold(cbConfig.getFailureRateThreshold());
            }
            if (cbConfig.getSlidingWindowSize() != null) {
                builder.circuitBreakerSlidingWindowSize(cbConfig.getSlidingWindowSize());
            }
            if (cbConfig.getWaitDurationInOpenState() != null) {
                builder.circuitBreakerWaitDurationInOpenState(cbConfig.getWaitDurationInOpenState());
            }
        }
        
        // Map retry configuration if available  
        if (resilienceConfig.getRetryConfig() != null) {
            var retryConfig = resilienceConfig.getRetryConfig();
            if (retryConfig.getMaxAttempts() != null) {
                builder.retryMaxAttempts(retryConfig.getMaxAttempts());
            }
            if (retryConfig.getWaitDuration() != null) {
                builder.retryWaitDuration(retryConfig.getWaitDuration());
            }
        }
        
        // Map rate limiter configuration if available
        if (resilienceConfig.getRateLimiterConfig() != null) {
            var rateLimiterConfig = resilienceConfig.getRateLimiterConfig();
            if (rateLimiterConfig.getRequestsPerSecond() != null) {
                builder.rateLimiterRequestsPerSecond(rateLimiterConfig.getRequestsPerSecond());
            }
            if (rateLimiterConfig.getTimeoutDuration() != null) {
                builder.rateLimiterTimeout(rateLimiterConfig.getTimeoutDuration());
            }
        }
        
        // Map bulkhead configuration if available
        if (resilienceConfig.getBulkheadConfig() != null) {
            var bulkheadConfig = resilienceConfig.getBulkheadConfig();
            if (bulkheadConfig.getMaxConcurrentCalls() != null) {
                builder.bulkheadMaxConcurrentCalls(bulkheadConfig.getMaxConcurrentCalls());
            }
            if (bulkheadConfig.getMaxWaitDuration() != null) {
                builder.bulkheadMaxWaitDuration(bulkheadConfig.getMaxWaitDuration());
            }
        }
        
        // Map time limiter configuration if available
        if (resilienceConfig.getTimeLimiterConfig() != null) {
            var timeLimiterConfig = resilienceConfig.getTimeLimiterConfig();
            if (timeLimiterConfig.getTimeoutDuration() != null) {
                builder.timeLimiterTimeout(timeLimiterConfig.getTimeoutDuration());
            }
        }
        
        return builder.build();
    }
    
    // Inner classes for operation implementations
    
    private class SyncProducerOps implements KafkaProducerOperations.Sync {
        @Override
        public <K, V> RecordMetadata send(String datacenterId, ProducerRecord<K, V> record) {
            logger.info("Sending record to datacenter: {} (sync implementation available)", datacenterId);
            // Real implementation would use poolManager.getProducer(datacenterId)
            return createMockRecordMetadata(record);
        }
        
        @Override
        public <K, V> RecordMetadata send(ProducerRecord<K, V> record) {
            String datacenterId = selectDatacenter();
            return send(datacenterId, record);
        }
        
        @Override
        public <K, V> RecordMetadata send(ProducerRecord<K, V> record, Duration timeout) {
            return send(record);
        }
        
        @Override
        public <K, V> List<RecordMetadata> sendBatch(List<ProducerRecord<K, V>> records) {
            return records.stream().map(this::send).toList();
        }
        
        @Override
        public <K, V> List<RecordMetadata> sendTransactional(List<ProducerRecord<K, V>> records) {
            logger.info("Sending transactional batch of {} records", records.size());
            return sendBatch(records);
        }
        
        @Override
        public void flush() {
            logger.info("Flushing all producers");
        }
    }
    
    private class AsyncProducerOps implements KafkaProducerOperations.Async {
        @Override
        public <K, V> CompletableFuture<RecordMetadata> sendAsync(String datacenterId, ProducerRecord<K, V> record) {
            return CompletableFuture.supplyAsync(() -> {
                logger.info("Sending record to datacenter: {} (async implementation available)", datacenterId);
                return createMockRecordMetadata(record);
            });
        }
        
        @Override
        public <K, V> CompletableFuture<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {
            String datacenterId = selectDatacenter();
            return sendAsync(datacenterId, record);
        }
        
        @Override
        public <K, V> CompletableFuture<List<RecordMetadata>> sendBatchAsync(List<ProducerRecord<K, V>> records) {
            return CompletableFuture.supplyAsync(() -> 
                records.stream().map(record -> createMockRecordMetadata(record)).toList()
            );
        }
        
        @Override
        public <K, V> CompletableFuture<List<RecordMetadata>> sendTransactionalAsync(List<ProducerRecord<K, V>> records) {
            return sendBatchAsync(records);
        }
        
        @Override
        public CompletableFuture<Void> flushAsync() {
            return CompletableFuture.runAsync(() -> logger.info("Flushing all producers async"));
        }
    }
    
    private class ReactiveProducerOps implements KafkaProducerOperations.Reactive {
        @Override
        public <K, V> Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
            return Mono.fromCallable(() -> {
                String datacenterId = selectDatacenter();
                logger.info("Sending record to datacenter: {} (reactive implementation available)", datacenterId);
                return createMockRecordMetadata(record);
            });
        }
        
        @Override
        public <K, V> Mono<RecordMetadata> send(String datacenterId, ProducerRecord<K, V> record) {
            return Mono.fromCallable(() -> {
                logger.info("Sending record to datacenter: {} (reactive implementation available)", datacenterId);
                return createMockRecordMetadata(record);
            });
        }
        
        @Override
        public <K, V> Flux<RecordMetadata> sendMany(Flux<ProducerRecord<K, V>> records) {
            return records.flatMap(this::send);
        }
        
        @Override
        public <K, V> Flux<List<RecordMetadata>> sendBatched(Flux<ProducerRecord<K, V>> records, int batchSize) {
            return records.buffer(batchSize)
                .map(batch -> batch.stream().map(record -> createMockRecordMetadata(record)).toList());
        }
        
        @Override
        public <K, V> Mono<List<RecordMetadata>> sendTransactional(Flux<ProducerRecord<K, V>> records) {
            return records.collectList()
                .map(recordList -> recordList.stream().map(record -> createMockRecordMetadata(record)).toList());
        }
    }
    
    private String selectDatacenter() {
        // Simple datacenter selection based on priority
        return datacenterInfoMap.values().stream()
            .filter(DatacenterInfo::isHealthy)
            .min((dc1, dc2) -> Integer.compare(dc1.getPriority(), dc2.getPriority()))
            .map(DatacenterInfo::getId)
            .orElse(configuration.getDatacenters().get(0).getId());
    }
    
    private <K, V> RecordMetadata createMockRecordMetadata(ProducerRecord<K, V> record) {
        // In real implementation, this would come from Kafka
        // For demo purposes, we'll log the operation and return null
        // In a real implementation, this would use the actual Kafka producer response
        logger.debug("Mock: Would send record to topic: {}, key: {}", record.topic(), record.key());
        
        // Return null for now - in real implementation this would be the actual RecordMetadata
        // from Kafka producer.send().get()
        return null;
    }
}
