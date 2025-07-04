package com.kafka.multidc.pool;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConnectionPoolManager {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectionPoolManager.class);
    
    private final KafkaDatacenterConfiguration configuration;
    private final MeterRegistry meterRegistry;
    private final Map<String, ConnectionPool> connectionPools;
    private final Map<String, KafkaConnectionMetrics> metricsMap;
    private final ScheduledExecutorService healthCheckExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public KafkaConnectionPoolManager(KafkaDatacenterConfiguration configuration, MeterRegistry meterRegistry) {
        this.configuration = configuration;
        this.meterRegistry = meterRegistry;
        this.connectionPools = new ConcurrentHashMap<>();
        this.metricsMap = new ConcurrentHashMap<>();
        this.healthCheckExecutor = Executors.newScheduledThreadPool(2);
        
        initializeConnectionPools();
        startHealthChecking();
    }
    
    private void initializeConnectionPools() {
        for (KafkaDatacenterEndpoint endpoint : configuration.getDatacenters()) {
            ConnectionPool pool = new ConnectionPool(endpoint, meterRegistry);
            connectionPools.put(endpoint.getId(), pool);
            metricsMap.put(endpoint.getId(), pool.getMetrics());
            
            logger.info("Initialized connection pool for datacenter: {}", endpoint.getId());
        }
    }
    
    private void startHealthChecking() {
        Duration interval = configuration.getHealthCheckInterval();
        healthCheckExecutor.scheduleAtFixedRate(
            this::performHealthChecks,
            0,
            interval.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }
    
    private void performHealthChecks() {
        if (closed.get()) {
            return;
        }
        
        connectionPools.values().parallelStream().forEach(pool -> {
            try {
                pool.performHealthCheck();
            } catch (Exception e) {
                logger.warn("Health check failed for datacenter: {}", pool.getDatacenterId(), e);
            }
        });
    }
    
    public <K, V> KafkaProducer<K, V> getProducer(String datacenterId) {
        ConnectionPool pool = connectionPools.get(datacenterId);
        if (pool == null) {
            throw new IllegalArgumentException("Unknown datacenter: " + datacenterId);
        }
        return pool.getProducer();
    }
    
    public <K, V> KafkaConsumer<K, V> getConsumer(String datacenterId, String groupId) {
        ConnectionPool pool = connectionPools.get(datacenterId);
        if (pool == null) {
            throw new IllegalArgumentException("Unknown datacenter: " + datacenterId);
        }
        return pool.getConsumer(groupId);
    }
    
    public <K, V> KafkaConsumer<K, V> getConsumer(String datacenterId) {
        return getConsumer(datacenterId, "default-consumer-group");
    }
    
    public boolean isHealthy(String datacenterId) {
        ConnectionPool pool = connectionPools.get(datacenterId);
        return pool != null && pool.isHealthy();
    }
    
    public Map<String, Boolean> getAllHealthStatus() {
        Map<String, Boolean> healthStatus = new ConcurrentHashMap<>();
        connectionPools.forEach((dcId, pool) -> healthStatus.put(dcId, pool.isHealthy()));
        return healthStatus;
    }
    
    public KafkaConnectionMetrics getMetrics(String datacenterId) {
        return metricsMap.get(datacenterId);
    }
    
    public AggregatedMetrics getAggregatedMetrics() {
        return new AggregatedMetrics(connectionPools);
    }
    
    public void drainPool(String datacenterId) {
        ConnectionPool pool = connectionPools.get(datacenterId);
        if (pool != null) {
            pool.drain();
        }
    }
    
    public void maintainAllPools() {
        connectionPools.values().forEach(ConnectionPool::performMaintenance);
    }
    
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing Kafka connection pool manager");
            
            healthCheckExecutor.shutdown();
            try {
                if (!healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    healthCheckExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                healthCheckExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            connectionPools.values().parallelStream().forEach(ConnectionPool::close);
            connectionPools.clear();
            metricsMap.clear();
        }
    }
    
    public ScheduledExecutorService getExecutorService() {
        return healthCheckExecutor;
    }
    
    private static class ConnectionPool {
        private final KafkaDatacenterEndpoint endpoint;
        private final MeterRegistry meterRegistry;
        private final Map<String, KafkaProducer<?, ?>> producers;
        private final Map<String, KafkaConsumer<?, ?>> consumers;
        private final AtomicBoolean healthy = new AtomicBoolean(true);
        private final AtomicInteger activeConnections = new AtomicInteger(0);
        private final AtomicLong lastHealthCheck = new AtomicLong(System.currentTimeMillis());
        private final KafkaConnectionMetrics metrics;
        private final CircuitBreaker circuitBreaker;
        private final Retry retry;
        
        ConnectionPool(KafkaDatacenterEndpoint endpoint, MeterRegistry meterRegistry) {
            this.endpoint = endpoint;
            this.meterRegistry = meterRegistry;
            this.producers = new ConcurrentHashMap<>();
            this.consumers = new ConcurrentHashMap<>();
            this.metrics = new KafkaConnectionMetrics(endpoint.getId(), meterRegistry);
            
            // Initialize resilience patterns
            this.circuitBreaker = CircuitBreaker.ofDefaults("kafka-" + endpoint.getId());
            this.retry = Retry.ofDefaults("kafka-retry-" + endpoint.getId());
            
            // Register metrics
            Gauge.builder("kafka.pool.active.connections", this, pool -> pool.activeConnections.get())
                .tag("datacenter", endpoint.getId())
                .register(meterRegistry);
            
            Gauge.builder("kafka.pool.health", this, pool -> pool.healthy.get() ? 1 : 0)
                .tag("datacenter", endpoint.getId())
                .register(meterRegistry);
        }
        
        @SuppressWarnings("unchecked")
        <K, V> KafkaProducer<K, V> getProducer() {
            String key = "default-producer";
            return (KafkaProducer<K, V>) producers.computeIfAbsent(key, k -> {
                activeConnections.incrementAndGet();
                metrics.incrementConnectionsCreated();
                
                Properties props = createProducerProperties();
                return new KafkaProducer<>(props);
            });
        }
        
        @SuppressWarnings("unchecked")
        <K, V> KafkaConsumer<K, V> getConsumer(String groupId) {
            String key = "consumer-" + groupId;
            return (KafkaConsumer<K, V>) consumers.computeIfAbsent(key, k -> {
                activeConnections.incrementAndGet();
                metrics.incrementConnectionsCreated();
                
                Properties props = createConsumerProperties(groupId);
                return new KafkaConsumer<>(props);
            });
        }
        
        private Properties createProducerProperties() {
            Properties props = new Properties();
            props.put("bootstrap.servers", endpoint.getBootstrapServers());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("compression.type", endpoint.getCompressionType());
            props.put("acks", "all");
            props.put("retries", 3);
            props.put("batch.size", 16384);
            props.put("linger.ms", 5);
            props.put("buffer.memory", 33554432);                props.put("enable.idempotence", endpoint.isIdempotenceEnabled());
            
            // Add any additional properties from endpoint
            if (endpoint.getAdditionalProperties() != null) {
                props.putAll(endpoint.getAdditionalProperties());
            }
            
            return props;
        }
        
        private Properties createConsumerProperties(String groupId) {
            Properties props = new Properties();
            props.put("bootstrap.servers", endpoint.getBootstrapServers());
            props.put("group.id", groupId);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "earliest");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("fetch.min.bytes", 1);
            props.put("fetch.max.wait.ms", 500);
            
            // Add any additional properties from endpoint
            if (endpoint.getAdditionalProperties() != null) {
                props.putAll(endpoint.getAdditionalProperties());
            }
            
            return props;
        }
        
        void performHealthCheck() {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                // Simple health check - create a temporary admin client and list topics
                Properties props = new Properties();
                props.put("bootstrap.servers", endpoint.getBootstrapServers());
                props.put("request.timeout.ms", "5000");
                props.put("connections.max.idle.ms", "10000");
                
                // In a real implementation, you'd create an AdminClient and test connectivity
                // For now, we'll simulate a health check
                boolean wasHealthy = healthy.get();
                boolean isHealthy = circuitBreaker.executeSupplier(() -> {
                    // Simulate connectivity check
                    return true; // Replace with actual admin client check
                });
                
                healthy.set(isHealthy);
                lastHealthCheck.set(System.currentTimeMillis());
                
                if (wasHealthy != isHealthy) {
                    logger.info("Datacenter {} health changed: {}", endpoint.getId(), isHealthy ? "HEALTHY" : "UNHEALTHY");
                }
                
                sample.stop(Timer.builder("kafka.health.check.duration")
                    .tag("datacenter", endpoint.getId())
                    .register(meterRegistry));
                
            } catch (Exception e) {
                healthy.set(false);
                logger.warn("Health check failed for datacenter: {}", endpoint.getId(), e);
                
                sample.stop(Timer.builder("kafka.health.check.duration")
                    .tag("datacenter", endpoint.getId())
                    .tag("error", "true")
                    .register(meterRegistry));
            }
        }
        
        void performMaintenance() {
            // Remove inactive connections
            producers.entrySet().removeIf(entry -> {
                // In a real implementation, check if producer is still active
                return false;
            });
            
            consumers.entrySet().removeIf(entry -> {
                // In a real implementation, check if consumer is still active
                return false;
            });
        }
        
        void drain() {
            logger.info("Draining connection pool for datacenter: {}", endpoint.getId());
            
            // Close all producers
            producers.values().forEach(producer -> {
                try {
                    producer.close(Duration.ofSeconds(5));
                    activeConnections.decrementAndGet();
                    metrics.incrementConnectionsClosed();
                } catch (Exception e) {
                    logger.warn("Error closing producer", e);
                }
            });
            producers.clear();
            
            // Close all consumers
            consumers.values().forEach(consumer -> {
                try {
                    consumer.close(Duration.ofSeconds(5));
                    activeConnections.decrementAndGet();
                    metrics.incrementConnectionsClosed();
                } catch (Exception e) {
                    logger.warn("Error closing consumer", e);
                }
            });
            consumers.clear();
        }
        
        void close() {
            drain();
        }
        
        boolean isHealthy() {
            return healthy.get();
        }
        
        String getDatacenterId() {
            return endpoint.getId();
        }
        
        KafkaConnectionMetrics getMetrics() {
            return metrics;
        }
    }
    
    public static class AggregatedMetrics {
        private final int totalPools;
        private final int healthyPools;
        private final int totalConnections;
        private final long totalConnectionsCreated;
        private final long totalConnectionsClosed;
        
        AggregatedMetrics(Map<String, ConnectionPool> connectionPools) {
            this.totalPools = connectionPools.size();
            this.healthyPools = (int) connectionPools.values().stream()
                .mapToInt(pool -> pool.isHealthy() ? 1 : 0)
                .sum();
            this.totalConnections = connectionPools.values().stream()
                .mapToInt(pool -> pool.activeConnections.get())
                .sum();
            this.totalConnectionsCreated = connectionPools.values().stream()
                .mapToLong(pool -> pool.getMetrics().getConnectionsCreated())
                .sum();
            this.totalConnectionsClosed = connectionPools.values().stream()
                .mapToLong(pool -> pool.getMetrics().getConnectionsClosed())
                .sum();
        }
        
        public int getTotalPools() { return totalPools; }
        public int getHealthyPools() { return healthyPools; }
        public int getTotalConnections() { return totalConnections; }
        public long getTotalConnectionsCreated() { return totalConnectionsCreated; }
        public long getTotalConnectionsClosed() { return totalConnectionsClosed; }
        public double getHealthRatio() { return totalPools > 0 ? (double) healthyPools / totalPools : 0.0; }
    }
}
