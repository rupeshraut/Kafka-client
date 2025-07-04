package com.kafka.multidc.pool;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaConnectionMetrics {
    
    private final String datacenterId;
    private final MeterRegistry meterRegistry;
    
    // Connection metrics
    private final AtomicLong connectionsCreated = new AtomicLong(0);
    private final AtomicLong connectionsClosed = new AtomicLong(0);
    private final AtomicLong connectionsActive = new AtomicLong(0);
    private final AtomicLong connectionErrors = new AtomicLong(0);
    
    // Performance metrics
    private final AtomicReference<Duration> averageLatency = new AtomicReference<>(Duration.ZERO);
    private final AtomicReference<Instant> lastHealthCheck = new AtomicReference<>(Instant.now());
    private final AtomicLong requestCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    
    // Micrometer counters and timers
    private final Counter connectionsCreatedCounter;
    private final Counter connectionsClosedCounter;
    private final Counter connectionErrorsCounter;
    private final Counter requestCounter;
    private final Counter errorCounter;
    private final Timer latencyTimer;
    
    public KafkaConnectionMetrics(String datacenterId, MeterRegistry meterRegistry) {
        this.datacenterId = datacenterId;
        this.meterRegistry = meterRegistry;
        
        // Initialize Micrometer metrics
        this.connectionsCreatedCounter = Counter.builder("kafka.connections.created")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        this.connectionsClosedCounter = Counter.builder("kafka.connections.closed")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        this.connectionErrorsCounter = Counter.builder("kafka.connections.errors")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        this.requestCounter = Counter.builder("kafka.requests.total")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        this.errorCounter = Counter.builder("kafka.requests.errors")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        this.latencyTimer = Timer.builder("kafka.request.latency")
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
        
        // Register gauges
        Gauge.builder("kafka.connections.active", this, metrics -> metrics.connectionsActive.get())
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
            
        Gauge.builder("kafka.latency.average.ms", this, metrics -> metrics.averageLatency.get().toMillis())
            .tag("datacenter", datacenterId)
            .register(meterRegistry);
    }
    
    public void incrementConnectionsCreated() {
        connectionsCreated.incrementAndGet();
        connectionsActive.incrementAndGet();
        connectionsCreatedCounter.increment();
    }
    
    public void incrementConnectionsClosed() {
        connectionsClosed.incrementAndGet();
        connectionsActive.decrementAndGet();
        connectionsClosedCounter.increment();
    }
    
    public void incrementConnectionErrors() {
        connectionErrors.incrementAndGet();
        connectionErrorsCounter.increment();
    }
    
    public void recordRequest() {
        requestCount.incrementAndGet();
        requestCounter.increment();
    }
    
    public void recordError() {
        errorCount.incrementAndGet();
        errorCounter.increment();
    }
    
    public void recordLatency(Duration latency) {
        averageLatency.set(latency);
        latencyTimer.record(latency);
    }
    
    public void updateLastHealthCheck() {
        lastHealthCheck.set(Instant.now());
    }
    
    public Timer.Sample startLatencyTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void resetMetrics() {
        connectionsCreated.set(0);
        connectionsClosed.set(0);
        connectionsActive.set(0);
        connectionErrors.set(0);
        requestCount.set(0);
        errorCount.set(0);
        averageLatency.set(Duration.ZERO);
    }
    
    // Getters
    public String getDatacenterId() {
        return datacenterId;
    }
    
    public long getConnectionsCreated() {
        return connectionsCreated.get();
    }
    
    public long getConnectionsClosed() {
        return connectionsClosed.get();
    }
    
    public long getConnectionsActive() {
        return connectionsActive.get();
    }
    
    public long getConnectionErrors() {
        return connectionErrors.get();
    }
    
    public long getRequestCount() {
        return requestCount.get();
    }
    
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public Duration getAverageLatency() {
        return averageLatency.get();
    }
    
    public Instant getLastHealthCheck() {
        return lastHealthCheck.get();
    }
    
    public double getErrorRate() {
        long total = requestCount.get();
        return total > 0 ? (double) errorCount.get() / total : 0.0;
    }
    
    public boolean isHealthy() {
        // Consider healthy if last health check was recent and error rate is low
        Instant lastCheck = lastHealthCheck.get();
        Duration timeSinceLastCheck = Duration.between(lastCheck, Instant.now());
        return timeSinceLastCheck.toMinutes() < 5 && getErrorRate() < 0.1;
    }
    
    @Override
    public String toString() {
        return String.format("KafkaConnectionMetrics{" +
            "datacenterId='%s', " +
            "connectionsActive=%d, " +
            "connectionsCreated=%d, " +
            "connectionsClosed=%d, " +
            "connectionErrors=%d, " +
            "requestCount=%d, " +
            "errorCount=%d, " +
            "averageLatency=%s, " +
            "errorRate=%.2f" +
            "}", 
            datacenterId, 
            connectionsActive.get(), 
            connectionsCreated.get(), 
            connectionsClosed.get(), 
            connectionErrors.get(), 
            requestCount.get(), 
            errorCount.get(), 
            averageLatency.get(), 
            getErrorRate());
    }
}
