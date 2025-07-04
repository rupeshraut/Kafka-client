package com.kafka.multidc.model;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DatacenterInfo {
    private final String id;
    private final String region;
    private final String bootstrapServers;
    private final int priority;
    private final AtomicBoolean healthy;
    private final AtomicReference<Duration> latency;
    private final AtomicReference<Instant> lastHealthCheck;
    
    public DatacenterInfo(String id, String region, String bootstrapServers, int priority, 
                         boolean healthy, Duration latency, Instant lastHealthCheck) {
        this.id = id;
        this.region = region;
        this.bootstrapServers = bootstrapServers;
        this.priority = priority;
        this.healthy = new AtomicBoolean(healthy);
        this.latency = new AtomicReference<>(latency);
        this.lastHealthCheck = new AtomicReference<>(lastHealthCheck);
    }
    
    // Getters
    public String getId() { return id; }
    public String getRegion() { return region; }
    public String getBootstrapServers() { return bootstrapServers; }
    public int getPriority() { return priority; }
    public boolean isHealthy() { return healthy.get(); }
    public Duration getLatency() { return latency.get(); }
    public Instant getLastHealthCheck() { return lastHealthCheck.get(); }
    
    // Update methods
    public void updateHealth(boolean healthy) {
        this.healthy.set(healthy);
    }
    
    public void updateLatency(Duration latency) {
        this.latency.set(latency);
    }
    
    public void updateLastHealthCheck(Instant timestamp) {
        this.lastHealthCheck.set(timestamp);
    }
    
    @Override
    public String toString() {
        return String.format("DatacenterInfo{id='%s', region='%s', healthy=%s, latency=%s, priority=%d}", 
            id, region, healthy.get(), latency.get(), priority);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatacenterInfo that = (DatacenterInfo) o;
        return id.equals(that.id);
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
