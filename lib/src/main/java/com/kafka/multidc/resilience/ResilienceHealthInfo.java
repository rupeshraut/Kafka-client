package com.kafka.multidc.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Health information for resilience patterns across all datacenters.
 */
public class ResilienceHealthInfo {
    
    private final Map<String, CircuitBreaker.State> circuitBreakerStates;
    private final Map<String, ResilienceMetrics> datacenterMetrics;
    
    private ResilienceHealthInfo(Builder builder) {
        this.circuitBreakerStates = Collections.unmodifiableMap(builder.circuitBreakerStates);
        this.datacenterMetrics = Collections.unmodifiableMap(builder.datacenterMetrics);
    }
    
    public Map<String, CircuitBreaker.State> getCircuitBreakerStates() {
        return circuitBreakerStates;
    }
    
    public Map<String, ResilienceMetrics> getDatacenterMetrics() {
        return datacenterMetrics;
    }
    
    public boolean isDatacenterHealthy(String datacenterName) {
        String cbName = "kafka-" + datacenterName;
        CircuitBreaker.State state = circuitBreakerStates.get(cbName);
        return state == null || state == CircuitBreaker.State.CLOSED || state == CircuitBreaker.State.HALF_OPEN;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private final Map<String, CircuitBreaker.State> circuitBreakerStates = new HashMap<>();
        private final Map<String, ResilienceMetrics> datacenterMetrics = new HashMap<>();
        
        public Builder addCircuitBreakerState(String name, CircuitBreaker.State state) {
            this.circuitBreakerStates.put(name, state);
            return this;
        }
        
        public Builder addDatacenterMetrics(String datacenterName, ResilienceMetrics metrics) {
            this.datacenterMetrics.put(datacenterName, metrics);
            return this;
        }
        
        public ResilienceHealthInfo build() {
            return new ResilienceHealthInfo(this);
        }
    }
    
    /**
     * Metrics for resilience patterns in a specific datacenter.
     */
    public static class ResilienceMetrics {
        private final int retryAttempts;
        private final int rateLimitRejections;
        private final int bulkheadRejections;
        private final int timeoutFailures;
        
        public ResilienceMetrics(int retryAttempts, int rateLimitRejections, 
                               int bulkheadRejections, int timeoutFailures) {
            this.retryAttempts = retryAttempts;
            this.rateLimitRejections = rateLimitRejections;
            this.bulkheadRejections = bulkheadRejections;
            this.timeoutFailures = timeoutFailures;
        }
        
        public int getRetryAttempts() { return retryAttempts; }
        public int getRateLimitRejections() { return rateLimitRejections; }
        public int getBulkheadRejections() { return bulkheadRejections; }
        public int getTimeoutFailures() { return timeoutFailures; }
    }
}
