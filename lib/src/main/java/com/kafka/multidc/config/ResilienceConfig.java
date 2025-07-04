package com.kafka.multidc.config;

import java.time.Duration;

/**
 * Configuration for resilience patterns used in Kafka Multi-Datacenter Client.
 */
public class ResilienceConfig {
    
    private CircuitBreakerConfig circuitBreakerConfig;
    private RetryConfig retryConfig;
    private RateLimiterConfig rateLimiterConfig;
    private BulkheadConfig bulkheadConfig;
    private TimeLimiterConfig timeLimiterConfig;
    
    public static ResilienceConfig defaultConfig() {
        // Return a simple config without any complex initialization
        return builder().build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public CircuitBreakerConfig getCircuitBreakerConfig() { return circuitBreakerConfig; }
    public RetryConfig getRetryConfig() { return retryConfig; }
    public RateLimiterConfig getRateLimiterConfig() { return rateLimiterConfig; }
    public BulkheadConfig getBulkheadConfig() { return bulkheadConfig; }
    public TimeLimiterConfig getTimeLimiterConfig() { return timeLimiterConfig; }
    
    public static class Builder {
        private final ResilienceConfig config = new ResilienceConfig();
        
        public Builder circuitBreakerConfig(CircuitBreakerConfig circuitBreakerConfig) {
            config.circuitBreakerConfig = circuitBreakerConfig;
            return this;
        }
        
        public Builder retryConfig(RetryConfig retryConfig) {
            config.retryConfig = retryConfig;
            return this;
        }
        
        public Builder rateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
            config.rateLimiterConfig = rateLimiterConfig;
            return this;
        }
        
        public Builder bulkheadConfig(BulkheadConfig bulkheadConfig) {
            config.bulkheadConfig = bulkheadConfig;
            return this;
        }
        
        public Builder timeLimiterConfig(TimeLimiterConfig timeLimiterConfig) {
            config.timeLimiterConfig = timeLimiterConfig;
            return this;
        }
        
        public ResilienceConfig build() {
            return config;
        }
    }
    
    // Inner configuration classes
    public static class CircuitBreakerConfig {
        private Float failureRateThreshold;
        private Integer slidingWindowSize;
        private Duration waitDurationInOpenState;
        private Integer minimumNumberOfCalls;
        
        public static Builder builder() { return new Builder(); }
        
        public Float getFailureRateThreshold() { return failureRateThreshold; }
        public Integer getSlidingWindowSize() { return slidingWindowSize; }
        public Duration getWaitDurationInOpenState() { return waitDurationInOpenState; }
        public Integer getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
        
        public static class Builder {
            private final CircuitBreakerConfig config = new CircuitBreakerConfig();
            
            public Builder failureRateThreshold(float threshold) {
                config.failureRateThreshold = threshold;
                return this;
            }
            
            public Builder slidingWindowSize(int size) {
                config.slidingWindowSize = size;
                return this;
            }
            
            public Builder waitDurationInOpenState(Duration duration) {
                config.waitDurationInOpenState = duration;
                return this;
            }
            
            public Builder minimumNumberOfCalls(int calls) {
                config.minimumNumberOfCalls = calls;
                return this;
            }
            
            public CircuitBreakerConfig build() { return config; }
        }
    }
    
    public static class RetryConfig {
        private Integer maxAttempts;
        private Duration waitDuration;
        
        public static Builder builder() { return new Builder(); }
        
        public Integer getMaxAttempts() { return maxAttempts; }
        public Duration getWaitDuration() { return waitDuration; }
        
        public static class Builder {
            private final RetryConfig config = new RetryConfig();
            
            public Builder maxAttempts(int attempts) {
                config.maxAttempts = attempts;
                return this;
            }
            
            public Builder waitDuration(Duration duration) {
                config.waitDuration = duration;
                return this;
            }
            
            public Builder exponentialBackoff(Duration initial, double multiplier, Duration max) {
                // For simplification, just use the initial duration
                config.waitDuration = initial;
                return this;
            }
            
            public RetryConfig build() { return config; }
        }
    }
    
    public static class RateLimiterConfig {
        private Integer requestsPerSecond;
        private Duration timeoutDuration;
        
        public static Builder builder() { return new Builder(); }
        
        public Integer getRequestsPerSecond() { return requestsPerSecond; }
        public Duration getTimeoutDuration() { return timeoutDuration; }
        
        public static class Builder {
            private final RateLimiterConfig config = new RateLimiterConfig();
            
            public Builder requestsPerSecond(int requests) {
                config.requestsPerSecond = requests;
                return this;
            }
            
            public Builder timeoutDuration(Duration duration) {
                config.timeoutDuration = duration;
                return this;
            }
            
            public RateLimiterConfig build() { return config; }
        }
    }
    
    public static class BulkheadConfig {
        private Integer maxConcurrentCalls;
        private Duration maxWaitDuration;
        
        public static Builder builder() { return new Builder(); }
        
        public Integer getMaxConcurrentCalls() { return maxConcurrentCalls; }
        public Duration getMaxWaitDuration() { return maxWaitDuration; }
        
        public static class Builder {
            private final BulkheadConfig config = new BulkheadConfig();
            
            public Builder maxConcurrentCalls(int calls) {
                config.maxConcurrentCalls = calls;
                return this;
            }
            
            public Builder maxWaitDuration(Duration duration) {
                config.maxWaitDuration = duration;
                return this;
            }
            
            public BulkheadConfig build() { return config; }
        }
    }
    
    public static class TimeLimiterConfig {
        private Duration timeoutDuration;
        private Boolean cancelRunningFuture;
        
        public static Builder builder() { return new Builder(); }
        
        public Duration getTimeoutDuration() { return timeoutDuration; }
        public Boolean getCancelRunningFuture() { return cancelRunningFuture; }
        
        public static class Builder {
            private final TimeLimiterConfig config = new TimeLimiterConfig();
            
            public Builder timeoutDuration(Duration duration) {
                config.timeoutDuration = duration;
                return this;
            }
            
            public Builder cancelRunningFuture(boolean cancel) {
                config.cancelRunningFuture = cancel;
                return this;
            }
            
            public TimeLimiterConfig build() { return config; }
        }
    }
}
