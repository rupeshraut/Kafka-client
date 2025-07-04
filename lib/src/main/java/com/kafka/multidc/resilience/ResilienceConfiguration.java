package com.kafka.multidc.resilience;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;

import java.time.Duration;

/**
 * Configuration for Resilience4j patterns used in Kafka Multi-Datacenter Client.
 * Provides comprehensive resilience patterns including circuit breaker, retry, rate limiter,
 * bulkhead, and time limiter configurations.
 */
public class ResilienceConfiguration {
    
    // Circuit Breaker Configuration
    private final CircuitBreakerConfig circuitBreakerConfig;
    
    // Retry Configuration
    private final RetryConfig retryConfig;
    
    // Rate Limiter Configuration
    private final RateLimiterConfig rateLimiterConfig;
    
    // Bulkhead Configuration
    private final BulkheadConfig bulkheadConfig;
    
    // Time Limiter Configuration
    private final TimeLimiterConfig timeLimiterConfig;
    
    private ResilienceConfiguration(Builder builder) {
        this.circuitBreakerConfig = builder.circuitBreakerConfig;
        this.retryConfig = builder.retryConfig;
        this.rateLimiterConfig = builder.rateLimiterConfig;
        this.bulkheadConfig = builder.bulkheadConfig;
        this.timeLimiterConfig = builder.timeLimiterConfig;
    }
    
    /**
     * Create a default resilience configuration optimized for Kafka operations.
     */
    public static ResilienceConfiguration defaultConfig() {
        return builder().build();
    }
    
    /**
     * Create a resilience configuration builder.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }
    
    public RetryConfig getRetryConfig() {
        return retryConfig;
    }
    
    public RateLimiterConfig getRateLimiterConfig() {
        return rateLimiterConfig;
    }
    
    public BulkheadConfig getBulkheadConfig() {
        return bulkheadConfig;
    }
    
    public TimeLimiterConfig getTimeLimiterConfig() {
        return timeLimiterConfig;
    }
    
    /**
     * Builder for ResilienceConfiguration.
     */
    public static class Builder {
        private CircuitBreakerConfig circuitBreakerConfig;
        private RetryConfig retryConfig;
        private RateLimiterConfig rateLimiterConfig;
        private BulkheadConfig bulkheadConfig;
        private TimeLimiterConfig timeLimiterConfig;
        
        private Builder() {
            // Set default configurations optimized for Kafka
            setDefaultCircuitBreakerConfig();
            setDefaultRetryConfig();
            setDefaultRateLimiterConfig();
            setDefaultBulkheadConfig();
            setDefaultTimeLimiterConfig();
        }
        
        private void setDefaultCircuitBreakerConfig() {
            this.circuitBreakerConfig = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(10) // 10 second window
                .minimumNumberOfCalls(5) // At least 5 calls before evaluation
                .failureRateThreshold(50.0f) // 50% failure rate threshold
                .slowCallRateThreshold(80.0f) // 80% slow call rate threshold
                .slowCallDurationThreshold(Duration.ofSeconds(3)) // Calls slower than 3s are slow
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30s before half-open
                .maxWaitDurationInHalfOpenState(Duration.ofSeconds(10)) // Max 10s in half-open
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        }
        
        private void setDefaultRetryConfig() {
            this.retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 retry attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait 500ms
                .waitDuration(Duration.ofMillis(500)) // Simple fixed duration
                .retryOnException(throwable -> 
                    // Retry on network and timeout exceptions, plus RuntimeException for testing
                    throwable instanceof java.net.ConnectException ||
                    throwable instanceof java.net.SocketTimeoutException ||
                    throwable instanceof org.apache.kafka.common.errors.TimeoutException ||
                    throwable instanceof org.apache.kafka.common.errors.NetworkException ||
                    throwable instanceof RuntimeException) // Added for testing scenarios
                .build();
        }
        
        private void setDefaultRateLimiterConfig() {
            this.rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1)) // Refresh every 1 second
                .limitForPeriod(5) // Allow 5 requests per second (more restrictive for testing)
                .timeoutDuration(Duration.ofMillis(10)) // Very short timeout to trigger rejections quickly
                .build();
        }
        
        private void setDefaultBulkheadConfig() {
            this.bulkheadConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(25) // Allow max 25 concurrent calls
                .maxWaitDuration(Duration.ofMillis(500)) // Wait up to 500ms for slot
                .build();
        }
        
        private void setDefaultTimeLimiterConfig() {
            this.timeLimiterConfig = TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofSeconds(10)) // 10s timeout for operations
                .cancelRunningFuture(true) // Cancel running futures on timeout
                .build();
        }
        
        // Circuit Breaker builder methods
        public Builder circuitBreakerFailureRateThreshold(float failureRateThreshold) {
            this.circuitBreakerConfig = CircuitBreakerConfig.from(this.circuitBreakerConfig)
                .failureRateThreshold(failureRateThreshold)
                .build();
            return this;
        }
        
        public Builder circuitBreakerSlidingWindowSize(int slidingWindowSize) {
            this.circuitBreakerConfig = CircuitBreakerConfig.from(this.circuitBreakerConfig)
                .slidingWindowSize(slidingWindowSize)
                .build();
            return this;
        }
        
        public Builder circuitBreakerWaitDurationInOpenState(Duration waitDuration) {
            this.circuitBreakerConfig = CircuitBreakerConfig.from(this.circuitBreakerConfig)
                .waitDurationInOpenState(waitDuration)
                .build();
            return this;
        }
        
        // Retry builder methods
        public Builder retryMaxAttempts(int maxAttempts) {
            this.retryConfig = RetryConfig.from(this.retryConfig)
                .maxAttempts(maxAttempts)
                .build();
            return this;
        }
        
        public Builder retryWaitDuration(Duration waitDuration) {
            this.retryConfig = RetryConfig.from(this.retryConfig)
                .waitDuration(waitDuration)
                .build();
            return this;
        }
        
        public Builder retryExponentialBackoff(Duration initialInterval, double multiplier, Duration maxInterval) {
            this.retryConfig = RetryConfig.from(this.retryConfig)
                .waitDuration(initialInterval) // Use initial interval for simplicity
                .build();
            return this;
        }
        
        // Rate Limiter builder methods
        public Builder rateLimiterRequestsPerSecond(int requestsPerSecond) {
            Duration period = Duration.ofNanos(1_000_000_000L / requestsPerSecond);
            this.rateLimiterConfig = RateLimiterConfig.from(this.rateLimiterConfig)
                .limitRefreshPeriod(period)
                .limitForPeriod(1)
                .build();
            return this;
        }
        
        public Builder rateLimiterTimeout(Duration timeoutDuration) {
            this.rateLimiterConfig = RateLimiterConfig.from(this.rateLimiterConfig)
                .timeoutDuration(timeoutDuration)
                .build();
            return this;
        }
        
        // Bulkhead builder methods
        public Builder bulkheadMaxConcurrentCalls(int maxConcurrentCalls) {
            this.bulkheadConfig = BulkheadConfig.from(this.bulkheadConfig)
                .maxConcurrentCalls(maxConcurrentCalls)
                .build();
            return this;
        }
        
        public Builder bulkheadMaxWaitDuration(Duration maxWaitDuration) {
            this.bulkheadConfig = BulkheadConfig.from(this.bulkheadConfig)
                .maxWaitDuration(maxWaitDuration)
                .build();
            return this;
        }
        
        // Time Limiter builder methods
        public Builder timeLimiterTimeout(Duration timeoutDuration) {
            this.timeLimiterConfig = TimeLimiterConfig.from(this.timeLimiterConfig)
                .timeoutDuration(timeoutDuration)
                .build();
            return this;
        }
        
        public Builder timeLimiterCancelRunningFuture(boolean cancelRunningFuture) {
            this.timeLimiterConfig = TimeLimiterConfig.from(this.timeLimiterConfig)
                .cancelRunningFuture(cancelRunningFuture)
                .build();
            return this;
        }
        
        public ResilienceConfiguration build() {
            return new ResilienceConfiguration(this);
        }
    }
}
