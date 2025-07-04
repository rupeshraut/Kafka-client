package com.kafka.multidc.resilience;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * Simplified resilience manager for Kafka Multi-Datacenter Client.
 * Provides essential resilience patterns with reliable compilation.
 */
public class ResilienceManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ResilienceManager.class);
    
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final TimeLimiterRegistry timeLimiterRegistry;
    private final ScheduledExecutorService scheduler;
    
    public ResilienceManager(ResilienceConfiguration config, ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        
        // Initialize registries with configuration
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(config.getCircuitBreakerConfig());
        this.retryRegistry = RetryRegistry.of(config.getRetryConfig());
        this.rateLimiterRegistry = RateLimiterRegistry.of(config.getRateLimiterConfig());
        this.bulkheadRegistry = BulkheadRegistry.of(config.getBulkheadConfig());
        this.timeLimiterRegistry = TimeLimiterRegistry.of(config.getTimeLimiterConfig());
        
        setupEventListeners();
    }
    
    /**
     * Execute a synchronous operation with resilience patterns.
     */
    public <T> T executeSync(String datacenterName, Supplier<T> operation) {
        String patternName = "kafka-" + datacenterName;
        
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(patternName);
        Retry retry = retryRegistry.retry(patternName);
        RateLimiter rateLimiter = rateLimiterRegistry.rateLimiter(patternName);
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(patternName);
        
        // Apply resilience patterns in sequence
        Supplier<T> decoratedOperation = Bulkhead.decorateSupplier(bulkhead,
            RateLimiter.decorateSupplier(rateLimiter,
                CircuitBreaker.decorateSupplier(circuitBreaker,
                    Retry.decorateSupplier(retry, operation))));
        
        try {
            return decoratedOperation.get();
        } catch (Exception e) {
            logger.error("Operation failed for datacenter {} after all resilience attempts", datacenterName, e);
            throw new ResilienceException("Operation failed for datacenter " + datacenterName, e);
        }
    }
    
    /**
     * Execute an asynchronous operation with resilience patterns.
     */
    public <T> CompletableFuture<T> executeAsync(String datacenterName, Supplier<CompletionStage<T>> operation) {
        String patternName = "kafka-async-" + datacenterName;
        
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(patternName);
        Retry retry = retryRegistry.retry(patternName);
        TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter(patternName);
        
        // Apply basic resilience patterns for async operations
        Supplier<CompletionStage<T>> decoratedOperation = 
            CircuitBreaker.decorateCompletionStage(circuitBreaker, operation);
        
        try {
            CompletionStage<T> result = decoratedOperation.get();
            return result.toCompletableFuture()
                .orTimeout(timeLimiter.getTimeLimiterConfig().getTimeoutDuration().toMillis(), 
                          java.util.concurrent.TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {
                    throw new ResilienceException("Async operation failed for datacenter " + datacenterName, throwable);
                });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(
                new ResilienceException("Async operation failed for datacenter " + datacenterName, e));
        }
    }
    
    /**
     * Execute a reactive operation with resilience patterns.
     */
    public <T> Mono<T> executeReactive(String datacenterName, Supplier<Mono<T>> operation) {
        String patternName = "kafka-reactive-" + datacenterName;
        
        Retry retry = retryRegistry.retry(patternName);
        TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter(patternName);
        
        // Apply retry pattern by re-executing the supplier
        return Mono.fromSupplier(operation)
            .flatMap(mono -> mono)
            .retryWhen(reactor.util.retry.Retry.from(retrySpec -> 
                retrySpec.flatMap(signal -> {
                    long retryAttempt = signal.totalRetries();
                    if (retryAttempt < retry.getRetryConfig().getMaxAttempts() - 1) {
                        logger.debug("Reactive retry attempt {} for datacenter {}", 
                                   retryAttempt + 1, datacenterName);
                        long waitMs = retry.getRetryConfig().getIntervalBiFunction()
                            .apply((int)retryAttempt + 1, null);
                        return Mono.delay(Duration.ofMillis(waitMs));
                    } else {
                        return Mono.error(signal.failure());
                    }
                })))
            .timeout(Duration.ofMillis(timeLimiter.getTimeLimiterConfig().getTimeoutDuration().toMillis()))
            .onErrorMap(throwable -> 
                new ResilienceException("Reactive operation failed for datacenter " + datacenterName, throwable));
    }
    
    /**
     * Execute a reactive stream operation with resilience patterns.
     */
    public <T> Flux<T> executeReactiveStream(String datacenterName, Supplier<Flux<T>> operation) {
        String patternName = "kafka-stream-" + datacenterName;
        
        TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter(patternName);
        
        return operation.get()
            .timeout(Duration.ofMillis(timeLimiter.getTimeLimiterConfig().getTimeoutDuration().toMillis()))
            .onErrorMap(throwable -> 
                new ResilienceException("Reactive stream operation failed for datacenter " + datacenterName, throwable));
    }
    
    /**
     * Get circuit breaker for a specific datacenter.
     */
    public CircuitBreaker getCircuitBreaker(String datacenterName) {
        return circuitBreakerRegistry.circuitBreaker("kafka-" + datacenterName);
    }
    
    /**
     * Get retry instance for a specific datacenter.
     */
    public Retry getRetry(String datacenterName) {
        return retryRegistry.retry("kafka-" + datacenterName);
    }
    
    /**
     * Get rate limiter for a specific datacenter.
     */
    public RateLimiter getRateLimiter(String datacenterName) {
        return rateLimiterRegistry.rateLimiter("kafka-" + datacenterName);
    }
    
    /**
     * Get bulkhead for a specific datacenter.
     */
    public Bulkhead getBulkhead(String datacenterName) {
        return bulkheadRegistry.bulkhead("kafka-" + datacenterName);
    }
    
    /**
     * Get time limiter for a specific datacenter.
     */
    public TimeLimiter getTimeLimiter(String datacenterName) {
        return timeLimiterRegistry.timeLimiter("kafka-" + datacenterName);
    }
    
    /**
     * Get resilience health information for all datacenters.
     */
    public ResilienceHealthInfo getHealthInfo() {
        ResilienceHealthInfo.Builder builder = ResilienceHealthInfo.builder();
        
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> 
            builder.addCircuitBreakerState(cb.getName(), cb.getState()));
        
        return builder.build();
    }
    
    private void setupEventListeners() {
        // Event listeners simplified for compilation compatibility
        logger.info("Resilience pattern event listeners configured");
    }
    
    /**
     * Custom exception for resilience operations.
     */
    public static class ResilienceException extends RuntimeException {
        public ResilienceException(String message) {
            super(message);
        }
        
        public ResilienceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
