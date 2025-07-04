package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.config.ResilienceConfig;
import com.kafka.multidc.routing.RoutingStrategy;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Advanced Health Monitoring and Auto-Recovery Example
 * 
 * This example demonstrates production-ready health monitoring features:
 * - Real-time datacenter health monitoring
 * - Circuit breaker pattern implementation
 * - Automatic failover and recovery
 * - Health metrics collection and alerting
 * - Graceful degradation under load
 * - Connection pool monitoring
 */
public class HealthMonitoringExample {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthMonitoringExample.class);
    
    // Metrics and monitoring
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Counter successfulMessages;
    private final Counter failedMessages;
    private final Counter failoverEvents;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong lastSuccessfulSend = new AtomicLong(System.currentTimeMillis());
    
    // Circuit breaker for each datacenter
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    
    // Health monitoring state
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private final ScheduledExecutorService healthMonitor = Executors.newScheduledThreadPool(2);
    private final ExecutorService alertService = Executors.newCachedThreadPool();
    
    public HealthMonitoringExample() {
        // Initialize metrics
        this.successfulMessages = Counter.builder("kafka.messages.success")
            .description("Number of successfully sent messages")
            .register(meterRegistry);
            
        this.failedMessages = Counter.builder("kafka.messages.failed")
            .description("Number of failed message sends")
            .register(meterRegistry);
            
        this.failoverEvents = Counter.builder("kafka.failover.events")
            .description("Number of datacenter failover events")
            .register(meterRegistry);
            
        // Monitor active connections
        Gauge.builder("kafka.connections.active", () -> activeConnections.get())
            .description("Number of active connections")
            .register(meterRegistry);
            
        // Monitor time since last successful send
        Gauge.builder("kafka.last.success.seconds", () -> 
                (System.currentTimeMillis() - lastSuccessfulSend.get()) / 1000.0)
            .description("Seconds since last successful message send")
            .register(meterRegistry);
        
        // Configure circuit breakers
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50) // 50% failure rate triggers circuit breaker
            .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30s before trying again
            .slidingWindowSize(10) // Consider last 10 calls
            .minimumNumberOfCalls(5) // Need at least 5 calls to calculate failure rate
            .slowCallRateThreshold(70) // 70% slow calls trigger circuit breaker
            .slowCallDurationThreshold(Duration.ofSeconds(3)) // Calls > 3s are considered slow
            .build();
            
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    }
    
    public static void main(String[] args) {
        HealthMonitoringExample example = new HealthMonitoringExample();
        example.runHealthMonitoringDemo();
    }
    
    public void runHealthMonitoringDemo() {
        logger.info("=== Starting Advanced Health Monitoring Demo ===");
        
        // Configure client with enhanced health monitoring
        KafkaDatacenterConfiguration config = createHealthAwareConfiguration();
        
        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            // Set up health monitoring
            setupHealthMonitoring(client);
            
            // Start health checks
            startContinuousHealthChecks();
            
            // Start metrics reporting
            startMetricsReporting();
            
            // Simulate production load with health monitoring
            simulateProductionWorkload(client);
            
            // Simulate datacenter failures
            simulateDatacenterFailures(client);
            
            // Test recovery scenarios
            testRecoveryScenarios(client);
            
        } catch (Exception e) {
            logger.error("Health monitoring demo failed", e);
        } finally {
            shutdown();
        }
    }
    
    private KafkaDatacenterConfiguration createHealthAwareConfiguration() {
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("primary-dc")
                    .region("us-east")
                    .bootstrapServers("localhost:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .maxConnections(10)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(10))
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("secondary-dc")
                    .region("us-west")
                    .bootstrapServers("localhost:9093")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .maxConnections(8)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(10))
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("backup-dc")
                    .region("eu-west")
                    .bootstrapServers("localhost:9094")
                    .priority(3)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .maxConnections(5)
                    .connectionTimeout(Duration.ofSeconds(8))
                    .requestTimeout(Duration.ofSeconds(15))
                    .build())
            .localDatacenter("primary-dc")
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)
            .healthCheckInterval(Duration.ofSeconds(10))
            .connectionTimeout(Duration.ofSeconds(5))
            .enableMetrics(true)
            .resilienceConfig(
                ResilienceConfig.builder()
                    .circuitBreakerConfig(
                        ResilienceConfig.CircuitBreakerConfig.builder()
                            .failureRateThreshold(50.0f)
                            .waitDurationInOpenState(Duration.ofSeconds(30))
                            .slidingWindowSize(10)
                            .minimumNumberOfCalls(5)
                            .build()
                    )
                    .retryConfig(
                        ResilienceConfig.RetryConfig.builder()
                            .maxAttempts(3)
                            .waitDuration(Duration.ofSeconds(1))
                            .build()
                    )
                    .timeLimiterConfig(
                        ResilienceConfig.TimeLimiterConfig.builder()
                            .timeoutDuration(Duration.ofSeconds(10))
                            .build()
                    )
                    .build()
            )
            .build();
    }
    
    private void setupHealthMonitoring(KafkaMultiDatacenterClient client) {
        logger.info("Setting up comprehensive health monitoring...");
        
        // Note: In a real implementation, you would integrate with the client's
        // internal health monitoring system. For this demo, we'll simulate
        // health monitoring using circuit breakers and metrics.
        
        logger.info("Health monitoring configured - using metrics and circuit breakers for health tracking");
    }
    
    private void startContinuousHealthChecks() {
        logger.info("Starting continuous health monitoring...");
        
        // Health check every 15 seconds
        healthMonitor.scheduleAtFixedRate(() -> {
            try {
                performHealthCheck();
            } catch (Exception e) {
                logger.error("Health check failed", e);
            }
        }, 0, 15, TimeUnit.SECONDS);
        
        // Deep health check every 2 minutes
        healthMonitor.scheduleAtFixedRate(() -> {
            try {
                performDeepHealthCheck();
            } catch (Exception e) {
                logger.error("Deep health check failed", e);
            }
        }, 30, 120, TimeUnit.SECONDS);
    }
    
    private void performHealthCheck() {
        long timeSinceLastSuccess = System.currentTimeMillis() - lastSuccessfulSend.get();
        boolean currentlyHealthy = timeSinceLastSuccess < 60000; // 1 minute threshold
        
        if (isHealthy.get() != currentlyHealthy) {
            isHealthy.set(currentlyHealthy);
            logger.warn("Overall system health changed: {}", currentlyHealthy ? "HEALTHY" : "UNHEALTHY");
            
            if (!currentlyHealthy) {
                alertService.execute(() -> sendSystemAlert("SYSTEM_UNHEALTHY", 
                    "No successful messages sent in " + (timeSinceLastSuccess / 1000) + " seconds"));
            }
        }
        
        logger.debug("Health check completed - System: {}, Last success: {}s ago", 
                    currentlyHealthy ? "HEALTHY" : "UNHEALTHY", timeSinceLastSuccess / 1000);
    }
    
    private void performDeepHealthCheck() {
        logger.info("Performing deep health check...");
        
        // Check connection pool health
        int connections = activeConnections.get();
        if (connections == 0) {
            logger.error("No active connections detected!");
            alertService.execute(() -> sendSystemAlert("NO_CONNECTIONS", 
                "No active Kafka connections"));
        }
        
        // Check circuit breaker states
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            CircuitBreaker.State state = cb.getState();
            if (state == CircuitBreaker.State.OPEN) {
                logger.warn("Circuit breaker {} is OPEN", cb.getName());
            }
        });
        
        // Log current metrics
        logger.info("Health Metrics - Success: {}, Failed: {}, Failovers: {}, Connections: {}", 
                   successfulMessages.count(), failedMessages.count(), 
                   failoverEvents.count(), activeConnections.get());
    }
    
    private void startMetricsReporting() {
        logger.info("Starting metrics reporting...");
        
        healthMonitor.scheduleAtFixedRate(() -> {
            try {
                reportMetrics();
            } catch (Exception e) {
                logger.error("Metrics reporting failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    private void reportMetrics() {
        double successRate = calculateSuccessRate();
        long timeSinceLastSuccess = (System.currentTimeMillis() - lastSuccessfulSend.get()) / 1000;
        
        logger.info("=== HEALTH METRICS REPORT ===");
        logger.info("Success Rate: {:.2f}%", successRate);
        logger.info("Messages - Success: {}, Failed: {}", 
                   (long)successfulMessages.count(), (long)failedMessages.count());
        logger.info("Failover Events: {}", (long)failoverEvents.count());
        logger.info("Active Connections: {}", activeConnections.get());
        logger.info("Seconds Since Last Success: {}", timeSinceLastSuccess);
        logger.info("System Health: {}", isHealthy.get() ? "HEALTHY" : "UNHEALTHY");
        logger.info("=============================");
        
        // Alert on low success rate
        if (successRate < 95.0 && (successfulMessages.count() + failedMessages.count()) > 10) {
            alertService.execute(() -> sendSystemAlert("LOW_SUCCESS_RATE", 
                String.format("Success rate is %.2f%%", successRate)));
        }
    }
    
    private void simulateProductionWorkload(KafkaMultiDatacenterClient client) {
        logger.info("Simulating production workload with health monitoring...");
        
        ExecutorService workloadExecutor = Executors.newFixedThreadPool(5);
        
        for (int i = 0; i < 5; i++) {
            final int workerId = i;
            workloadExecutor.execute(() -> {
                for (int j = 0; j < 20; j++) {
                    try {
                        sendMonitoredMessage(client, workerId, j);
                        Thread.sleep(200); // Simulate processing time
                    } catch (Exception e) {
                        logger.error("Worker {} failed on message {}", workerId, j, e);
                        failedMessages.increment();
                    }
                }
            });
        }
        
        workloadExecutor.shutdown();
        try {
            workloadExecutor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private void sendMonitoredMessage(KafkaMultiDatacenterClient client, int workerId, int messageId) {
        String key = String.format("worker-%d-msg-%d", workerId, messageId);
        String value = String.format("Production message from worker %d, id %d, timestamp %d", 
                                    workerId, messageId, System.currentTimeMillis());
        
        ProducerRecord<String, String> record = new ProducerRecord<>("production-events", key, value);
        
        try {
            long startTime = System.currentTimeMillis();
            RecordMetadata metadata = client.producerSync().send(record);
            long duration = System.currentTimeMillis() - startTime;
            
            successfulMessages.increment();
            lastSuccessfulSend.set(System.currentTimeMillis());
            
            logger.debug("Message sent successfully: worker={}, message={}, partition={}, offset={}, duration={}ms", 
                        workerId, messageId, metadata.partition(), metadata.offset(), duration);
                        
            // Monitor for slow sends
            if (duration > 2000) {
                logger.warn("Slow message send detected: {}ms for worker={}, message={}", 
                           duration, workerId, messageId);
            }
            
        } catch (Exception e) {
            failedMessages.increment();
            logger.error("Failed to send message: worker={}, message={}", workerId, messageId, e);
            throw e;
        }
    }
    
    private void simulateDatacenterFailures(KafkaMultiDatacenterClient client) {
        logger.info("Simulating datacenter failure scenarios...");
        
        // This would normally involve stopping actual Kafka brokers
        // For demo purposes, we'll simulate by forcing circuit breakers
        
        healthMonitor.schedule(() -> {
            logger.warn("SIMULATING: Primary datacenter failure");
            CircuitBreaker primaryCB = circuitBreakerRegistry.circuitBreaker("primary-dc");
            primaryCB.transitionToForcedOpenState();
            failoverEvents.increment();
        }, 10, TimeUnit.SECONDS);
        
        healthMonitor.schedule(() -> {
            logger.warn("SIMULATING: Secondary datacenter also failing");
            CircuitBreaker secondaryCB = circuitBreakerRegistry.circuitBreaker("secondary-dc");
            secondaryCB.transitionToForcedOpenState();
            failoverEvents.increment();
        }, 20, TimeUnit.SECONDS);
        
        healthMonitor.schedule(() -> {
            logger.info("SIMULATING: Primary datacenter recovery");
            CircuitBreaker primaryCB = circuitBreakerRegistry.circuitBreaker("primary-dc");
            primaryCB.transitionToClosedState();
        }, 45, TimeUnit.SECONDS);
    }
    
    private void testRecoveryScenarios(KafkaMultiDatacenterClient client) {
        logger.info("Testing recovery scenarios...");
        
        // Continue sending messages during simulated failures
        healthMonitor.schedule(() -> {
            logger.info("Testing message sending during failures...");
            for (int i = 0; i < 10; i++) {
                try {
                    sendMonitoredMessage(client, 99, i);
                    Thread.sleep(500);
                } catch (Exception e) {
                    logger.error("Expected failure during datacenter outage", e);
                }
            }
        }, 15, TimeUnit.SECONDS);
        
        // Wait for recovery and test again
        healthMonitor.schedule(() -> {
            logger.info("Testing message sending after recovery...");
            for (int i = 0; i < 5; i++) {
                try {
                    sendMonitoredMessage(client, 98, i);
                    Thread.sleep(300);
                } catch (Exception e) {
                    logger.error("Unexpected failure after recovery", e);
                }
            }
        }, 50, TimeUnit.SECONDS);
    }
    
    private void sendSystemAlert(String alertType, String message) {
        // In a real system, this would integrate with PagerDuty, Slack, etc.
        logger.error("🚨 SYSTEM ALERT [{}]: {}", alertType, message);
        
        switch (alertType) {
            case "SYSTEM_UNHEALTHY":
                logger.error("   Action Required: Check Kafka cluster health and network connectivity");
                break;
            case "NO_CONNECTIONS":
                logger.error("   Action Required: Verify Kafka broker availability and network");
                break;
            case "LOW_SUCCESS_RATE":
                logger.error("   Action Required: Investigate message delivery failures");
                break;
        }
    }
    
    private double calculateSuccessRate() {
        double total = successfulMessages.count() + failedMessages.count();
        return total > 0 ? (successfulMessages.count() / total) * 100.0 : 100.0;
    }
    
    private void shutdown() {
        logger.info("Shutting down health monitoring...");
        
        healthMonitor.shutdown();
        alertService.shutdown();
        
        try {
            if (!healthMonitor.awaitTermination(5, TimeUnit.SECONDS)) {
                healthMonitor.shutdownNow();
            }
            if (!alertService.awaitTermination(5, TimeUnit.SECONDS)) {
                alertService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            healthMonitor.shutdownNow();
            alertService.shutdownNow();
        }
        
        logger.info("Health monitoring shutdown complete");
    }
}
