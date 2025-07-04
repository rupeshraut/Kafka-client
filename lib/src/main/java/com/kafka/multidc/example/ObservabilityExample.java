package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating observability and metrics features of the Kafka Multi-Datacenter Client.
 * Shows health monitoring, metrics collection, alerting, and performance tracking.
 */
public class ObservabilityExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ObservabilityExample.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Observability Example ===");
        
        try {
            // Create configuration with comprehensive observability
            KafkaDatacenterConfiguration config = createObservabilityConfiguration();
            
            // Build the client with observability features enabled
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Observability client created successfully");
                
                // Start monitoring tasks
                startHealthMonitoring(client);
                startMetricsCollection(client);
                
                // Demonstrate health monitoring
                demonstrateHealthMonitoring(client);
                
                // Demonstrate metrics collection
                demonstrateMetricsCollection(client);
                
                // Demonstrate connection pool monitoring
                demonstrateConnectionPoolMonitoring(client);
                
                // Demonstrate performance tracking
                demonstratePerformanceTracking(client);
                
                // Demonstrate alerting
                demonstrateAlerting(client);
                
                // Run for a while to see monitoring in action
                logger.info("Running observability demo for 30 seconds...");
                Thread.sleep(30000);
                
                logger.info("Observability example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Observability example failed", e);
        } finally {
            scheduler.shutdown();
        }
    }
    
    /**
     * Create configuration with comprehensive observability features.
     */
    private static KafkaDatacenterConfiguration createObservabilityConfiguration() {
        logger.info("Creating observability configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .datacenters(List.of(
                KafkaDatacenterEndpoint.builder()
                    .id("observability-primary")
                    .region("us-east-1")
                    .bootstrapServers("kafka-obs-1.company.com:9092,kafka-obs-2.company.com:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(25)
                    .minConnections(5)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("observability-secondary")
                    .region("us-west-2")
                    .bootstrapServers("kafka-obs-west-1.company.com:9092,kafka-obs-west-2.company.com:9092")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(20)
                    .minConnections(3)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("observability-monitor")
                    .region("eu-central-1")
                    .bootstrapServers("kafka-obs-eu-1.company.com:9092")
                    .priority(3)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(15))
                    .requestTimeout(Duration.ofSeconds(45))
                    .maxConnections(15)
                    .minConnections(2)
                    .build()
            ))
            .localDatacenter("observability-primary")
            .routingStrategy(RoutingStrategy.WEIGHTED)
            .healthCheckInterval(Duration.ofSeconds(10))
            .connectionTimeout(Duration.ofSeconds(15))
            .requestTimeout(Duration.ofMinutes(1))
            .enableMetrics(true)
            .metricsPrefix("observability.kafka.multidc")
            .build();
    }
    
    /**
     * Start continuous health monitoring.
     */
    private static void startHealthMonitoring(KafkaMultiDatacenterClient client) {
        logger.info("Starting health monitoring...");
        
        // Subscribe to health changes (store subscription for cleanup)
        client.subscribeToHealthChanges((datacenter, healthy) -> {
            if (healthy) {
                logger.info("🟢 HEALTH ALERT: Datacenter {} ({}) is now HEALTHY", 
                           datacenter.getId(), datacenter.getRegion());
            } else {
                logger.warn("🔴 HEALTH ALERT: Datacenter {} ({}) is now UNHEALTHY", 
                           datacenter.getId(), datacenter.getRegion());
            }
        });
        
        // Schedule periodic health checks
        scheduler.scheduleAtFixedRate(() -> {
            try {
                var healthFuture = client.checkDatacenterHealth();
                var datacenters = healthFuture.get(5, TimeUnit.SECONDS);
                
                long healthyCount = datacenters.stream().mapToLong(dc -> dc.isHealthy() ? 1 : 0).sum();
                long totalCount = datacenters.size();
                
                logger.info("📊 Health Summary: {}/{} datacenters healthy", healthyCount, totalCount);
                
                if (healthyCount < totalCount) {
                    logger.warn("⚠️ HEALTH WARNING: {} datacenters are unhealthy", totalCount - healthyCount);
                }
                
            } catch (Exception e) {
                logger.error("Health monitoring failed", e);
            }
        }, 5, 15, TimeUnit.SECONDS);
    }
    
    /**
     * Start metrics collection.
     */
    private static void startMetricsCollection(KafkaMultiDatacenterClient client) {
        logger.info("Starting metrics collection...");
        
        // Schedule periodic metrics collection
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Collect connection pool metrics
                var aggregatedMetrics = client.getAggregatedConnectionMetrics();
                
                logger.info("📈 Connection Pool Metrics:");
                logger.info("  Total Pools: {}, Healthy: {}", 
                           aggregatedMetrics.getTotalPools(),
                           aggregatedMetrics.getHealthyPools());
                           
                logger.info("  Total Connections: {}", aggregatedMetrics.getTotalConnections());
                logger.info("  Total Created: {}", aggregatedMetrics.getTotalConnectionsCreated());
                logger.info("  Total Closed: {}", aggregatedMetrics.getTotalConnectionsClosed());
                
                logger.info("  Pool Health Ratio: {:.2f}", aggregatedMetrics.getHealthRatio());
                
                // Collect individual datacenter metrics
                var allMetrics = client.getAllConnectionMetrics();
                allMetrics.forEach((datacenterId, metrics) -> {
                    logger.info("  {}: Active={}, Created={}, Errors={}, Requests={}", 
                               datacenterId,
                               metrics.getConnectionsActive(),
                               metrics.getConnectionsCreated(),
                               metrics.getConnectionErrors(),
                               metrics.getRequestCount());
                });
                
            } catch (Exception e) {
                logger.error("Metrics collection failed", e);
            }
        }, 10, 20, TimeUnit.SECONDS);
    }
    
    /**
     * Demonstrate health monitoring capabilities.
     */
    private static void demonstrateHealthMonitoring(KafkaMultiDatacenterClient client) {
        logger.info("=== Health Monitoring Demo ===");
        
        try {
            // Get current health status
            logger.info("Checking current datacenter health...");
            var healthFuture = client.checkDatacenterHealth();
            var datacenters = healthFuture.get();
            
            logger.info("Current health status:");
            datacenters.forEach(dc -> {
                String status = dc.isHealthy() ? "🟢 HEALTHY" : "🔴 UNHEALTHY";
                logger.info("  {} ({}): {} - Priority: {}", 
                           dc.getId(), dc.getRegion(), status, dc.getPriority());
            });
            
            // Check connection pool health
            var poolHealth = client.getConnectionPoolHealth();
            logger.info("Connection pool health:");
            poolHealth.forEach((datacenterId, healthy) -> {
                String status = healthy ? "🟢 HEALTHY" : "🔴 UNHEALTHY";
                logger.info("  {}: {}", datacenterId, status);
            });
            
            // Check if all pools are healthy
            boolean allHealthy = client.areAllConnectionPoolsHealthy();
            logger.info("All connection pools healthy: {}", allHealthy ? "✅ YES" : "❌ NO");
            
        } catch (Exception e) {
            logger.error("❌ Health monitoring demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate metrics collection and analysis.
     */
    private static void demonstrateMetricsCollection(KafkaMultiDatacenterClient client) {
        logger.info("=== Metrics Collection Demo ===");
        
        try {
            // Generate some load to create metrics
            logger.info("Generating load for metrics collection...");
            
            for (int i = 0; i < 20; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "observability-metrics-test",
                    "metrics-key-" + i,
                    String.format("{\"messageId\":%d,\"test\":\"metrics-collection\",\"timestamp\":\"%s\",\"load\":\"test\"}", 
                                 i, Instant.now().toString())
                );
                
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("Metrics test message {} sent to partition {} at offset {}", 
                           i, metadata.partition(), metadata.offset());
                
                // Add small delay to simulate realistic load
                Thread.sleep(100);
            }
            
            // Wait a moment for metrics to update
            Thread.sleep(2000);
            
            // Collect and display detailed metrics
            var aggregatedMetrics = client.getAggregatedConnectionMetrics();
            
            logger.info("📊 Detailed Metrics Analysis:");
            logger.info("  Connection Statistics:");
            logger.info("    Total Pools: {}", aggregatedMetrics.getTotalPools());
            logger.info("    Healthy Pools: {}", aggregatedMetrics.getHealthyPools());
            logger.info("    Total Connections: {}", aggregatedMetrics.getTotalConnections());
            logger.info("    Health Ratio: {:.1f}%", aggregatedMetrics.getHealthRatio() * 100);
            logger.info("    Total Created: {}", aggregatedMetrics.getTotalConnectionsCreated());
            logger.info("    Total Closed: {}", aggregatedMetrics.getTotalConnectionsClosed());
            
            // Per-datacenter metrics
            logger.info("  Per-Datacenter Metrics:");
            var allMetrics = client.getAllConnectionMetrics();
            allMetrics.forEach((datacenterId, metrics) -> {
                logger.info("    {}:", datacenterId);
                logger.info("      Active Connections: {}", metrics.getConnectionsActive());
                logger.info("      Created Connections: {}", metrics.getConnectionsCreated());
                logger.info("      Closed Connections: {}", metrics.getConnectionsClosed());
                logger.info("      Connection Errors: {}", metrics.getConnectionErrors());
                logger.info("      Total Requests: {}", metrics.getRequestCount());
                logger.info("      Error Count: {}", metrics.getErrorCount());
                logger.info("      Average Latency: {}", metrics.getAverageLatency());
                logger.info("      Error Rate: {:.2f}%", metrics.getErrorRate() * 100);
                logger.info("      Is Healthy: {}", metrics.isHealthy());
            });
            
        } catch (Exception e) {
            logger.error("❌ Metrics collection demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate connection pool monitoring.
     */
    private static void demonstrateConnectionPoolMonitoring(KafkaMultiDatacenterClient client) {
        logger.info("=== Connection Pool Monitoring Demo ===");
        
        try {
            // Warm up connections
            logger.info("Warming up connection pools...");
            client.warmUpConnections().get();
            logger.info("✅ Connection pools warmed up");
            
            // Monitor individual datacenter metrics
            logger.info("Individual datacenter connection metrics:");
            client.getDatacenters().forEach(datacenter -> {
                try {
                    var metrics = client.getConnectionMetrics(datacenter.getId());
                    logger.info("  {} Connection Pool:", datacenter.getId());
                    logger.info("    Active: {}", metrics.getConnectionsActive());
                    logger.info("    Created: {}", metrics.getConnectionsCreated());
                    logger.info("    Errors: {}", metrics.getConnectionErrors());
                    logger.info("    Error Rate: {:.2f}%", metrics.getErrorRate() * 100);
                    logger.info("    Health: {}", metrics.isHealthy() ? "HEALTHY" : "UNHEALTHY");
                } catch (Exception e) {
                    logger.warn("Failed to get metrics for {}: {}", datacenter.getId(), e.getMessage());
                }
            });
            
            // Perform maintenance
            logger.info("Performing connection pool maintenance...");
            client.maintainAllConnectionPools();
            logger.info("✅ Connection pool maintenance completed");
            
            // Reset metrics for clean slate
            logger.info("Resetting connection metrics...");
            client.getDatacenters().forEach(datacenter -> {
                client.resetConnectionMetrics(datacenter.getId());
            });
            logger.info("✅ Connection metrics reset");
            
        } catch (Exception e) {
            logger.error("❌ Connection pool monitoring demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate performance tracking.
     */
    private static void demonstratePerformanceTracking(KafkaMultiDatacenterClient client) {
        logger.info("=== Performance Tracking Demo ===");
        
        try {
            // Measure synchronous performance
            logger.info("Measuring synchronous operation performance...");
            long syncStartTime = System.currentTimeMillis();
            
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "performance-sync-test",
                    "perf-sync-" + i,
                    String.format("{\"perfTest\":\"sync\",\"messageId\":%d,\"timestamp\":\"%s\"}", 
                                 i, Instant.now().toString())
                );
                
                client.producerSync().send(record);
            }
            
            long syncDuration = System.currentTimeMillis() - syncStartTime;
            double syncThroughput = (10.0 * 1000) / syncDuration;
            
            logger.info("Synchronous Performance: {} messages in {}ms ({:.2f} msg/sec)", 
                       10, syncDuration, syncThroughput);
            
            // Measure asynchronous performance
            logger.info("Measuring asynchronous operation performance...");
            long asyncStartTime = System.currentTimeMillis();
            
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "performance-async-test",
                    "perf-async-" + i,
                    String.format("{\"perfTest\":\"async\",\"messageId\":%d,\"timestamp\":\"%s\"}", 
                                 i, Instant.now().toString())
                );
                
                client.producerAsync().sendAsync(record);
            }
            
            // Wait for async operations to complete
            Thread.sleep(1000);
            long asyncDuration = System.currentTimeMillis() - asyncStartTime;
            double asyncThroughput = (10.0 * 1000) / asyncDuration;
            
            logger.info("Asynchronous Performance: {} messages in {}ms ({:.2f} msg/sec)", 
                       10, asyncDuration, asyncThroughput);
            
            // Performance comparison
            logger.info("📈 Performance Summary:");
            logger.info("  Sync throughput: {:.2f} msg/sec", syncThroughput);
            logger.info("  Async throughput: {:.2f} msg/sec", asyncThroughput);
            logger.info("  Async advantage: {:.1f}x faster", asyncThroughput / syncThroughput);
            
        } catch (Exception e) {
            logger.error("❌ Performance tracking demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate alerting capabilities.
     */
    private static void demonstrateAlerting(KafkaMultiDatacenterClient client) {
        logger.info("=== Alerting Demo ===");
        
        try {
            // Simulate alert conditions
            logger.info("Testing alerting system...");
            
            // Monitor for unhealthy datacenters
            client.subscribeToHealthChanges((datacenter, healthy) -> {
                if (!healthy) {
                    logger.error("🚨 CRITICAL ALERT: Datacenter {} is UNHEALTHY!", datacenter.getId());
                    logger.error("🚨 ALERT DETAILS:");
                    logger.error("   - Datacenter: {} ({})", datacenter.getId(), datacenter.getRegion());
                    logger.error("   - Priority: {}", datacenter.getPriority());
                    logger.error("   - Status: UNHEALTHY");
                    logger.error("   - Recommended Action: Check network connectivity and Kafka broker status");
                } else {
                    logger.info("✅ RECOVERY ALERT: Datacenter {} is back to HEALTHY", datacenter.getId());
                }
            });
            
            // Check for performance issues
            var allMetrics = client.getAllConnectionMetrics();
            allMetrics.forEach((datacenterId, metrics) -> {
                // Alert on high error rate
                if (metrics.getErrorRate() > 0.05) { // 5% error rate
                    logger.warn("⚠️ RELIABILITY ALERT: High error rate in {}: {:.2f}%", 
                               datacenterId, metrics.getErrorRate() * 100);
                }
                
                // Alert on connection errors
                if (metrics.getConnectionErrors() > 10) {
                    logger.warn("⚠️ CONNECTION ALERT: High connection errors in {}: {}", 
                               datacenterId, metrics.getConnectionErrors());
                }
                
                // Alert on high average latency
                if (metrics.getAverageLatency().toMillis() > 1000) {
                    logger.warn("⚠️ LATENCY ALERT: High average latency in {}: {}ms", 
                               datacenterId, metrics.getAverageLatency().toMillis());
                }
            });
            
            // Send alert test message
            ProducerRecord<String, Object> alertRecord = new ProducerRecord<>(
                "observability-alerts",
                "alert-test",
                Map.of(
                    "alertType", "TEST_ALERT",
                    "timestamp", Instant.now().toString(),
                    "message", "Observability system test alert",
                    "severity", "INFO",
                    "source", "observability-example"
                )
            );
            
            RecordMetadata alertMetadata = client.producerSync().send(alertRecord);
            logger.info("✅ Alert test message sent to partition {} at offset {}", 
                       alertMetadata.partition(), alertMetadata.offset());
            
        } catch (Exception e) {
            logger.error("❌ Alerting demonstration failed", e);
        }
    }
}
