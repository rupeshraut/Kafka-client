/*
 * Copyright 2024 Kafka Multi-Datacenter Client
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.actuator.KafkaMultiDatacenterHealthIndicator;
import com.kafka.multidc.autoconfigure.KafkaMultiDatacenterHealthAutoConfiguration;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating how to use the Kafka Multi-Datacenter Client health indicators
 * for monitoring and observability in enterprise applications.
 * 
 * <p>This example shows:
 * <ul>
 *   <li>Standalone health indicator usage</li>
 *   <li>Spring Boot integration</li>
 *   <li>Periodic health monitoring</li>
 *   <li>Health status interpretation</li>
 * </ul>
 */
public class HealthIndicatorExample {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthIndicatorExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Health Indicator Example ===");
        
        // Create a multi-datacenter client
        KafkaMultiDatacenterClient client = createMultiDatacenterClient();
        
        try {
            // Example 1: Standalone health indicator
            demonstrateStandaloneHealthIndicator(client);
            
            // Example 2: Spring Boot integration
            demonstrateSpringBootIntegration(client);
            
            // Example 3: Periodic health monitoring
            demonstratePeriodicHealthMonitoring(client);
            
            // Wait a bit to see the periodic monitoring
            Thread.sleep(10000);
            
        } catch (Exception e) {
            logger.error("Example execution failed", e);
        } finally {
            client.close();
            logger.info("=== Example completed ===");
        }
    }
    
    private static KafkaMultiDatacenterClient createMultiDatacenterClient() {
        logger.info("Creating multi-datacenter Kafka client...");
        
        return KafkaMultiDatacenterClientBuilder.create()
                .configuration(
                        KafkaDatacenterConfiguration.builder()
                                .addDatacenter(KafkaDatacenterEndpoint.builder()
                                        .id("primary")
                                        .region("us-east-1")
                                        .bootstrapServers("localhost:9092")
                                        .priority(1)
                                        .build())
                                .addDatacenter(KafkaDatacenterEndpoint.builder()
                                        .id("secondary")
                                        .region("us-west-2")
                                        .bootstrapServers("localhost:9093")
                                        .priority(2)
                                        .build())
                                .localDatacenter("primary")
                                .build()
                )
                .build();
    }
    
    private static void demonstrateStandaloneHealthIndicator(KafkaMultiDatacenterClient client) {
        logger.info("\n--- Standalone Health Indicator Example ---");
        
        // Create health indicator with custom configuration
        KafkaMultiDatacenterHealthIndicator healthIndicator = 
                new KafkaMultiDatacenterHealthIndicator(
                        client,
                        Duration.ofSeconds(3), // Custom timeout
                        true // Enable detailed checks
                );
        
        // Perform health check
        KafkaMultiDatacenterHealthIndicator.HealthStatus health = healthIndicator.health();
        
        // Log health status
        logger.info("Health Status: {}", health.getStatus());
        logger.info("Is Healthy: {}", health.isHealthy());
        
        if (health.getException() != null) {
            logger.warn("Health check exception: {}", health.getException().getMessage());
        }
        
        // Log detailed health information
        Map<String, Object> details = health.getDetails();
        details.forEach((key, value) -> {
            if (value instanceof Map) {
                logger.info("  {}: {}", key, formatMap((Map<?, ?>) value));
            } else {
                logger.info("  {}: {}", key, value);
            }
        });
    }
    
    private static void demonstrateSpringBootIntegration(KafkaMultiDatacenterClient client) {
        logger.info("\n--- Spring Boot Integration Example ---");
        
        try {
            // Create Spring Boot health indicator (if Spring Boot is available)
            Object springHealthIndicator = KafkaMultiDatacenterHealthAutoConfiguration
                    .createHealthIndicator(client);
            
            if (springHealthIndicator != null) {
                logger.info("Spring Boot HealthIndicator created successfully");
                logger.info("Type: {}", springHealthIndicator.getClass().getName());
                
                // In a real Spring Boot application, this would be automatically registered
                // and exposed via the /actuator/health endpoint
                logger.info("In Spring Boot, this would be available at: /actuator/health/kafka-multidc");
            } else {
                logger.info("Spring Boot Actuator not available on classpath");
            }
            
        } catch (Exception e) {
            logger.warn("Spring Boot integration failed: {}", e.getMessage());
        }
    }
    
    private static void demonstratePeriodicHealthMonitoring(KafkaMultiDatacenterClient client) {
        logger.info("\n--- Periodic Health Monitoring Example ---");
        
        KafkaMultiDatacenterHealthIndicator healthIndicator = 
                new KafkaMultiDatacenterHealthIndicator(client);
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        
        // Schedule health checks every 3 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                KafkaMultiDatacenterHealthIndicator.HealthStatus health = healthIndicator.health();
                
                logger.info("Periodic Health Check - Status: {}, Healthy: {}", 
                        health.getStatus(), health.isHealthy());
                
                // Log key metrics
                Map<String, Object> details = health.getDetails();
                if (details.containsKey("datacenters.healthy") && details.containsKey("datacenters.total")) {
                    logger.info("  Datacenters: {}/{} healthy", 
                            details.get("datacenters.healthy"), 
                            details.get("datacenters.total"));
                }
                
                if (details.containsKey("connection.pools.all.healthy")) {
                    boolean allHealthy = Boolean.TRUE.equals(details.get("connection.pools.all.healthy"));
                    logger.info("  Connection Pools: {}", 
                            allHealthy ? "All Healthy" : "Some Degraded");
                }
                
                // Alert on unhealthy status
                if (!health.isHealthy()) {
                    logger.warn("ALERT: Kafka Multi-Datacenter Client is UNHEALTHY!");
                    if (health.getException() != null) {
                        logger.warn("  Error: {}", health.getException().getMessage());
                    }
                    
                    // In a real application, you might:
                    // - Send alerts to monitoring systems
                    // - Update circuit breakers
                    // - Trigger failover procedures
                }
                
            } catch (Exception e) {
                logger.error("Health check failed", e);
            }
        }, 0, 3, TimeUnit.SECONDS);
        
        // Register shutdown hook to stop scheduler
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down health monitoring...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
    
    private static String formatMap(Map<?, ?> map) {
        if (map.isEmpty()) {
            return "{}";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        map.forEach((k, v) -> {
            sb.append(k).append("=").append(v).append(", ");
        });
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2); // Remove trailing ", "
        }
        sb.append(" }");
        return sb.toString();
    }
}

/**
 * Spring Boot Configuration Example
 * 
 * <p>In a real Spring Boot application, you would configure the health indicator like this:
 * 
 * <pre>
 * &#64;Configuration
 * &#64;ConditionalOnClass(HealthIndicator.class)
 * public class KafkaHealthConfiguration {
 *     
 *     &#64;Bean
 *     &#64;ConditionalOnBean(KafkaMultiDatacenterClient.class)
 *     &#64;ConditionalOnProperty(name = "management.health.kafka-multidc.enabled", matchIfMissing = true)
 *     public HealthIndicator kafkaMultiDatacenterHealthIndicator(
 *             KafkaMultiDatacenterClient client,
 *             &#64;Value("${management.health.kafka-multidc.timeout:5s}") Duration timeout,
 *             &#64;Value("${management.health.kafka-multidc.detailed-checks:true}") boolean detailedChecks) {
 *         
 *         return (HealthIndicator) KafkaMultiDatacenterHealthAutoConfiguration
 *                 .createHealthIndicator(client, timeout, detailedChecks);
 *     }
 * }
 * </pre>
 * 
 * <p>Application properties:
 * <pre>
 * # Enable/disable health indicator
 * management.health.kafka-multidc.enabled=true
 * 
 * # Health check timeout
 * management.health.kafka-multidc.timeout=5s
 * 
 * # Include detailed checks
 * management.health.kafka-multidc.detailed-checks=true
 * 
 * # Expose health endpoint
 * management.endpoints.web.exposure.include=health
 * management.endpoint.health.show-details=always
 * </pre>
 */
class SpringBootConfigurationExample {
    // This is just documentation - see the comment above
}
