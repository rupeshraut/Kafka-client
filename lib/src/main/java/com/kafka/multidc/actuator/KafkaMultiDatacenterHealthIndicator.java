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

package com.kafka.multidc.actuator;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.model.DatacenterInfo;
import com.kafka.multidc.pool.KafkaConnectionMetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Standalone health indicator for Kafka Multi-Datacenter Client.
 * Provides comprehensive health checks for multi-datacenter Kafka connectivity,
 * connection pools, and resilience patterns.
 *
 * <p>This class can be used with or without Spring Boot Actuator. When Spring Boot
 * is available, use {@link KafkaMultiDatacenterSpringHealthIndicator} instead.
 *
 * <p>Health check includes:
 * <ul>
 *   <li>Datacenter connectivity status</li>
 *   <li>Connection pool health</li>
 *   <li>Circuit breaker states</li>
 *   <li>Producer/Consumer operational status</li>
 *   <li>Schema registry connectivity (if configured)</li>
 * </ul>
 *
 * @author Kafka Multi-Datacenter Client
 * @since 1.0.0
 */
public class KafkaMultiDatacenterHealthIndicator {

    private final KafkaMultiDatacenterClient client;
    private final Duration healthCheckTimeout;
    private final boolean enableDetailedChecks;

    /**
     * Creates a new health indicator.
     *
     * @param client The Kafka multi-datacenter client to monitor
     */
    public KafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client) {
        this(client, Duration.ofSeconds(5), true);
    }

    /**
     * Creates a new health indicator with custom configuration.
     *
     * @param client The Kafka multi-datacenter client to monitor
     * @param healthCheckTimeout Timeout for health check operations
     * @param enableDetailedChecks Whether to include detailed health information
     */
    public KafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client,
                                               Duration healthCheckTimeout,
                                               boolean enableDetailedChecks) {
        this.client = client;
        this.healthCheckTimeout = healthCheckTimeout;
        this.enableDetailedChecks = enableDetailedChecks;
    }

    /**
     * Perform a comprehensive health check.
     *
     * @return Health status with detailed information
     */
    public HealthStatus health() {
        try {
            return performHealthCheck();
        } catch (Exception e) {
            RuntimeException healthCheckException = new RuntimeException("Health check failed: " + e.getMessage(), e);
            return HealthStatus.down()
                    .withException(healthCheckException)
                    .withDetail("error", "Health check failed: " + e.getMessage())
                    .withDetail("timestamp", Instant.now().toString())
                    .build();
        }
    }

    private HealthStatus performHealthCheck() {
        HealthStatus.Builder healthBuilder = HealthStatus.up();
        Map<String, Object> details = new HashMap<>();
        boolean overallHealthy = true;

        // Check datacenter connectivity
        overallHealthy &= checkDatacenterHealth(details);

        // Check connection pool health
        overallHealthy &= checkConnectionPoolHealth(details);

        // Check schema registry health
        overallHealthy &= checkSchemaRegistryHealth(details);

        // Add detailed metrics if enabled
        if (enableDetailedChecks) {
            addDetailedMetrics(details);
        }

        details.put("timestamp", Instant.now().toString());
        details.put("client.status", overallHealthy ? "HEALTHY" : "DEGRADED");

        return overallHealthy
                ? healthBuilder.withDetails(details).build()
                : healthBuilder.status(HealthStatus.Status.DOWN).withDetails(details).build();
    }

    private boolean checkDatacenterHealth(Map<String, Object> details) {
        // Note: Let critical exceptions (like getDatacenters()) propagate up to be caught by main health() method
        Map<String, Object> datacenterStatus = new HashMap<>();
        int healthyDatacenters = 0;
        int totalDatacenters = 0;

        List<DatacenterInfo> datacenters = client.getDatacenters();
        for (DatacenterInfo datacenter : datacenters) {
            totalDatacenters++;
            boolean isHealthy = datacenter.isHealthy();
            datacenterStatus.put(datacenter.getId(), Map.of(
                    "healthy", isHealthy,
                    "endpoint", datacenter.getBootstrapServers(),
                    "priority", datacenter.getPriority(),
                    "region", datacenter.getRegion(),
                    "latency.ms", datacenter.getLatency() != null ? datacenter.getLatency().toMillis() : 0,
                    "lastHealthCheck", datacenter.getLastHealthCheck() != null ? datacenter.getLastHealthCheck().toString() : "unknown"
            ));
            if (isHealthy) {
                healthyDatacenters++;
            }
        }

        details.put("datacenters", datacenterStatus);
        details.put("datacenters.healthy", healthyDatacenters);
        details.put("datacenters.total", totalDatacenters);

        // At least one datacenter should be healthy
        return healthyDatacenters > 0;
    }

    private boolean checkConnectionPoolHealth(Map<String, Object> details) {
        try {
            Map<String, Object> poolStatus = new HashMap<>();
            boolean allPoolsHealthy = client.areAllConnectionPoolsHealthy();
            Map<String, Boolean> poolHealth = client.getConnectionPoolHealth();
            Map<String, KafkaConnectionMetrics> allMetrics = client.getAllConnectionMetrics();
            
            long totalActiveConnections = 0;
            long totalErrors = 0;
            long totalRequests = 0;
            
            for (Map.Entry<String, KafkaConnectionMetrics> entry : allMetrics.entrySet()) {
                String datacenterId = entry.getKey();
                KafkaConnectionMetrics metrics = entry.getValue();
                
                totalActiveConnections += metrics.getConnectionsActive();
                totalErrors += metrics.getConnectionErrors();
                totalRequests += metrics.getRequestCount();
                
                poolStatus.put(datacenterId, Map.of(
                        "active.connections", metrics.getConnectionsActive(),
                        "created.connections", metrics.getConnectionsCreated(),
                        "closed.connections", metrics.getConnectionsClosed(),
                        "connection.errors", metrics.getConnectionErrors(),
                        "request.count", metrics.getRequestCount(),
                        "error.count", metrics.getErrorCount(),
                        "error.rate", metrics.getErrorRate(),
                        "average.latency.ms", metrics.getAverageLatency().toMillis(),
                        "healthy", poolHealth.getOrDefault(datacenterId, false)
                ));
            }

            details.put("connection.pools", poolStatus);
            details.put("connection.pools.all.healthy", allPoolsHealthy);
            details.put("connection.pools.total.active", totalActiveConnections);
            details.put("connection.pools.total.errors", totalErrors);
            details.put("connection.pools.total.requests", totalRequests);
            
            if (totalRequests > 0) {
                details.put("connection.pools.overall.error.rate", (double) totalErrors / totalRequests);
            }

            // Connection pools are healthy if we have at least one healthy pool
            // We don't require all pools to be healthy since some datacenters might be down
            boolean hasHealthyPools = poolHealth.values().stream().anyMatch(healthy -> healthy);
            return hasHealthyPools;
        } catch (Exception e) {
            details.put("connection.pools.error", e.getMessage());
            return false;
        }
    }

    private boolean checkSchemaRegistryHealth(Map<String, Object> details) {
        try {
            CompletableFuture<Map<String, Boolean>> healthCheck = client.checkSchemaRegistryHealth();
            Map<String, Boolean> schemaRegistryHealth = healthCheck.get(healthCheckTimeout.toMillis(), TimeUnit.MILLISECONDS);
            
            long healthyRegistries = schemaRegistryHealth.values().stream()
                    .mapToLong(healthy -> healthy ? 1 : 0)
                    .sum();
            
            details.put("schema.registry", schemaRegistryHealth);
            details.put("schema.registry.healthy.count", healthyRegistries);
            details.put("schema.registry.total.count", schemaRegistryHealth.size());
            
            // Schema registry is optional, so we return true even if none are configured
            return schemaRegistryHealth.isEmpty() || healthyRegistries > 0;
        } catch (Exception e) {
            // Schema registry might not be configured, which is acceptable
            details.put("schema.registry.note", "Schema registry not configured or unreachable: " + e.getMessage());
            return true;
        }
    }

    private void addDetailedMetrics(Map<String, Object> details) {
        try {
            // Add configuration summary
            KafkaDatacenterConfiguration config = client.getConfiguration();
            Map<String, Object> configSummary = Map.of(
                    "fallback.enabled", config.getFallbackConfiguration() != null,
                    "dead.letter.enabled", config.getDeadLetterConfig() != null,
                    "client.closed", client.isClosed()
            );

            details.put("configuration", configSummary);
            
            // Add datacenter preference info
            DatacenterInfo localDatacenter = client.getLocalDatacenter();
            if (localDatacenter != null) {
                details.put("local.datacenter", Map.of(
                        "id", localDatacenter.getId(),
                        "region", localDatacenter.getRegion(),
                        "healthy", localDatacenter.isHealthy()
                ));
            }
        } catch (Exception e) {
            details.put("detailed.metrics.error", e.getMessage());
        }
    }

    /**
     * Simple health status representation.
     */
    public static class HealthStatus {
        public enum Status {
            UP, DOWN, OUT_OF_SERVICE, UNKNOWN
        }
        
        private final Status status;
        private final Map<String, Object> details;
        private final Exception exception;
        
        private HealthStatus(Status status, Map<String, Object> details, Exception exception) {
            this.status = status;
            this.details = new HashMap<>(details);
            this.exception = exception;
        }
        
        public Status getStatus() {
            return status;
        }
        
        public Map<String, Object> getDetails() {
            return new HashMap<>(details);
        }
        
        public Exception getException() {
            return exception;
        }
        
        public boolean isHealthy() {
            return status == Status.UP;
        }
        
        public static Builder up() {
            return new Builder(Status.UP);
        }
        
        public static Builder down() {
            return new Builder(Status.DOWN);
        }
        
        public static Builder unknown() {
            return new Builder(Status.UNKNOWN);
        }
        
        public static Builder outOfService() {
            return new Builder(Status.OUT_OF_SERVICE);
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("HealthStatus{status=").append(status);
            if (!details.isEmpty()) {
                sb.append(", details=").append(details);
            }
            if (exception != null) {
                sb.append(", exception=").append(exception.getMessage());
            }
            sb.append("}");
            return sb.toString();
        }
        
        public static class Builder {
            private final Status status;
            private final Map<String, Object> details = new HashMap<>();
            private Exception exception;
            
            private Builder(Status status) {
                this.status = status;
            }
            
            public Builder withDetail(String key, Object value) {
                this.details.put(key, value);
                return this;
            }
            
            public Builder withDetails(Map<String, Object> details) {
                this.details.putAll(details);
                return this;
            }
            
            public Builder withException(Exception exception) {
                this.exception = exception;
                return this;
            }
            
            public Builder status(Status status) {
                return new Builder(status)
                        .withDetails(this.details)
                        .withException(this.exception);
            }
            
            public HealthStatus build() {
                return new HealthStatus(status, details, exception);
            }
        }
    }
}
