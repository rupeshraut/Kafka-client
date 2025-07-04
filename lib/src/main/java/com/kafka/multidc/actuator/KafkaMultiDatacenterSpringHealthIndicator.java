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

import java.time.Duration;

/**
 * Spring Boot Actuator health indicator adapter for Kafka Multi-Datacenter Client.
 * This class adapts the standalone health indicator to Spring Boot's HealthIndicator interface.
 * 
 * <p>Only available when Spring Boot Actuator is on the classpath.
 * 
 * @author Kafka Multi-Datacenter Client
 * @since 1.0.0
 */
public class KafkaMultiDatacenterSpringHealthIndicator {

    private final KafkaMultiDatacenterHealthIndicator delegate;

    /**
     * Creates a new Spring Boot health indicator.
     *
     * @param client The Kafka multi-datacenter client to monitor
     */
    public KafkaMultiDatacenterSpringHealthIndicator(KafkaMultiDatacenterClient client) {
        this.delegate = new KafkaMultiDatacenterHealthIndicator(client);
    }

    /**
     * Creates a new Spring Boot health indicator with custom configuration.
     *
     * @param client The Kafka multi-datacenter client to monitor
     * @param healthCheckTimeout Timeout for health check operations
     * @param enableDetailedChecks Whether to include detailed health information
     */
    public KafkaMultiDatacenterSpringHealthIndicator(KafkaMultiDatacenterClient client,
                                                     Duration healthCheckTimeout,
                                                     boolean enableDetailedChecks) {
        this.delegate = new KafkaMultiDatacenterHealthIndicator(client, healthCheckTimeout, enableDetailedChecks);
    }

    /**
     * Create a Spring Boot Actuator Health object from our standalone health status.
     * This method will only compile when Spring Boot Actuator is available.
     */
    public Object health() {
        try {
            // Use reflection to avoid compile-time dependency on Spring Boot
            Class<?> healthClass = Class.forName("org.springframework.boot.actuate.health.Health");
            
            KafkaMultiDatacenterHealthIndicator.HealthStatus status = delegate.health();
            
            // Get Health.Builder
            Object healthBuilder;
            switch (status.getStatus()) {
                case UP:
                    healthBuilder = healthClass.getMethod("up").invoke(null);
                    break;
                case DOWN:
                    healthBuilder = healthClass.getMethod("down").invoke(null);
                    break;
                case OUT_OF_SERVICE:
                    healthBuilder = healthClass.getMethod("outOfService").invoke(null);
                    break;
                case UNKNOWN:
                default:
                    healthBuilder = healthClass.getMethod("unknown").invoke(null);
                    break;
            }
            
            // Add details
            Class<?> builderClass = healthBuilder.getClass();
            for (var entry : status.getDetails().entrySet()) {
                healthBuilder = builderClass.getMethod("withDetail", String.class, Object.class)
                        .invoke(healthBuilder, entry.getKey(), entry.getValue());
            }
            
            // Add exception if present
            if (status.getException() != null) {
                healthBuilder = builderClass.getMethod("withException", Exception.class)
                        .invoke(healthBuilder, status.getException());
            }
            
            // Build and return
            return builderClass.getMethod("build").invoke(healthBuilder);
            
        } catch (Exception e) {
            // Fallback if reflection fails
            throw new RuntimeException("Failed to create Spring Boot Health object", e);
        }
    }

    /**
     * Get the underlying standalone health indicator.
     *
     * @return the standalone health indicator
     */
    public KafkaMultiDatacenterHealthIndicator getDelegate() {
        return delegate;
    }
}
