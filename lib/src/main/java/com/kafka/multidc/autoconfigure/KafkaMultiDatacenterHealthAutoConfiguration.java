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

package com.kafka.multidc.autoconfigure;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.actuator.KafkaMultiDatacenterSpringHealthIndicator;

import java.time.Duration;

/**
 * Spring Boot auto-configuration for Kafka Multi-Datacenter Client health indicators.
 * 
 * <p>This class will automatically configure health indicators when:
 * <ul>
 *   <li>Spring Boot Actuator is on the classpath</li>
 *   <li>A KafkaMultiDatacenterClient bean is available</li>
 *   <li>Health indicators are enabled</li>
 * </ul>
 * 
 * <p>Configuration properties:
 * <pre>
 * management.health.kafka-multidc.enabled=true
 * management.health.kafka-multidc.timeout=5s
 * management.health.kafka-multidc.detailed-checks=true
 * </pre>
 * 
 * @author Kafka Multi-Datacenter Client
 * @since 1.0.0
 */
public class KafkaMultiDatacenterHealthAutoConfiguration {

    /**
     * Configuration properties for the health indicator.
     */
    public static class HealthProperties {
        private boolean enabled = true;
        private Duration timeout = Duration.ofSeconds(5);
        private boolean detailedChecks = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getTimeout() {
            return timeout;
        }

        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }

        public boolean isDetailedChecks() {
            return detailedChecks;
        }

        public void setDetailedChecks(boolean detailedChecks) {
            this.detailedChecks = detailedChecks;
        }
    }

    /**
     * Create a health indicator bean when conditions are met.
     * 
     * <p>This method uses reflection to avoid compile-time dependency on Spring Boot.
     * In a real Spring Boot environment, this would be annotated with @Bean and @ConditionalOnXxx.
     * 
     * @param client The Kafka multi-datacenter client
     * @param properties The health properties
     * @return Spring Boot HealthIndicator instance
     */
    public Object kafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client, 
                                                      HealthProperties properties) {
        if (!properties.isEnabled()) {
            return null;
        }

        try {
            // Check if Spring Boot Actuator HealthIndicator is available
            Class<?> healthIndicatorClass = Class.forName("org.springframework.boot.actuate.health.HealthIndicator");
            
            // Create our health indicator
            KafkaMultiDatacenterSpringHealthIndicator healthIndicator = 
                    new KafkaMultiDatacenterSpringHealthIndicator(
                            client, 
                            properties.getTimeout(), 
                            properties.isDetailedChecks()
                    );
            
            // Create a proxy that implements HealthIndicator interface
            return java.lang.reflect.Proxy.newProxyInstance(
                    healthIndicatorClass.getClassLoader(),
                    new Class<?>[]{healthIndicatorClass},
                    (proxy, method, args) -> {
                        if ("health".equals(method.getName())) {
                            return healthIndicator.health();
                        }
                        throw new UnsupportedOperationException("Method not supported: " + method.getName());
                    }
            );
            
        } catch (ClassNotFoundException e) {
            // Spring Boot Actuator not available, return null
            return null;
        }
    }

    /**
     * Manual configuration helper for Spring Boot applications.
     * 
     * <p>Usage in Spring Boot application:
     * <pre>
     * &#64;Configuration
     * public class HealthConfig {
     *     
     *     &#64;Bean
     *     &#64;ConditionalOnClass(HealthIndicator.class)
     *     &#64;ConditionalOnBean(KafkaMultiDatacenterClient.class)
     *     &#64;ConditionalOnProperty(name = "management.health.kafka-multidc.enabled", matchIfMissing = true)
     *     public HealthIndicator kafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client) {
     *         return KafkaMultiDatacenterHealthAutoConfiguration.createHealthIndicator(client);
     *     }
     * }
     * </pre>
     * 
     * @param client The Kafka multi-datacenter client
     * @return Spring Boot HealthIndicator instance
     */
    public static Object createHealthIndicator(KafkaMultiDatacenterClient client) {
        HealthProperties properties = new HealthProperties();
        KafkaMultiDatacenterHealthAutoConfiguration config = new KafkaMultiDatacenterHealthAutoConfiguration();
        return config.kafkaMultiDatacenterHealthIndicator(client, properties);
    }

    /**
     * Manual configuration helper with custom properties.
     * 
     * @param client The Kafka multi-datacenter client
     * @param timeout Health check timeout
     * @param detailedChecks Whether to include detailed checks
     * @return Spring Boot HealthIndicator instance
     */
    public static Object createHealthIndicator(KafkaMultiDatacenterClient client, 
                                               Duration timeout, 
                                               boolean detailedChecks) {
        HealthProperties properties = new HealthProperties();
        properties.setTimeout(timeout);
        properties.setDetailedChecks(detailedChecks);
        
        KafkaMultiDatacenterHealthAutoConfiguration config = new KafkaMultiDatacenterHealthAutoConfiguration();
        return config.kafkaMultiDatacenterHealthIndicator(client, properties);
    }
}
