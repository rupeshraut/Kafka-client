package com.kafka.multidc.config;

import com.kafka.multidc.deadletter.DeadLetterQueueHandler;
import com.kafka.multidc.routing.RoutingStrategy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for Kafka multi-datacenter client.
 * Immutable configuration object with builder pattern.
 */
public class KafkaDatacenterConfiguration {
    
    private final List<KafkaDatacenterEndpoint> datacenters;
    private final String localDatacenterId;
    private final RoutingStrategy routingStrategy;
    private final Duration healthCheckInterval;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;
    private final ResilienceConfig resilienceConfig;
    private final FallbackConfiguration fallbackConfiguration;
    private final SchemaRegistryConfig schemaRegistryConfig;
    private final SecurityConfig securityConfig;
    private final ProducerConfig producerConfig;
    private final ConsumerConfig consumerConfig;
    private final DeadLetterQueueHandler.DeadLetterConfig deadLetterConfig;
    private final Map<String, Object> additionalProperties;
    private final boolean enableMetrics;
    private final boolean enableDetailedMetrics;
    private final String metricsPrefix;
    
    private KafkaDatacenterConfiguration(Builder builder) {
        this.datacenters = List.copyOf(builder.datacenters);
        this.localDatacenterId = builder.localDatacenterId;
        this.routingStrategy = builder.routingStrategy;
        this.healthCheckInterval = builder.healthCheckInterval;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.resilienceConfig = builder.resilienceConfig != null ? builder.resilienceConfig : createDefaultResilienceConfig();
        this.fallbackConfiguration = builder.fallbackConfiguration != null ? builder.fallbackConfiguration : createDefaultFallbackConfig();
        this.schemaRegistryConfig = builder.schemaRegistryConfig;
        this.securityConfig = builder.securityConfig;
        this.producerConfig = builder.producerConfig != null ? builder.producerConfig : createDefaultProducerConfig();
        this.consumerConfig = builder.consumerConfig != null ? builder.consumerConfig : createDefaultConsumerConfig();
        this.deadLetterConfig = builder.deadLetterConfig;
        this.additionalProperties = Map.copyOf(builder.additionalProperties);
        this.enableMetrics = builder.enableMetrics;
        this.enableDetailedMetrics = builder.enableDetailedMetrics;
        this.metricsPrefix = builder.metricsPrefix;
        
        validate();
    }
    
    private static ResilienceConfig createDefaultResilienceConfig() {
        return ResilienceConfig.builder().build();
    }
    
    private static FallbackConfiguration createDefaultFallbackConfig() {
        return new FallbackConfiguration();
    }
    
    private static ProducerConfig createDefaultProducerConfig() {
        return new ProducerConfig();
    }
    
    private static ConsumerConfig createDefaultConsumerConfig() {
        return new ConsumerConfig();
    }
    
    private void validate() {
        if (datacenters == null || datacenters.isEmpty()) {
            throw new IllegalArgumentException("At least one datacenter must be configured");
        }
        
        if (localDatacenterId != null) {
            boolean localFound = datacenters.stream()
                .anyMatch(dc -> dc.getId().equals(localDatacenterId));
            if (!localFound) {
                throw new IllegalArgumentException("Local datacenter ID not found in configured datacenters");
            }
        }
        
        if (healthCheckInterval.isNegative() || healthCheckInterval.isZero()) {
            throw new IllegalArgumentException("Health check interval must be positive");
        }
        
        if (connectionTimeout.isNegative() || connectionTimeout.isZero()) {
            throw new IllegalArgumentException("Connection timeout must be positive");
        }
    }
    
    // Getters
    public List<KafkaDatacenterEndpoint> getDatacenters() { return datacenters; }
    public String getLocalDatacenterId() { return localDatacenterId; }
    public RoutingStrategy getRoutingStrategy() { return routingStrategy; }
    public Duration getHealthCheckInterval() { return healthCheckInterval; }
    public Duration getConnectionTimeout() { return connectionTimeout; }
    public Duration getRequestTimeout() { return requestTimeout; }
    public ResilienceConfig getResilienceConfig() { return resilienceConfig; }
    public FallbackConfiguration getFallbackConfiguration() { return fallbackConfiguration; }
    public SchemaRegistryConfig getSchemaRegistryConfig() { return schemaRegistryConfig; }
    public SecurityConfig getSecurityConfig() { return securityConfig; }
    public ProducerConfig getProducerConfig() { return producerConfig; }
    public ConsumerConfig getConsumerConfig() { return consumerConfig; }
    public DeadLetterQueueHandler.DeadLetterConfig getDeadLetterConfig() { return deadLetterConfig; }
    public Map<String, Object> getAdditionalProperties() { return additionalProperties; }
    public boolean isMetricsEnabled() { return enableMetrics; }
    public boolean isDetailedMetricsEnabled() { return enableDetailedMetrics; }
    public String getMetricsPrefix() { return metricsPrefix; }
    
    /**
     * Create a new builder instance.
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for KafkaDatacenterConfiguration.
     */
    public static class Builder {
        private List<KafkaDatacenterEndpoint> datacenters = List.of();
        private String localDatacenterId;
        private RoutingStrategy routingStrategy = RoutingStrategy.LATENCY_BASED;
        private Duration healthCheckInterval = Duration.ofSeconds(30);
        private Duration connectionTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private ResilienceConfig resilienceConfig;
        private FallbackConfiguration fallbackConfiguration;
        private SchemaRegistryConfig schemaRegistryConfig;
        private SecurityConfig securityConfig;
        private ProducerConfig producerConfig;
        private ConsumerConfig consumerConfig;
        private DeadLetterQueueHandler.DeadLetterConfig deadLetterConfig;
        private Map<String, Object> additionalProperties = Map.of();
        private boolean enableMetrics = true;
        private boolean enableDetailedMetrics = false;
        private String metricsPrefix = "kafka.multidc";
        
        public Builder datacenters(List<KafkaDatacenterEndpoint> datacenters) {
            this.datacenters = datacenters != null ? datacenters : List.of();
            return this;
        }
        
        public Builder addDatacenter(KafkaDatacenterEndpoint datacenter) {
            this.datacenters = new java.util.ArrayList<>(this.datacenters);
            this.datacenters.add(datacenter);
            return this;
        }
        
        public Builder localDatacenter(String localDatacenterId) {
            this.localDatacenterId = localDatacenterId;
            return this;
        }
        
        public Builder routingStrategy(RoutingStrategy routingStrategy) {
            this.routingStrategy = Objects.requireNonNull(routingStrategy);
            return this;
        }
        
        public Builder healthCheckInterval(Duration healthCheckInterval) {
            this.healthCheckInterval = Objects.requireNonNull(healthCheckInterval);
            return this;
        }
        
        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = Objects.requireNonNull(connectionTimeout);
            return this;
        }
        
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = Objects.requireNonNull(requestTimeout);
            return this;
        }
        
        public Builder resilienceConfig(ResilienceConfig resilienceConfig) {
            this.resilienceConfig = Objects.requireNonNull(resilienceConfig);
            return this;
        }
        
        public Builder fallbackConfiguration(FallbackConfiguration fallbackConfiguration) {
            this.fallbackConfiguration = Objects.requireNonNull(fallbackConfiguration);
            return this;
        }
        
        public Builder schemaRegistryConfig(SchemaRegistryConfig schemaRegistryConfig) {
            this.schemaRegistryConfig = schemaRegistryConfig;
            return this;
        }
        
        public Builder securityConfig(SecurityConfig securityConfig) {
            this.securityConfig = securityConfig;
            return this;
        }
        
        public Builder producerConfig(ProducerConfig producerConfig) {
            this.producerConfig = Objects.requireNonNull(producerConfig);
            return this;
        }
        
        public Builder consumerConfig(ConsumerConfig consumerConfig) {
            this.consumerConfig = Objects.requireNonNull(consumerConfig);
            return this;
        }
        
        public Builder deadLetterConfig(DeadLetterQueueHandler.DeadLetterConfig deadLetterConfig) {
            this.deadLetterConfig = deadLetterConfig;
            return this;
        }
        
        public Builder additionalProperties(Map<String, Object> additionalProperties) {
            this.additionalProperties = additionalProperties != null ? additionalProperties : Map.of();
            return this;
        }
        
        public Builder enableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
            return this;
        }
        
        public Builder enableDetailedMetrics(boolean enableDetailedMetrics) {
            this.enableDetailedMetrics = enableDetailedMetrics;
            return this;
        }
        
        public Builder metricsPrefix(String metricsPrefix) {
            this.metricsPrefix = Objects.requireNonNull(metricsPrefix);
            return this;
        }
        
        public KafkaDatacenterConfiguration build() {
            return new KafkaDatacenterConfiguration(this);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaDatacenterConfiguration that = (KafkaDatacenterConfiguration) o;
        return enableMetrics == that.enableMetrics &&
               enableDetailedMetrics == that.enableDetailedMetrics &&
               Objects.equals(datacenters, that.datacenters) &&
               Objects.equals(localDatacenterId, that.localDatacenterId) &&
               routingStrategy == that.routingStrategy &&
               Objects.equals(healthCheckInterval, that.healthCheckInterval) &&
               Objects.equals(connectionTimeout, that.connectionTimeout) &&
               Objects.equals(requestTimeout, that.requestTimeout) &&
               Objects.equals(resilienceConfig, that.resilienceConfig) &&
               Objects.equals(fallbackConfiguration, that.fallbackConfiguration) &&
               Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig) &&
               Objects.equals(securityConfig, that.securityConfig) &&
               Objects.equals(producerConfig, that.producerConfig) &&
               Objects.equals(consumerConfig, that.consumerConfig) &&
               Objects.equals(deadLetterConfig, that.deadLetterConfig) &&
               Objects.equals(additionalProperties, that.additionalProperties) &&
               Objects.equals(metricsPrefix, that.metricsPrefix);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(datacenters, localDatacenterId, routingStrategy, healthCheckInterval,
                           connectionTimeout, requestTimeout, resilienceConfig, fallbackConfiguration,
                           schemaRegistryConfig, securityConfig, producerConfig, consumerConfig,
                           deadLetterConfig, additionalProperties, enableMetrics, enableDetailedMetrics, metricsPrefix);
    }
    
    @Override
    public String toString() {
        return "KafkaDatacenterConfiguration{" +
               "datacenters=" + datacenters.size() +
               ", localDatacenterId='" + localDatacenterId + '\'' +
               ", routingStrategy=" + routingStrategy +
               ", healthCheckInterval=" + healthCheckInterval +
               ", connectionTimeout=" + connectionTimeout +
               ", requestTimeout=" + requestTimeout +
               ", enableMetrics=" + enableMetrics +
               ", enableDetailedMetrics=" + enableDetailedMetrics +
               ", metricsPrefix='" + metricsPrefix + '\'' +
               '}';
    }
}
