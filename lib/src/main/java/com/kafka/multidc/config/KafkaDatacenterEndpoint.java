package com.kafka.multidc.config;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for a Kafka datacenter endpoint.
 * Immutable configuration with builder pattern.
 */
public class KafkaDatacenterEndpoint {
    
    private final String id;
    private final String region;
    private final String bootstrapServers;
    private final int priority;
    private final boolean ssl;
    private final String securityProtocol;
    private final String saslMechanism;
    private final String saslUsername;
    private final String saslPassword;
    private final String keystorePath;
    private final String keystorePassword;
    private final String truststorePath;
    private final String truststorePassword;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;
    private final int maxConnections;
    private final int minConnections;
    private final boolean enableIdempotence;
    private final String compressionType;
    private final Map<String, Object> additionalProperties;
    
    private KafkaDatacenterEndpoint(Builder builder) {
        this.id = builder.id;
        this.region = builder.region;
        this.bootstrapServers = builder.bootstrapServers;
        this.priority = builder.priority;
        this.ssl = builder.ssl;
        this.securityProtocol = builder.securityProtocol;
        this.saslMechanism = builder.saslMechanism;
        this.saslUsername = builder.saslUsername;
        this.saslPassword = builder.saslPassword;
        this.keystorePath = builder.keystorePath;
        this.keystorePassword = builder.keystorePassword;
        this.truststorePath = builder.truststorePath;
        this.truststorePassword = builder.truststorePassword;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.maxConnections = builder.maxConnections;
        this.minConnections = builder.minConnections;
        this.enableIdempotence = builder.enableIdempotence;
        this.compressionType = builder.compressionType;
        this.additionalProperties = Map.copyOf(builder.additionalProperties);
        
        validate();
    }
    
    private void validate() {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Datacenter ID cannot be null or empty");
        }
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("Bootstrap servers cannot be null or empty");
        }
        if (priority < 0) {
            throw new IllegalArgumentException("Priority cannot be negative");
        }
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("Max connections must be positive");
        }
        if (minConnections < 0) {
            throw new IllegalArgumentException("Min connections cannot be negative");
        }
        if (minConnections > maxConnections) {
            throw new IllegalArgumentException("Min connections cannot exceed max connections");
        }
    }
    
    // Getters
    public String getId() { return id; }
    public String getRegion() { return region; }
    public String getBootstrapServers() { return bootstrapServers; }
    public int getPriority() { return priority; }
    public boolean isSsl() { return ssl; }
    public String getSecurityProtocol() { return securityProtocol; }
    public String getSaslMechanism() { return saslMechanism; }
    public String getSaslUsername() { return saslUsername; }
    public String getSaslPassword() { return saslPassword; }
    public String getKeystorePath() { return keystorePath; }
    public String getKeystorePassword() { return keystorePassword; }
    public String getTruststorePath() { return truststorePath; }
    public String getTruststorePassword() { return truststorePassword; }
    public Duration getConnectionTimeout() { return connectionTimeout; }
    public Duration getRequestTimeout() { return requestTimeout; }
    public int getMaxConnections() { return maxConnections; }
    public int getMinConnections() { return minConnections; }
    public boolean isIdempotenceEnabled() { return enableIdempotence; }
    public String getCompressionType() { return compressionType; }
    public Map<String, Object> getAdditionalProperties() { return additionalProperties; }
    
    /**
     * Create a new builder instance.
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for KafkaDatacenterEndpoint.
     */
    public static class Builder {
        private String id;
        private String region;
        private String bootstrapServers;
        private int priority = 1;
        private boolean ssl = false;
        private String securityProtocol = "PLAINTEXT";
        private String saslMechanism;
        private String saslUsername;
        private String saslPassword;
        private String keystorePath;
        private String keystorePassword;
        private String truststorePath;
        private String truststorePassword;
        private Duration connectionTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private int maxConnections = 50;
        private int minConnections = 5;
        private boolean enableIdempotence = true;
        private String compressionType = "lz4";
        private Map<String, Object> additionalProperties = Map.of();
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }
        
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            if (ssl) {
                this.securityProtocol = "SSL";
            }
            return this;
        }
        
        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }
        
        public Builder saslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }
        
        public Builder saslCredentials(String username, String password) {
            this.saslUsername = username;
            this.saslPassword = password;
            return this;
        }
        
        public Builder keystore(String keystorePath, String keystorePassword) {
            this.keystorePath = keystorePath;
            this.keystorePassword = keystorePassword;
            return this;
        }
        
        public Builder truststore(String truststorePath, String truststorePassword) {
            this.truststorePath = truststorePath;
            this.truststorePassword = truststorePassword;
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
        
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }
        
        public Builder minConnections(int minConnections) {
            this.minConnections = minConnections;
            return this;
        }
        
        public Builder enableIdempotence(boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence;
            return this;
        }
        
        public Builder compressionType(String compressionType) {
            this.compressionType = compressionType;
            return this;
        }
        
        public Builder additionalProperties(Map<String, Object> additionalProperties) {
            this.additionalProperties = additionalProperties != null ? additionalProperties : Map.of();
            return this;
        }
        
        public Builder property(String key, Object value) {
            this.additionalProperties = new java.util.HashMap<>(this.additionalProperties);
            this.additionalProperties.put(key, value);
            return this;
        }
        
        public KafkaDatacenterEndpoint build() {
            return new KafkaDatacenterEndpoint(this);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaDatacenterEndpoint that = (KafkaDatacenterEndpoint) o;
        return priority == that.priority &&
               ssl == that.ssl &&
               maxConnections == that.maxConnections &&
               minConnections == that.minConnections &&
               enableIdempotence == that.enableIdempotence &&
               Objects.equals(id, that.id) &&
               Objects.equals(region, that.region) &&
               Objects.equals(bootstrapServers, that.bootstrapServers) &&
               Objects.equals(securityProtocol, that.securityProtocol) &&
               Objects.equals(saslMechanism, that.saslMechanism) &&
               Objects.equals(saslUsername, that.saslUsername) &&
               Objects.equals(connectionTimeout, that.connectionTimeout) &&
               Objects.equals(requestTimeout, that.requestTimeout) &&
               Objects.equals(compressionType, that.compressionType) &&
               Objects.equals(additionalProperties, that.additionalProperties);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, region, bootstrapServers, priority, ssl, securityProtocol,
                           saslMechanism, saslUsername, connectionTimeout, requestTimeout,
                           maxConnections, minConnections, enableIdempotence, compressionType,
                           additionalProperties);
    }
    
    @Override
    public String toString() {
        return "KafkaDatacenterEndpoint{" +
               "id='" + id + '\'' +
               ", region='" + region + '\'' +
               ", bootstrapServers='" + bootstrapServers + '\'' +
               ", priority=" + priority +
               ", ssl=" + ssl +
               ", securityProtocol='" + securityProtocol + '\'' +
               ", compressionType='" + compressionType + '\'' +
               '}';
    }
}
