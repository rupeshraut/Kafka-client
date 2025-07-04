package com.kafka.multidc.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Schema Registry configuration for multi-datacenter Kafka deployments.
 * Supports Confluent Schema Registry with comprehensive security and caching capabilities.
 */
public class SchemaRegistryConfig {
    
    private final List<String> urls;
    private final String basicAuthUsername;
    private final String basicAuthPassword;
    private final String bearerToken;
    private final SecurityConfig securityConfig;
    private final Duration connectionTimeout;
    private final Duration readTimeout;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final boolean cacheEnabled;
    private final int cacheMaxSize;
    private final Duration cacheTtl;
    private final String subjectNameStrategy;
    private final Map<String, String> additionalProperties;
    private final boolean failoverEnabled;
    private final Duration healthCheckInterval;
    private final String userAgent;
    private final Map<String, List<String>> datacenterUrls;
    
    private SchemaRegistryConfig(Builder builder) {
        this.urls = List.copyOf(builder.urls);
        this.basicAuthUsername = builder.basicAuthUsername;
        this.basicAuthPassword = builder.basicAuthPassword;
        this.bearerToken = builder.bearerToken;
        this.securityConfig = builder.securityConfig;
        this.connectionTimeout = builder.connectionTimeout;
        this.readTimeout = builder.readTimeout;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff;
        this.cacheEnabled = builder.cacheEnabled;
        this.cacheMaxSize = builder.cacheMaxSize;
        this.cacheTtl = builder.cacheTtl;
        this.subjectNameStrategy = builder.subjectNameStrategy;
        this.additionalProperties = Map.copyOf(builder.additionalProperties);
        this.failoverEnabled = builder.failoverEnabled;
        this.healthCheckInterval = builder.healthCheckInterval;
        this.userAgent = builder.userAgent;
        this.datacenterUrls = Map.copyOf(builder.datacenterUrls);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public List<String> getUrls() { return urls; }
    public String getBasicAuthUsername() { return basicAuthUsername; }
    public String getBasicAuthPassword() { return basicAuthPassword; }
    public String getBearerToken() { return bearerToken; }
    public SecurityConfig getSecurityConfig() { return securityConfig; }
    public Duration getConnectionTimeout() { return connectionTimeout; }
    public Duration getReadTimeout() { return readTimeout; }
    public int getMaxRetries() { return maxRetries; }
    public Duration getRetryBackoff() { return retryBackoff; }
    public boolean isCacheEnabled() { return cacheEnabled; }
    public int getCacheMaxSize() { return cacheMaxSize; }
    public Duration getCacheTtl() { return cacheTtl; }
    public String getSubjectNameStrategy() { return subjectNameStrategy; }
    public Map<String, String> getAdditionalProperties() { return additionalProperties; }
    public boolean isFailoverEnabled() { return failoverEnabled; }
    public Duration getHealthCheckInterval() { return healthCheckInterval; }
    public String getUserAgent() { return userAgent; }
    public Map<String, List<String>> getDatacenterUrls() { return datacenterUrls; }
    
    /**
     * Get URLs for a specific datacenter.
     */
    public List<String> getUrlsForDatacenter(String datacenterId) {
        return datacenterUrls.getOrDefault(datacenterId, urls);
    }
    
    /**
     * Check if authentication is configured.
     */
    public boolean hasAuthentication() {
        return (basicAuthUsername != null && basicAuthPassword != null) || bearerToken != null;
    }
    
    public static class Builder {
        private List<String> urls = List.of("http://localhost:8081");
        private String basicAuthUsername;
        private String basicAuthPassword;
        private String bearerToken;
        private SecurityConfig securityConfig;
        private Duration connectionTimeout = Duration.ofSeconds(30);
        private Duration readTimeout = Duration.ofSeconds(30);
        private int maxRetries = 3;
        private Duration retryBackoff = Duration.ofMillis(500);
        private boolean cacheEnabled = true;
        private int cacheMaxSize = 1000;
        private Duration cacheTtl = Duration.ofMinutes(30);
        private String subjectNameStrategy = "io.confluent.kafka.serializers.subject.TopicNameStrategy";
        private Map<String, String> additionalProperties = Map.of();
        private boolean failoverEnabled = true;
        private Duration healthCheckInterval = Duration.ofMinutes(1);
        private String userAgent = "kafka-multidc-client";
        private Map<String, List<String>> datacenterUrls = Map.of();
        
        /**
         * Set the schema registry URLs.
         */
        public Builder urls(String... urls) {
            this.urls = List.of(urls);
            return this;
        }
        
        /**
         * Set the schema registry URLs.
         */
        public Builder urls(List<String> urls) {
            this.urls = List.copyOf(urls);
            return this;
        }
        
        /**
         * Configure basic authentication.
         */
        public Builder basicAuth(String username, String password) {
            this.basicAuthUsername = username;
            this.basicAuthPassword = password;
            return this;
        }
        
        /**
         * Configure bearer token authentication.
         */
        public Builder bearerToken(String token) {
            this.bearerToken = token;
            return this;
        }
        
        /**
         * Configure SSL/TLS security.
         */
        public Builder security(SecurityConfig securityConfig) {
            this.securityConfig = securityConfig;
            return this;
        }
        
        /**
         * Set connection timeout.
         */
        public Builder connectionTimeout(Duration timeout) {
            this.connectionTimeout = timeout;
            return this;
        }
        
        /**
         * Set read timeout.
         */
        public Builder readTimeout(Duration timeout) {
            this.readTimeout = timeout;
            return this;
        }
        
        /**
         * Configure retry behavior.
         */
        public Builder retries(int maxRetries, Duration backoff) {
            this.maxRetries = maxRetries;
            this.retryBackoff = backoff;
            return this;
        }
        
        /**
         * Configure schema caching.
         */
        public Builder cache(boolean enabled, int maxSize, Duration ttl) {
            this.cacheEnabled = enabled;
            this.cacheMaxSize = maxSize;
            this.cacheTtl = ttl;
            return this;
        }
        
        /**
         * Disable schema caching.
         */
        public Builder disableCache() {
            this.cacheEnabled = false;
            return this;
        }
        
        /**
         * Set subject name strategy.
         */
        public Builder subjectNameStrategy(String strategy) {
            this.subjectNameStrategy = strategy;
            return this;
        }
        
        /**
         * Set subject name strategy using class.
         */
        public Builder subjectNameStrategy(Class<?> strategyClass) {
            this.subjectNameStrategy = strategyClass.getName();
            return this;
        }
        
        /**
         * Add additional client properties.
         */
        public Builder additionalProperties(Map<String, String> properties) {
            this.additionalProperties = Map.copyOf(properties);
            return this;
        }
        
        /**
         * Configure failover behavior.
         */
        public Builder failover(boolean enabled) {
            this.failoverEnabled = enabled;
            return this;
        }
        
        /**
         * Set health check interval.
         */
        public Builder healthCheckInterval(Duration interval) {
            this.healthCheckInterval = interval;
            return this;
        }
        
        /**
         * Set custom user agent.
         */
        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }
        
        /**
         * Configure datacenter-specific URLs.
         */
        public Builder datacenterUrls(Map<String, List<String>> datacenterUrls) {
            this.datacenterUrls = Map.copyOf(datacenterUrls);
            return this;
        }
        
        /**
         * Add URLs for a specific datacenter.
         */
        public Builder addDatacenterUrls(String datacenterId, String... urls) {
            var currentUrls = Map.copyOf(this.datacenterUrls);
            var newUrls = new java.util.HashMap<>(currentUrls);
            newUrls.put(datacenterId, List.of(urls));
            this.datacenterUrls = Map.copyOf(newUrls);
            return this;
        }
        
        public SchemaRegistryConfig build() {
            // Validation
            if (urls.isEmpty()) {
                throw new IllegalStateException("At least one schema registry URL must be provided");
            }
            
            if (maxRetries < 0) {
                throw new IllegalArgumentException("Max retries cannot be negative");
            }
            
            if (cacheMaxSize <= 0) {
                throw new IllegalArgumentException("Cache max size must be positive");
            }
            
            return new SchemaRegistryConfig(this);
        }
    }
}
