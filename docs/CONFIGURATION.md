# Configuration Guide

This guide provides comprehensive information on configuring the Kafka Multi-Datacenter Client Library for various deployment scenarios.

## Table of Contents

- [Overview](#overview)
- [Basic Configuration](#basic-configuration)
- [Multi-Datacenter Configuration](#multi-datacenter-configuration)
- [Producer Configuration](#producer-configuration)
- [Consumer Configuration](#consumer-configuration)
- [Security Configuration](#security-configuration)
- [Resilience Configuration](#resilience-configuration)
- [Schema Registry Configuration](#schema-registry-configuration)
- [Performance Tuning](#performance-tuning)
- [Environment-Specific Configuration](#environment-specific-configuration)
- [Configuration Examples](#configuration-examples)

## Overview

The Kafka Multi-Datacenter Client Library uses a builder pattern for configuration, providing type safety and validation. All configuration objects are immutable once built, ensuring thread safety.

### Key Configuration Components

- **KafkaDatacenterConfiguration**: Main configuration container
- **KafkaDatacenterEndpoint**: Individual datacenter endpoint configuration
- **ProducerConfig**: Producer-specific settings
- **ConsumerConfig**: Consumer-specific settings
- **SecurityConfig**: Security and authentication settings
- **ResilienceConfig**: Fault tolerance and retry configuration
- **SchemaRegistryConfig**: Schema registry integration settings

## Basic Configuration

### Minimal Configuration

```java
// Single datacenter setup
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(KafkaDatacenterEndpoint.builder()
        .id("local")
        .region("us-east-1")
        .bootstrapServers("localhost:9092")
        .build())
    .localDatacenter("local")
    .build();
```

### Builder Pattern Usage

All configuration classes follow the builder pattern:

```java
// Configuration with validation
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(endpoint1)
    .addDatacenter(endpoint2)
    .localDatacenter("primary")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .producerConfig(ProducerConfig.defaultConfig())
    .consumerConfig(ConsumerConfig.defaultConfig())
    .healthCheckInterval(Duration.ofSeconds(30))
    .build(); // Validates all required fields
```

## Multi-Datacenter Configuration

### Basic Multi-Datacenter Setup

```java
// Define datacenter endpoints
KafkaDatacenterEndpoint primaryDC = KafkaDatacenterEndpoint.builder()
    .id("us-east-1")
    .region("us-east-1")
    .bootstrapServers("kafka-east.company.com:9092,kafka-east-2.company.com:9092")
    .priority(1)
    .build();

KafkaDatacenterEndpoint secondaryDC = KafkaDatacenterEndpoint.builder()
    .id("us-west-2")
    .region("us-west-2")
    .bootstrapServers("kafka-west.company.com:9092,kafka-west-2.company.com:9092")
    .priority(2)
    .build();

KafkaDatacenterEndpoint emergencyDC = KafkaDatacenterEndpoint.builder()
    .id("eu-west-1")
    .region("eu-west-1")
    .bootstrapServers("kafka-eu.company.com:9092")
    .priority(3)
    .build();

// Multi-datacenter configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(primaryDC)
    .addDatacenter(secondaryDC)
    .addDatacenter(emergencyDC)
    .localDatacenter("us-east-1")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .enableCrossDCOperations(true)
    .build();
```

### Datacenter Endpoint Configuration

```java
KafkaDatacenterEndpoint endpoint = KafkaDatacenterEndpoint.builder()
    // Required fields
    .id("datacenter-id")
    .region("aws-region")
    .bootstrapServers("host1:9092,host2:9092")
    
    // Optional fields
    .datacenterPriority(1)
    .availabilityZone("us-east-1a")
    .rack("rack-1")
    .tags(Map.of("environment", "production", "tier", "primary"))
    .healthCheckEndpoint("http://health.company.com/kafka")
    .customProperties(Map.of("custom.property", "value"))
    
    // Connection settings
    .connectionTimeout(Duration.ofSeconds(10))
    .requestTimeout(Duration.ofSeconds(30))
    .maxConnections(50)
    
    // Health settings
    .enableHealthChecks(true)
    .healthCheckInterval(Duration.ofSeconds(15))
    .healthCheckTimeout(Duration.ofSeconds(5))
    
    .build();
```

### Routing Strategy Configuration

```java
// Different routing strategies
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(primary)
    .addDatacenter(secondary)
    .localDatacenter("primary")
    
    // Routing strategies
    .routingStrategy(RoutingStrategy.LATENCY_BASED)    // Route to lowest latency DC
    // .routingStrategy(RoutingStrategy.NEAREST)        // Route to geographically nearest DC
    // .routingStrategy(RoutingStrategy.ROUND_ROBIN)    // Distribute load evenly
    // .routingStrategy(RoutingStrategy.LOCAL_ONLY)     // Only use local datacenter
    // .routingStrategy(RoutingStrategy.PRIORITY_BASED) // Use datacenter priority
    
    .build();
```

## Producer Configuration

### Basic Producer Configuration

```java
// The library provides a simplified ProducerConfig
ProducerConfig producerConfig = ProducerConfig.defaultConfig();

// For custom producer settings, use the actual Kafka producer properties
// through the additional properties in datacenter configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .producerConfig(producerConfig)
    .additionalProperties(Map.of(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer", "org.apache.kafka.common.serialization.JsonSerializer",
        "batch.size", 16384,
        "linger.ms", 5,
        "compression.type", "snappy",
        "acks", "all",
        "retries", Integer.MAX_VALUE,
        "enable.idempotence", true
    ))
    .build();
```

### Advanced Producer Configuration

```java
// Use additional properties for advanced Kafka producer configuration
ProducerConfig producerConfig = ProducerConfig.defaultConfig();

KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .producerConfig(producerConfig)
    .additionalProperties(Map.of(
        "key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "batch.size", 32768,
        "buffer.memory", 67108864L, // 64MB
        "linger.ms", 10,
        "compression.type", "lz4",
        "acks", "all",
        "retries", Integer.MAX_VALUE,
        "enable.idempotence", true
    ))
    .build();
    
    // Partitioning
    .partitionerClass("com.kafka.multidc.partitioning.CustomPartitioner")
    
    // Monitoring
    .enableMetrics(true)
    .metricsRecordingLevel("INFO")
    .metricReporters(List.of("com.kafka.multidc.metrics.CustomMetricsReporter"))
    
    // Custom properties
    .customProperties(Map.of(
        "schema.registry.url", "http://schema-registry:8081",
        "auto.register.schemas", "false",
        "use.latest.version", "true"
    ))
    
    .build();
```

### Producer Defaults

```java
// Use default configuration with customizations
ProducerConfig config = ProducerConfig.defaultConfig()
    .toBuilder()
    .compressionType("snappy")
    .batchSize(32768)
    .build();
```

## Consumer Configuration

### Basic Consumer Configuration

```java
// The library provides a simplified ConsumerConfig
ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();

// For custom consumer settings, use the actual Kafka consumer properties
// through the additional properties in datacenter configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .consumerConfig(consumerConfig)
    .additionalProperties(Map.of(
        "group.id", "my-consumer-group",
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer", 
        "value.deserializer", "org.apache.kafka.common.serialization.JsonDeserializer",
        "auto.offset.reset", "earliest",
        "enable.auto.commit", false, // Manual commit for better control
        "max.poll.records", 500,
        "session.timeout.ms", 30000,
        "heartbeat.interval.ms", 3000
    ))
    .build();
```

### Advanced Consumer Configuration

```java
// Use additional properties for advanced Kafka consumer configuration
ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();

KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .consumerConfig(consumerConfig)
    .additionalProperties(Map.of(
        "group.id", "high-throughput-consumer-group",
        "client.id", "consumer-instance-1",
        "key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        "auto.offset.reset", "latest",
        "enable.auto.commit", false,
        "max.poll.records", 1000,
        "fetch.min.bytes", 50000,
        "fetch.max.wait.ms", 500,
        "session.timeout.ms", 45000,
        "heartbeat.interval.ms", 3000,
        "max.poll.interval.ms", 600000, // 10 minutes
        "isolation.level", "read_committed",
        "schema.registry.url", "http://schema-registry:8081",
        "specific.avro.reader", true
    ))
    .build();
```

### Consumer Group Management

```java
ConsumerConfig consumerConfig = ConsumerConfig.builder()
    .groupId("datacenter-aware-group")
    
    // Partition assignment strategy
    .partitionAssignmentStrategy(List.of(
        "com.kafka.multidc.consumer.DatacenterAwareRangeAssignor",
        "org.apache.kafka.clients.consumer.RangeAssignor"
    ))
    
    // Consumer group coordination
    .sessionTimeoutMs(30000)
    .heartbeatIntervalMs(3000)
    .maxPollIntervalMs(300000)
    
    // Rebalancing settings
    .groupInstanceId("consumer-instance-1") // Static membership
    
    .build();
```

## Security Configuration

### SSL/TLS Configuration

```java
SecurityConfig securityConfig = SecurityConfig.builder()
    // Protocol and encryption
    .securityProtocol("SSL")
    .sslProtocol("TLSv1.2")
    
    // Trust store (for server verification)
    .sslTruststoreLocation("/path/to/kafka.client.truststore.jks")
    .sslTruststorePassword("truststore-password")
    .sslTruststoreType("JKS")
    
    // Key store (for client authentication)
    .sslKeystoreLocation("/path/to/kafka.client.keystore.jks")
    .sslKeystorePassword("keystore-password")
    .sslKeyPassword("key-password")
    .sslKeystoreType("JKS")
    
    // SSL verification
    .sslEndpointIdentificationAlgorithm("https")
    .sslCheckHostname(true)
    
    // SSL context configuration
    .sslProvider("SunJSSE")
    .sslCipherSuites(List.of(
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    ))
    .sslEnabledProtocols(List.of("TLSv1.2", "TLSv1.3"))
    
    .build();
```

### SASL Authentication

```java
SecurityConfig securityConfig = SecurityConfig.builder()
    // SASL/SCRAM authentication
    .securityProtocol("SASL_SSL")
    .saslMechanism("SCRAM-SHA-512")
    .saslJaasConfig(
        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
        "username=\"kafka-user\" " +
        "password=\"secure-password\";"
    )
    
    // SSL settings for SASL_SSL
    .sslTruststoreLocation("/path/to/truststore.jks")
    .sslTruststorePassword("truststore-password")
    
    .build();
```

### Kerberos Authentication

```java
SecurityConfig securityConfig = SecurityConfig.builder()
    // Kerberos authentication
    .securityProtocol("SASL_SSL")
    .saslMechanism("GSSAPI")
    .saslKerberosServiceName("kafka")
    .saslJaasConfig(
        "com.sun.security.auth.module.Krb5LoginModule required " +
        "useKeyTab=true " +
        "storeKey=true " +
        "keyTab=\"/path/to/kafka.keytab\" " +
        "principal=\"kafka-client@COMPANY.COM\";"
    )
    
    .build();
```

## Resilience Configuration

### Basic Resilience Configuration

```java
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    // Circuit breaker
    .circuitBreakerEnabled(true)
    .failureRateThreshold(50.0f)
    .waitDurationInOpenState(Duration.ofSeconds(30))
    .slidingWindowSize(100)
    .minimumNumberOfCalls(10)
    
    // Retry configuration
    .retryEnabled(true)
    .maxRetryAttempts(3)
    .waitDuration(Duration.ofSeconds(1))
    .exponentialBackoffMultiplier(2.0)
    .maxWaitDuration(Duration.ofSeconds(10))
    
    // Rate limiter
    .rateLimiterEnabled(true)
    .limitForPeriod(1000)
    .limitRefreshPeriod(Duration.ofSeconds(1))
    .timeoutDuration(Duration.ofSeconds(5))
    
    // Bulkhead
    .bulkheadEnabled(true)
    .maxConcurrentCalls(25)
    .maxWaitDuration(Duration.ofSeconds(2))
    
    // Time limiter
    .timeLimiterEnabled(true)
    .timeoutDuration(Duration.ofSeconds(30))
    
    .build();
```

### Advanced Resilience Configuration

```java
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    // Circuit breaker with custom settings
    .circuitBreakerEnabled(true)
    .failureRateThreshold(60.0f)
    .slowCallRateThreshold(80.0f)
    .slowCallDurationThreshold(Duration.ofSeconds(2))
    .waitDurationInOpenState(Duration.ofMinutes(1))
    .slidingWindowType(SlidingWindowType.COUNT_BASED)
    .slidingWindowSize(50)
    .minimumNumberOfCalls(20)
    .permittedNumberOfCallsInHalfOpenState(5)
    
    // Retry with custom logic
    .retryEnabled(true)
    .maxRetryAttempts(5)
    .waitDuration(Duration.ofMillis(500))
    .exponentialBackoffMultiplier(1.5)
    .maxWaitDuration(Duration.ofSeconds(30))
    .retryOnExceptions(List.of(
        "org.apache.kafka.common.errors.RetriableException",
        "java.net.ConnectException"
    ))
    .ignoreExceptions(List.of(
        "org.apache.kafka.common.errors.AuthorizationException"
    ))
    
    // Rate limiter per datacenter
    .rateLimiterEnabled(true)
    .limitForPeriod(2000)
    .limitRefreshPeriod(Duration.ofSeconds(1))
    .timeoutDuration(Duration.ofSeconds(10))
    
    // Bulkhead isolation
    .bulkheadEnabled(true)
    .maxConcurrentCalls(50)
    .maxWaitDuration(Duration.ofSeconds(5))
    
    // Global timeout
    .timeLimiterEnabled(true)
    .timeoutDuration(Duration.ofMinutes(2))
    .cancelRunningFuture(true)
    
    .build();
```

## Schema Registry Configuration

### Basic Schema Registry Configuration

```java
SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
    .urls("http://schema-registry:8081")
    .basicAuth("schema-user", "schema-password")
    .connectionTimeout(Duration.ofSeconds(10))
    .readTimeout(Duration.ofSeconds(30))
    .retries(3, Duration.ofMillis(1000))
    .build();
```

### Advanced Schema Registry Configuration

```java
SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
    // Multiple URLs for HA
    .urls("http://schema-registry-1:8081", "http://schema-registry-2:8081")
    
    // Authentication
    .basicAuth("schema-client", "secure-password")
    // .bearerToken("jwt-token") // Alternative authentication
    
    // SSL/TLS security
    .security(SecurityConfig.builder()
        .securityProtocol("SSL")
        .sslTruststoreLocation("/path/to/truststore.jks")
        .sslTruststorePassword("password")
        .build())
    
    // Connection settings
    .connectionTimeout(Duration.ofSeconds(15))
    .readTimeout(Duration.ofMinutes(1))
    .retries(5, Duration.ofSeconds(2))
    
    // Caching
    .cache(true, 1000, Duration.ofMinutes(30))
    .subjectNameStrategy("io.confluent.kafka.serializers.subject.TopicNameStrategy")
    
    // Failover
    .failover(true)
    .healthCheckInterval(Duration.ofSeconds(30))
    
    // Datacenter-specific URLs
    .datacenterUrls(Map.of(
        "us-east-1", List.of("http://schema-east-1:8081", "http://schema-east-2:8081"),
        "us-west-2", List.of("http://schema-west-1:8081", "http://schema-west-2:8081")
    ))
    
    // Additional properties
    .additionalProperties(Map.of(
        "schema.registry.request.timeout.ms", "30000",
        "schema.registry.retry.backoff.ms", "1000"
    ))
    
    .build();
```

## Performance Tuning

### High-Throughput Configuration

```java
// Producer for high throughput
ProducerConfig highThroughputProducer = ProducerConfig.builder()
    .batchSize(65536) // 64KB batches
    .bufferMemory(134217728L) // 128MB buffer
    .lingerMs(20) // Wait up to 20ms for batching
    .compressionType("lz4") // Fast compression
    .maxInFlightRequestsPerConnection(5)
    .acks("1") // Leader acknowledgment only
    .retries(0) // No retries for maximum throughput
    .build();

// Consumer for high throughput
ConsumerConfig highThroughputConsumer = ConsumerConfig.builder()
    .maxPollRecords(2000)
    .fetchMinBytes(100000) // 100KB minimum fetch
    .fetchMaxBytes(104857600) // 100MB maximum fetch
    .fetchMaxWaitMs(100) // Low latency
    .enableAutoCommit(true)
    .autoCommitIntervalMs(1000)
    .build();
```

### Low-Latency Configuration

```java
// Producer for low latency
ProducerConfig lowLatencyProducer = ProducerConfig.builder()
    .batchSize(1) // Send immediately
    .lingerMs(0) // No batching delay
    .compressionType("none") // No compression overhead
    .maxInFlightRequestsPerConnection(1)
    .acks("1")
    .requestTimeoutMs(5000) // Quick timeout
    .build();

// Consumer for low latency
ConsumerConfig lowLatencyConsumer = ConsumerConfig.builder()
    .maxPollRecords(1) // Process one record at a time
    .fetchMinBytes(1) // Fetch immediately
    .fetchMaxWaitMs(1) // Very low wait time
    .enableAutoCommit(false) // Manual commit for control
    .sessionTimeoutMs(6000) // Quick session timeout
    .heartbeatIntervalMs(2000)
    .build();
```

## Environment-Specific Configuration

### Development Environment

```java
public static KafkaDatacenterConfiguration developmentConfig() {
    return KafkaDatacenterConfiguration.builder()
        .addDatacenter(KafkaDatacenterEndpoint.builder()
            .id("dev")
            .region("local")
            .bootstrapServers("localhost:9092")
            .build())
        .localDatacenter("dev")
        .producerConfig(ProducerConfig.builder()
            .acks("1") // Faster for development
            .retries(3)
            .lingerMs(5)
            .build())
        .consumerConfig(ConsumerConfig.builder()
            .autoOffsetReset("earliest")
            .enableAutoCommit(true)
            .build())
        .enableHealthChecks(false) // Simplified for dev
        .build();
}
```

### Production Environment

```java
public static KafkaDatacenterConfiguration productionConfig() {
    return KafkaDatacenterConfiguration.builder()
        .addDatacenter(primaryProductionDC())
        .addDatacenter(secondaryProductionDC())
        .localDatacenter("prod-primary")
        .routingStrategy(RoutingStrategy.LATENCY_BASED)
        
        .producerConfig(ProducerConfig.builder()
            .acks("all") // Maximum durability
            .retries(Integer.MAX_VALUE)
            .enableIdempotence(true)
            .compressionType("snappy")
            .batchSize(32768)
            .lingerMs(10)
            .build())
            
        .consumerConfig(ConsumerConfig.builder()
            .enableAutoCommit(false) // Manual control
            .isolationLevel("read_committed")
            .maxPollRecords(500)
            .sessionTimeoutMs(30000)
            .build())
            
        .securityConfig(productionSecurityConfig())
        .resilienceConfig(productionResilienceConfig())
        .schemaRegistryConfig(productionSchemaRegistryConfig())
        
        .enableHealthChecks(true)
        .healthCheckInterval(Duration.ofSeconds(30))
        .enableCrossDCOperations(true)
        
        .build();
}
```

## Configuration Examples

### Complete Enterprise Configuration

```java
public class KafkaConfigurationFactory {
    
    public static KafkaDatacenterConfiguration createEnterpriseConfig() {
        // Define multiple datacenters
        KafkaDatacenterEndpoint primary = KafkaDatacenterEndpoint.builder()
            .id("us-east-1-prod")
            .region("us-east-1")
            .bootstrapServers("kafka-1.east.company.com:9092,kafka-2.east.company.com:9092")
            .datacenterPriority(1)
            .availabilityZone("us-east-1a")
            .tags(Map.of("tier", "primary", "environment", "production"))
            .build();
            
        KafkaDatacenterEndpoint secondary = KafkaDatacenterEndpoint.builder()
            .id("us-west-2-prod")
            .region("us-west-2")
            .bootstrapServers("kafka-1.west.company.com:9092,kafka-2.west.company.com:9092")
            .datacenterPriority(2)
            .availabilityZone("us-west-2a")
            .tags(Map.of("tier", "secondary", "environment", "production"))
            .build();
        
        // Security configuration
        SecurityConfig security = SecurityConfig.builder()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .saslJaasConfig(loadSaslConfig())
            .sslTruststoreLocation("/etc/kafka/ssl/truststore.jks")
            .sslTruststorePassword(loadPassword("truststore"))
            .build();
        
        // Resilience configuration
        ResilienceConfig resilience = ResilienceConfig.builder()
            .circuitBreakerEnabled(true)
            .failureRateThreshold(50.0f)
            .waitDurationInOpenState(Duration.ofMinutes(1))
            .retryEnabled(true)
            .maxRetryAttempts(5)
            .rateLimiterEnabled(true)
            .limitForPeriod(1000)
            .build();
        
        // Schema registry configuration
        SchemaRegistryConfig schemaRegistry = SchemaRegistryConfig.builder()
            .urls("https://schema-registry.company.com:8081")
            .basicAuth("schema-client", loadPassword("schema-registry"))
            .security(security)
            .cache(true, 2000, Duration.ofHours(1))
            .build();
        
        // Producer configuration
        ProducerConfig producer = ProducerConfig.builder()
            .acks("all")
            .retries(Integer.MAX_VALUE)
            .enableIdempotence(true)
            .compressionType("snappy")
            .batchSize(32768)
            .lingerMs(10)
            .maxInFlightRequestsPerConnection(5)
            .requestTimeoutMs(30000)
            .deliveryTimeoutMs(120000)
            .customProperties(Map.of(
                "schema.registry.url", "https://schema-registry.company.com:8081"
            ))
            .build();
        
        // Consumer configuration
        ConsumerConfig consumer = ConsumerConfig.builder()
            .groupId("enterprise-consumer-group")
            .enableAutoCommit(false)
            .autoOffsetReset("latest")
            .isolationLevel("read_committed")
            .maxPollRecords(1000)
            .fetchMinBytes(50000)
            .sessionTimeoutMs(45000)
            .heartbeatIntervalMs(15000)
            .maxPollIntervalMs(600000)
            .customProperties(Map.of(
                "schema.registry.url", "https://schema-registry.company.com:8081"
            ))
            .build();
        
        // Main configuration
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(primary)
            .addDatacenter(secondary)
            .localDatacenter("us-east-1-prod")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .producerConfig(producer)
            .consumerConfig(consumer)
            .securityConfig(security)
            .resilienceConfig(resilience)
            .schemaRegistryConfig(schemaRegistry)
            .enableHealthChecks(true)
            .healthCheckInterval(Duration.ofSeconds(30))
            .enableCrossDCOperations(true)
            .enableMetrics(true)
            .build();
    }
    
    private static String loadSaslConfig() {
        // Load SASL configuration from secure storage
        return "org.apache.kafka.common.security.scram.ScramLoginModule required " +
               "username=\"" + loadUsername() + "\" " +
               "password=\"" + loadPassword("kafka") + "\";";
    }
    
    private static String loadUsername() {
        // Implementation to load username from secure storage
        return System.getenv("KAFKA_USERNAME");
    }
    
    private static String loadPassword(String service) {
        // Implementation to load password from secure storage
        return System.getenv("KAFKA_" + service.toUpperCase() + "_PASSWORD");
    }
}
```

### Configuration Validation

All configuration objects include built-in validation:

```java
try {
    KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
        .localDatacenter("non-existent-dc") // This will fail validation
        .build();
} catch (IllegalArgumentException e) {
    logger.error("Configuration validation failed: {}", e.getMessage());
}
```

### Configuration Best Practices

1. **Use Environment Variables**: Store sensitive information in environment variables
2. **Validate Early**: Use builder validation to catch configuration errors early
3. **Use Defaults**: Start with default configurations and override only what's needed
4. **Test Configurations**: Test configurations in development environments
5. **Document Overrides**: Document any custom configuration values and their purpose
6. **Security First**: Always use proper authentication and encryption in production
7. **Monitor Configuration**: Use health checks to validate configuration effectiveness

For more detailed examples, see the [examples directory](../lib/src/main/java/com/kafka/multidc/example/).
