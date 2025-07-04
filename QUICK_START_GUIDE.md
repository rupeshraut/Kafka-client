# Kafka Multi-Datacenter Client - Quick Start Guide

Welcome to the Kafka Multi-Datacenter Client Library! This guide will get you up and running quickly with enterprise-grade multi-datacenter Kafka operations.

## 🚀 What You'll Build

By the end of this guide, you'll have:
- A working Kafka multi-datacenter client with enterprise-grade features
- Producer and consumer operations across multiple datacenters
- Health monitoring and resilience patterns with automatic failover
- Dead letter queue handling and error recovery
- Schema registry integration for data governance
- Advanced security configuration (SSL/TLS, SASL)
- Comprehensive observability and metrics
- Spring Boot integration with Actuator health checks (optional)
- Transaction support for exactly-once semantics
- Performance optimization for high-throughput scenarios

## 📋 Prerequisites

- **Java 17+** (OpenJDK or Oracle JDK)
- **Gradle 8.5+** or **Maven 3.8+**
- **Apache Kafka 3.0+** (for production)
- **Docker** (optional, for local testing)

## 🏁 Step 1: Add Dependencies

### Gradle
```gradle
dependencies {
    implementation 'com.kafka.multidc:kafka-multidc-client:1.0.0'
    implementation 'org.apache.kafka:kafka-clients:3.7.0'
    implementation 'io.projectreactor:reactor-core:3.6.0'
    implementation 'io.github.resilience4j:resilience4j-all:2.1.0'
    implementation 'io.micrometer:micrometer-core:1.12.0'
    
    // Optional: Spring Boot integration
    implementation 'org.springframework.boot:spring-boot-starter-actuator:3.2.0'
}
```

### Maven
```xml
<dependencies>
    <dependency>
        <groupId>com.kafka.multidc</groupId>
        <artifactId>kafka-multidc-client</artifactId>
        <version>1.0.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.7.0</version>
    </dependency>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-core</artifactId>
        <version>3.6.0</version>
    </dependency>
    <dependency>
        <groupId>io.github.resilience4j</groupId>
        <artifactId>resilience4j-all</artifactId>
        <version>2.1.0</version>
    </dependency>
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-core</artifactId>
        <version>1.12.0</version>
    </dependency>
</dependencies>
```

## 🏗️ Step 2: Basic Configuration

Create your first multi-datacenter client:

```java
import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.routing.RoutingStrategy;

public class QuickStartExample {
    
    public static void main(String[] args) {
        // Step 1: Configure your datacenters
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                // Primary datacenter
                KafkaDatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .bootstrapServers("localhost:9092")  // Replace with your Kafka brokers
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .addDatacenter(
                // Secondary datacenter
                KafkaDatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .bootstrapServers("kafka-west.example.com:9092")  // Replace with your Kafka brokers
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .localDatacenter("us-east-1")  // Your local datacenter
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)  // Intelligent routing
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(10))
            .enableMetrics(true)
            .build();

        // Step 2: Create the client
        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            // Step 3: Start using the client!
            runBasicExample(client);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
```

## 📤 Step 3: Producer Operations

### Synchronous Producer (Simple & Reliable)
```java
private static void runProducerExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Producer Example ===");
    
    // Send a simple message
    try {
        RecordMetadata metadata = client.producerSync().send("user-events", "user123", "login");
        System.out.println("Message sent! Topic: " + metadata.topic() + 
                          ", Partition: " + metadata.partition() + 
                          ", Offset: " + metadata.offset());
    } catch (Exception e) {
        System.err.println("Failed to send message: " + e.getMessage());
    }
    
    // Send with headers
    Map<String, String> headers = Map.of(
        "source", "mobile-app",
        "version", "1.0"
    );
    
    client.producerSync().send("user-events", "user456", "purchase", headers);
    System.out.println("Message with headers sent!");
}
```

### Asynchronous Producer (High Performance)
```java
private static void runAsyncProducerExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Async Producer Example ===");
    
    // Send multiple messages asynchronously
    List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    
    for (int i = 0; i < 10; i++) {
        String key = "user" + i;
        String value = "event-data-" + i;
        
        CompletableFuture<RecordMetadata> future = client.producerAsync().send("events", key, value);
        
        future.thenAccept(metadata -> 
            System.out.println("Async message sent: " + key + " -> partition " + metadata.partition())
        ).exceptionally(throwable -> {
            System.err.println("Failed to send " + key + ": " + throwable.getMessage());
            return null;
        });
        
        futures.add(future);
    }
    
    // Wait for all messages to be sent
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenRun(() -> System.out.println("All async messages sent!"))
        .join();
}
```

### Reactive Producer (Backpressure Handling)
```java
private static void runReactiveProducerExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Reactive Producer Example ===");
    
    // Create a stream of messages
    Flux<String> messageStream = Flux.range(1, 100)
        .map(i -> "reactive-message-" + i);
    
    // Send with backpressure control
    messageStream
        .flatMap(message -> 
            client.producerReactive().send("reactive-topic", message, message), 
            10  // Control concurrency
        )
        .doOnNext(metadata -> System.out.println("Reactive message sent to partition: " + metadata.partition()))
        .doOnError(error -> System.err.println("Reactive send error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("All reactive messages sent!"))
        .blockLast(); // Wait for completion in this example
}
```

## 📥 Step 4: Consumer Operations

### Synchronous Consumer (Simple Processing)
```java
private static void runConsumerExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Consumer Example ===");
    
    // Subscribe to topics
    client.consumerSync().subscribe(List.of("user-events", "events"));
    
    // Poll for messages
    for (int i = 0; i < 5; i++) {  // Poll 5 times for demo
        ConsumerRecords<String, String> records = client.consumerSync().poll(Duration.ofSeconds(2));
        
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("Consumed: key=%s, value=%s, topic=%s, partition=%d, offset=%d%n",
                record.key(), record.value(), record.topic(), record.partition(), record.offset());
            
            // Process the message
            processMessage(record);
        }
        
        // Commit offsets
        client.consumerSync().commitSync();
        System.out.println("Offsets committed");
    }
}

private static void processMessage(ConsumerRecord<String, String> record) {
    // Your business logic here
    System.out.println("Processing message: " + record.value());
    
    // Simulate processing time
    try {
        Thread.sleep(100);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
}
```

### Reactive Consumer (Stream Processing)
```java
private static void runReactiveConsumerExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Reactive Consumer Example ===");
    
    // Subscribe and process records as a stream
    Disposable subscription = client.consumerReactive()
        .subscribe("user-events")
        .take(10)  // Take only 10 messages for demo
        .doOnNext(record -> {
            System.out.println("Reactive consumed: " + record.key() + " -> " + record.value());
            // Your processing logic here
        })
        .doOnError(error -> System.err.println("Consumer error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("Reactive consumer completed"))
        .subscribe();
    
    // Wait a bit for messages
    try {
        Thread.sleep(5000);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    
    subscription.dispose();
}
```

## ⚡ Step 5: Add Resilience & Error Handling

Enable enterprise-grade resilience patterns:

```java
import com.kafka.multidc.config.ResilienceConfig;
import com.kafka.multidc.deadletter.DefaultDeadLetterConfig;
import com.kafka.multidc.deadletter.DeadLetterQueueHandler;

// Enhanced configuration with resilience
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenter1)  // Your datacenters from Step 2
    .addDatacenter(datacenter2)
    .localDatacenter("us-east-1")
    .routingStrategy(RoutingStrategy.HEALTH_AWARE)
    
    // Add resilience patterns
    .resilienceConfig(ResilienceConfig.builder()
        .circuitBreakerFailureRateThreshold(50.0f)
        .circuitBreakerWaitDurationInOpenState(Duration.ofSeconds(30))
        .retryMaxAttempts(3)
        .retryWaitDuration(Duration.ofMillis(500))
        .enableBasicPatterns()
        .build())
    
    // Add dead letter queue handling
    .deadLetterConfig(DefaultDeadLetterConfig.builder()
        .deadLetterTopicSuffix(".dlq")
        .maxRetryAttempts(3)
        .enabled(true)
        .strategy(DeadLetterQueueHandler.DeadLetterStrategy.TOPIC_BASED)
        .build())
    
    .enableMetrics(true)
    .build();

// Usage remains the same - resilience is automatic!
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
    // Your operations will now have automatic:
    // - Circuit breaker protection
    // - Retry with exponential backoff
    // - Dead letter queue handling
    // - Health-aware routing
    
    runBasicExample(client);
}
```

## 🏥 Step 6: Health Monitoring

### Standalone Health Checks
```java
import com.kafka.multidc.actuator.KafkaMultiDatacenterHealthIndicator;

private static void checkHealth(KafkaMultiDatacenterClient client) {
    System.out.println("=== Health Check Example ===");
    
    // Create health indicator
    KafkaMultiDatacenterHealthIndicator healthIndicator = 
        new KafkaMultiDatacenterHealthIndicator(client);
    
    // Check overall health
    var health = healthIndicator.health();
    
    System.out.println("Overall Health: " + (health.isHealthy() ? "HEALTHY" : "UNHEALTHY"));
    System.out.println("Status: " + health.getStatus());
    
    // Get detailed health information
    Map<String, Object> details = health.getDetails();
    details.forEach((key, value) -> System.out.println("  " + key + ": " + value));
    
    // Check individual datacenter health
    Map<String, Boolean> datacenterHealth = client.getConnectionPoolHealth();
    datacenterHealth.forEach((dcId, healthy) -> 
        System.out.println("Datacenter " + dcId + ": " + (healthy ? "HEALTHY" : "UNHEALTHY"))
    );
}
```

### Spring Boot Integration (Optional)
If you're using Spring Boot, add this configuration:

```java
@Configuration
@ConditionalOnClass(HealthIndicator.class)
public class KafkaHealthConfiguration {
    
    @Bean
    @ConditionalOnBean(KafkaMultiDatacenterClient.class)
    public HealthIndicator kafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client) {
        return new KafkaMultiDatacenterSpringHealthIndicator(client);
    }
}
```

**application.yml:**
```yaml
management:
  health:
    kafka-multidc:
      enabled: true
      timeout: 5s
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: always
```

Access health endpoint: `GET http://localhost:8080/actuator/health/kafka-multidc`

## � Step 8: Enterprise Security Configuration

Configure SSL/TLS and SASL authentication for production deployments:

```java
import com.kafka.multidc.config.SecurityConfig;
import com.kafka.multidc.security.AuthenticationManager;

// Enhanced configuration with enterprise security
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(
        KafkaDatacenterEndpoint.builder()
            .id("us-east-1")
            .region("us-east")
            .bootstrapServers("secure-kafka-east.example.com:9093")
            .priority(1)
            
            // Add SSL/TLS security
            .securityConfig(SecurityConfig.builder()
                .sslProtocol("TLSv1.3")
                .sslKeystoreLocation("/path/to/client.keystore.jks")
                .sslKeystorePassword("keystore-password")
                .sslTruststoreLocation("/path/to/client.truststore.jks")
                .sslTruststorePassword("truststore-password")
                
                // Add SASL authentication
                .saslMechanism("SCRAM-SHA-512")
                .saslUsername("kafka-client")
                .saslPassword("secure-password")
                
                .enableHostnameVerification(true)
                .build())
            .build())
        
    .addDatacenter(
        KafkaDatacenterEndpoint.builder()
            .id("us-west-1")
            .region("us-west")
            .bootstrapServers("secure-kafka-west.example.com:9093")
            .priority(2)
            
            .securityConfig(SecurityConfig.builder()
                .sslProtocol("TLSv1.3")
                .sslKeystoreLocation("/path/to/client-west.keystore.jks")
                .sslKeystorePassword("keystore-password")
                .sslTruststoreLocation("/path/to/client-west.truststore.jks")
                .sslTruststorePassword("truststore-password")
                .saslMechanism("SCRAM-SHA-512")
                .saslUsername("kafka-client-west")
                .saslPassword("secure-password-west")
                .enableHostnameVerification(true)
                .build())
            .build())
    // ... rest of configuration
    .build();

// Security is automatically applied to all operations
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
    // All operations now use secure connections
    client.producerSync().send("secure-topic", "key", "secure-message");
    System.out.println("Secure message sent successfully!");
}
```

## 📋 Step 9: Schema Registry Integration

Enable schema registry for data governance and evolution:

```java
import com.kafka.multidc.config.SchemaRegistryConfig;
import com.kafka.multidc.schema.SchemaRegistryClient;

// Configure schema registry for multiple datacenters
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenter1)  // Your secure datacenters from Step 8
    .addDatacenter(datacenter2)
    
    // Add schema registry configuration
    .schemaRegistryConfig(SchemaRegistryConfig.builder()
        .endpoints(List.of(
            "https://schema-registry-east.example.com:8081",
            "https://schema-registry-west.example.com:8081"
        ))
        .username("schema-user")
        .password("schema-password")
        .enableSsl(true)
        .maxCacheSize(1000)
        .cacheExpireAfterWriteMinutes(60)
        .healthCheckInterval(Duration.ofSeconds(30))
        .build())
    
    .build();

// Usage with schema validation
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
    
    // Get schema registry client
    SchemaRegistryClient schemaClient = client.getSchemaRegistryClient();
    
    // Check schema health
    boolean healthy = schemaClient.isHealthy();
    System.out.println("Schema Registry Health: " + (healthy ? "HEALTHY" : "UNHEALTHY"));
    
    // Your messages will now be automatically validated against registered schemas
    client.producerSync().send("user-events", "user123", createUserEvent());
}

private static Object createUserEvent() {
    // Return your schema-compliant object (Avro, JSON Schema, etc.)
    return Map.of(
        "userId", "user123",
        "action", "login",
        "timestamp", System.currentTimeMillis()
    );
}
```

## 💳 Step 10: Transactional Operations

Implement exactly-once semantics with transactions:

```java
import com.kafka.multidc.operations.transaction.KafkaTransactionOperations;

private static void transactionalExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Transactional Operations Example ===");
    
    // Get transaction operations
    KafkaTransactionOperations transactionOps = client.getTransactionOperations();
    
    // Execute synchronous transaction
    try {
        transactionOps.executeInTransaction(() -> {
            // All operations within this block are part of the same transaction
            client.producerSync().send("accounts", "account123", "debit:100");
            client.producerSync().send("accounts", "account456", "credit:100");
            client.producerSync().send("audit", "transaction123", "transfer:account123->account456:100");
            
            System.out.println("Transaction committed successfully!");
            return null;
        });
    } catch (Exception e) {
        System.err.println("Transaction failed and was rolled back: " + e.getMessage());
    }
    
    // Asynchronous transaction
    CompletableFuture<Void> transactionFuture = transactionOps.executeInTransactionAsync(() -> {
        // Async operations within transaction
        return client.producerAsync().send("orders", "order789", "created")
            .thenCompose(metadata -> 
                client.producerAsync().send("inventory", "item456", "reserved:1"))
            .thenCompose(metadata -> 
                client.producerAsync().send("notifications", "user123", "order-confirmed"))
            .thenApply(metadata -> null);
    });
    
    transactionFuture.thenRun(() -> 
        System.out.println("Async transaction completed successfully!")
    ).exceptionally(throwable -> {
        System.err.println("Async transaction failed: " + throwable.getMessage());
        return null;
    });
}
```

## �📊 Step 7: Monitoring & Metrics

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

private static void monitoringExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== Monitoring Example ===");
    
    // Get metrics registry (or inject from Spring)
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    
    // Subscribe to health changes
    client.subscribeToHealthChanges((datacenter, healthy) -> {
        System.out.println("Health change: " + datacenter.getId() + " is now " + 
                          (healthy ? "HEALTHY" : "UNHEALTHY"));
        
        // You can trigger alerts here
        if (!healthy) {
            System.out.println("🚨 ALERT: Datacenter " + datacenter.getId() + " is down!");
        }
    });
    
    // Check connection pool metrics
    client.getConnectionPoolMetrics().forEach((dcId, metrics) -> {
        System.out.println("Datacenter " + dcId + " pool metrics:");
        System.out.println("  Active connections: " + metrics.getActiveConnections());
        System.out.println("  Total connections: " + metrics.getTotalConnections());
        System.out.println("  Pool utilization: " + metrics.getUtilizationPercentage() + "%");
    });
}
```

## 🎯 Complete Enterprise Example

Here's a complete example with all enterprise features enabled:

```java
import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import com.kafka.multidc.deadletter.DefaultDeadLetterConfig;
import com.kafka.multidc.deadletter.DeadLetterQueueHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class KafkaEnterpriseQuickStart {
    
    public static void main(String[] args) {
        // Complete enterprise configuration
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .bootstrapServers("secure-kafka-east.example.com:9093")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    
                    // Enterprise security
                    .securityConfig(SecurityConfig.builder()
                        .sslProtocol("TLSv1.3")
                        .sslKeystoreLocation("/path/to/client.keystore.jks")
                        .sslKeystorePassword("keystore-password")
                        .sslTruststoreLocation("/path/to/client.truststore.jks")
                        .sslTruststorePassword("truststore-password")
                        .saslMechanism("SCRAM-SHA-512")
                        .saslUsername("kafka-client")
                        .saslPassword("secure-password")
                        .enableHostnameVerification(true)
                        .build())
                    .build())
                
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .bootstrapServers("secure-kafka-west.example.com:9093")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    
                    .securityConfig(SecurityConfig.builder()
                        .sslProtocol("TLSv1.3")
                        .sslKeystoreLocation("/path/to/client-west.keystore.jks")
                        .sslKeystorePassword("keystore-password")
                        .sslTruststoreLocation("/path/to/client-west.truststore.jks")
                        .sslTruststorePassword("truststore-password")
                        .saslMechanism("SCRAM-SHA-512")
                        .saslUsername("kafka-client-west")
                        .saslPassword("secure-password-west")
                        .enableHostnameVerification(true)
                        .build())
                    .build())
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(10))
            
            // Enterprise resilience
            .resilienceConfig(ResilienceConfig.builder()
                .circuitBreakerFailureRateThreshold(50.0f)
                .circuitBreakerWaitDurationInOpenState(Duration.ofSeconds(30))
                .retryMaxAttempts(3)
                .retryWaitDuration(Duration.ofMillis(500))
                .enableBasicPatterns()
                .build())
            
            // Dead letter queue
            .deadLetterConfig(DefaultDeadLetterConfig.builder()
                .deadLetterTopicSuffix(".dlq")
                .maxRetryAttempts(3)
                .enabled(true)
                .strategy(DeadLetterQueueHandler.DeadLetterStrategy.TOPIC_BASED)
                .build())
            
            // Schema registry
            .schemaRegistryConfig(SchemaRegistryConfig.builder()
                .endpoints(List.of(
                    "https://schema-registry-east.example.com:8081",
                    "https://schema-registry-west.example.com:8081"
                ))
                .username("schema-user")
                .password("schema-password")
                .enableSsl(true)
                .maxCacheSize(1000)
                .cacheExpireAfterWriteMinutes(60)
                .healthCheckInterval(Duration.ofSeconds(30))
                .build())
            
            .enableMetrics(true)
            .build();

        // Run the complete enterprise example
        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            System.out.println("🚀 Enterprise Kafka Multi-Datacenter Client Started!");
            
            // Check overall health
            checkEnterpriseHealth(client);
            
            // Run secure producer operations
            runSecureProducerExample(client);
            
            // Run transactional operations
            runTransactionalExample(client);
            
            // Run schema-validated operations
            runSchemaValidatedExample(client);
            
            // Monitor comprehensive metrics
            monitorEnterpriseMetrics(client);
            
            System.out.println("✅ Enterprise quick start completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void checkEnterpriseHealth(KafkaMultiDatacenterClient client) {
        System.out.println("=== Enterprise Health Check ===");
        
        // Check schema registry health
        boolean schemaHealthy = client.getSchemaRegistryClient().isHealthy();
        System.out.println("Schema Registry: " + (schemaHealthy ? "✅ HEALTHY" : "❌ UNHEALTHY"));
        
        // Check datacenter health
        Map<String, Boolean> dcHealth = client.getConnectionPoolHealth();
        dcHealth.forEach((dcId, healthy) -> 
            System.out.println("Datacenter " + dcId + ": " + (healthy ? "✅ HEALTHY" : "❌ UNHEALTHY"))
        );
        
        // Check connection pool metrics
        client.getConnectionPoolMetrics().forEach((dcId, metrics) -> {
            System.out.println("Pool " + dcId + " - Active: " + metrics.getActiveConnections() + 
                              ", Total: " + metrics.getTotalConnections() + 
                              ", Utilization: " + metrics.getUtilizationPercentage() + "%");
        });
    }
    
    private static void runSecureProducerExample(KafkaMultiDatacenterClient client) {
        System.out.println("=== Secure Producer Example ===");
        
        try {
            // Send secure messages with headers
            Map<String, String> headers = Map.of(
                "source", "enterprise-app",
                "version", "2.0",
                "security-level", "high"
            );
            
            var metadata = client.producerSync().send("secure-events", "enterprise-key", "secure-payload", headers);
            System.out.println("✅ Secure message sent to partition: " + metadata.partition());
            
        } catch (Exception e) {
            System.err.println("❌ Secure send failed: " + e.getMessage());
        }
    }
    
    private static void runTransactionalExample(KafkaMultiDatacenterClient client) {
        System.out.println("=== Transactional Example ===");
        
        try {
            client.getTransactionOperations().executeInTransaction(() -> {
                client.producerSync().send("orders", "order123", "created");
                client.producerSync().send("inventory", "item456", "reserved");
                client.producerSync().send("billing", "invoice789", "generated");
                
                System.out.println("✅ Transaction completed successfully!");
                return null;
            });
        } catch (Exception e) {
            System.err.println("❌ Transaction failed: " + e.getMessage());
        }
    }
    
    private static void runSchemaValidatedExample(KafkaMultiDatacenterClient client) {
        System.out.println("=== Schema Validated Example ===");
        
        // Create schema-compliant message
        Map<String, Object> userEvent = Map.of(
            "userId", "user123",
            "action", "enterprise-login",
            "timestamp", System.currentTimeMillis(),
            "metadata", Map.of("source", "enterprise-portal", "version", "2.0")
        );
        
        try {
            var metadata = client.producerSync().send("user-events", "user123", userEvent);
            System.out.println("✅ Schema-validated message sent to partition: " + metadata.partition());
        } catch (Exception e) {
            System.err.println("❌ Schema validation failed: " + e.getMessage());
        }
    }
    
    private static void monitorEnterpriseMetrics(KafkaMultiDatacenterClient client) {
        System.out.println("=== Enterprise Metrics ===");
        
        // Monitor health changes
        client.subscribeToHealthChanges((datacenter, healthy) -> {
            System.out.println("🔔 Health Alert: " + datacenter.getId() + " is now " + 
                              (healthy ? "HEALTHY" : "UNHEALTHY"));
        });
        
        // Log comprehensive metrics
        System.out.println("📊 Metrics collection enabled - check your monitoring dashboard");
        System.out.println("🔍 Distributed tracing enabled - check your trace collector");
        System.out.println("🏥 Health endpoints available for monitoring systems");
    }
}
```

## 🛠️ Step 11: Advanced Performance Tuning

Optimize for high-throughput production workloads:

```java
import com.kafka.multidc.performance.PerformanceOptimizationManager;

// Performance-optimized configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenter1)  // Your enterprise datacenters
    .addDatacenter(datacenter2)
    
    // Performance optimizations
    .producerConfig(ProducerConfig.builder()
        .batchSize(32768)  // 32KB batches
        .lingerMs(10)      // 10ms linger for batching
        .compressionType("lz4")
        .enableIdempotence(true)
        .acks("all")       // Strong consistency
        .maxInFlightRequestsPerConnection(5)
        .bufferMemory(67108864)  // 64MB buffer
        .build())
    
    .consumerConfig(ConsumerConfig.builder()
        .maxPollRecords(1000)    // Process 1000 records per poll
        .fetchMinBytes(50000)     // 50KB min fetch
        .fetchMaxWaitMs(500)      // 500ms max wait
        .enableAutoCommit(false)  // Manual commit for reliability
        .autoOffsetReset("earliest")
        .build())
    
    // Connection pool optimization
    .connectionPoolSize(10)  // 10 connections per datacenter
    .enableConnectionWarmup(true)
    .connectionWarmupTimeout(Duration.ofSeconds(30))
    
    .build();

// Monitor performance
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
    
    // Get performance manager
    PerformanceOptimizationManager perfManager = client.getPerformanceOptimizationManager();
    
    // Enable performance monitoring
    perfManager.enablePerformanceMetrics();
    
    // Get performance recommendations
    var recommendations = perfManager.getPerformanceRecommendations();
    recommendations.forEach(rec -> 
        System.out.println("💡 Performance Tip: " + rec.getDescription()));
    
    // Your high-performance operations here
    runHighThroughputExample(client);
}

private static void runHighThroughputExample(KafkaMultiDatacenterClient client) {
    System.out.println("=== High Throughput Example ===");
    
    // Batch produce for maximum throughput
    List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    
    for (int i = 0; i < 10000; i++) {
        String key = "high-volume-" + i;
        String value = "payload-" + i + "-" + System.currentTimeMillis();
        
        CompletableFuture<RecordMetadata> future = client.producerAsync().send("high-volume", key, value);
        futures.add(future);
        
        // Batch every 100 requests
        if (i % 100 == 0) {
            System.out.println("Queued " + (i + 1) + " messages...");
        }
    }
    
    // Wait for all to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenRun(() -> System.out.println("✅ All 10,000 messages sent successfully!"))
        .join();
}
```
```

## 🎉 What's Next?

Congratulations! You now have a fully functional enterprise-grade Kafka multi-datacenter client with advanced features. Here's what you can explore next:

### 🏢 Enterprise Production Deployment

#### Advanced Configuration
- **Consumer Group Management**: [Enterprise Features](ENTERPRISE_FEATURES_SUMMARY.md#consumer-group-management)
- **Advanced Serialization**: Avro, Protobuf, custom formats with compression and encryption
- **Performance Optimization**: High-throughput tuning and connection pool optimization
- **Certificate Management**: Advanced SSL/TLS certificate rotation and management

#### Production Monitoring
- **Spring Boot Actuator Integration**: [Health Indicators](HEALTH_INDICATOR.md)
- **Comprehensive Metrics**: [Observability Features](ENTERPRISE_FEATURES_SUMMARY.md#observability)
- **Distributed Tracing**: Integration with Jaeger, Zipkin, or cloud tracing services
- **Alerting & Notifications**: Custom health change listeners and alert configurations

#### Resilience & Reliability
- **Advanced Resilience Patterns**: [Resilience Guide](RESILIENCE.md)
- **Dead Letter Queue Management**: [DLQ Implementation](DLQ_IMPLEMENTATION_SUMMARY.md)
- **Disaster Recovery**: Multi-datacenter failover and data replication strategies
- **Chaos Engineering**: Test your resilience patterns with controlled failures

### 🚀 Advanced Use Cases

#### High-Performance Scenarios
- **Reactive Streaming**: Project Reactor integration for backpressure handling
- **Batch Processing**: Optimized batch operations for high-throughput scenarios
- **Connection Pooling**: Advanced pool management and monitoring
- **Compression Strategies**: LZ4, Snappy, ZSTD optimization for different data types

#### Enterprise Integration
- **Spring Boot Integration**: Auto-configuration and dependency injection
- **Microservices Architecture**: Service mesh integration and distributed patterns
- **Cloud-Native Deployment**: Kubernetes operators and cloud provider integration
- **Schema Evolution**: Advanced schema registry patterns and compatibility strategies

#### Security & Compliance
- **Advanced Authentication**: Kerberos, OAuth2, and custom authentication providers
- **Encryption**: Field-level encryption and key management integration
- **Audit Logging**: Comprehensive audit trails for compliance requirements
- **Network Security**: VPN, private networking, and security group configurations

### 🔧 Development & Testing

#### Testing Strategies
- **Integration Testing**: Advanced TestContainers patterns and test environments
- **Performance Testing**: Load testing and benchmark frameworks
- **Chaos Testing**: Resilience testing with datacenter failures
- **Schema Testing**: Schema evolution and compatibility testing

#### Development Productivity
- **IDE Integration**: Advanced debugging and profiling tools
- **Local Development**: Docker Compose setups for local multi-datacenter testing
- **CI/CD Integration**: Automated testing and deployment pipelines
- **Documentation**: API documentation generation and maintenance

### 📚 Learning Resources

#### Architecture Patterns
- **Event Sourcing**: Implementing event sourcing with multi-datacenter coordination
- **CQRS**: Command Query Responsibility Segregation patterns
- **Saga Pattern**: Distributed transaction management across datacenters
- **Event-Driven Architecture**: Advanced event-driven microservices patterns

#### Performance Optimization
- **Benchmarking**: Performance testing methodologies and tools
- **Profiling**: Memory and CPU profiling for high-performance applications
- **Tuning**: JVM tuning and garbage collection optimization
- **Scaling**: Horizontal and vertical scaling strategies

### 🌐 Community & Support

#### Getting Help
- 📖 **Full Documentation**: [README.md](README.md) - Comprehensive feature documentation
- 🏥 **Health Monitoring**: [Health Indicator Guide](HEALTH_INDICATOR.md) - Spring Boot integration
- 🔄 **Resilience Patterns**: [Resilience Guide](RESILIENCE.md) - Fault tolerance implementation
- 📬 **Dead Letter Queues**: [DLQ Guide](DLQ_IMPLEMENTATION_SUMMARY.md) - Error handling strategies
- ⭐ **Enterprise Features**: [Feature Summary](ENTERPRISE_FEATURES_SUMMARY.md) - All enterprise capabilities

#### Community Resources
- 🐛 **Issue Tracking**: Report bugs and request features on GitHub
- 💬 **Discussions**: Join community discussions and share best practices
- 📝 **Contributing**: Contribute to the project with improvements and extensions
- 🎓 **Examples**: Explore additional examples and use case implementations

### � Future Enhancements

The library is actively maintained and continuously enhanced with:

- **AI-Powered Optimization**: Machine learning for automatic performance tuning
- **Advanced Analytics**: Real-time analytics and insights dashboard
- **Dynamic Configuration**: Runtime configuration updates without restarts
- **Cloud Integration**: Enhanced cloud provider integrations (AWS MSK, Azure Event Hubs, GCP Pub/Sub)
- **Kafka Streams Integration**: Advanced stream processing capabilities
- **ksqlDB Support**: SQL-based stream processing integration
- **Change Data Capture**: CDC patterns for database integration

## 🆘 Troubleshooting

### Common Issues

**Connection Issues:**
```bash
# Check if Kafka is running
telnet localhost 9092

# Verify broker configuration
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Health Check Failures:**
- Verify datacenter endpoints are reachable
- Check network connectivity between datacenters
- Ensure Kafka brokers are running and accessible

**Performance Issues:**
- Enable detailed metrics: `.enableDetailedMetrics(true)`
- Monitor connection pool utilization
- Adjust batch size and linger settings for producers

### Getting Help

- 📖 Check the [Full Documentation](README.md)
- 🐛 Report issues on GitHub
- 💬 Join our community discussions

---

## 📋 Quick Start Summary

You've successfully implemented an enterprise-grade Kafka multi-datacenter client! Here's what you accomplished:

### ✅ **Core Features Implemented**
- ✅ Multi-datacenter configuration with intelligent routing
- ✅ Producer operations (sync, async, reactive) with high performance
- ✅ Consumer operations with reliable message processing
- ✅ Health monitoring and automatic failover
- ✅ Resilience patterns (circuit breaker, retry, rate limiting)
- ✅ Dead letter queue handling for error recovery

### ✅ **Enterprise Features Enabled**
- ✅ SSL/TLS encryption with SASL authentication
- ✅ Schema registry integration for data governance
- ✅ Transactional support for exactly-once semantics
- ✅ Advanced observability and metrics collection
- ✅ Performance optimization for high-throughput scenarios
- ✅ Spring Boot Actuator integration (optional)

### ✅ **Production-Ready Capabilities**
- ✅ Connection pooling with monitoring and warmup
- ✅ Comprehensive error handling and recovery
- ✅ Security best practices implementation
- ✅ Advanced serialization with compression
- ✅ Distributed tracing integration points
- ✅ Enterprise-grade health checks

### 🚀 **Next Steps**
1. **Deploy to Production**: Use the enterprise configuration as a template
2. **Monitor Performance**: Set up dashboards with the provided metrics
3. **Implement Security**: Configure SSL certificates and authentication
4. **Test Resilience**: Verify failover and recovery mechanisms
5. **Scale Operations**: Optimize for your specific throughput requirements

**🎯 You're now ready to build enterprise-grade multi-datacenter Kafka applications with confidence!**

---

**🎯 You're now ready to build enterprise-grade multi-datacenter Kafka applications!**
