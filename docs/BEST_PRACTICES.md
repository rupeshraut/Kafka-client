# Best Practices Guide

This guide provides comprehensive best practices for using the Kafka Multi-Datacenter Client Library in production environments. Following these guidelines will help you achieve optimal performance, reliability, and maintainability.

## 📋 Table of Contents

- [Configuration Best Practices](#configuration-best-practices)
- [Producer Best Practices](#producer-best-practices)
- [Consumer Best Practices](#consumer-best-practices)
- [Multi-Datacenter Architecture](#multi-datacenter-architecture)
- [Performance Optimization](#performance-optimization)
- [Resilience and Fault Tolerance](#resilience-and-fault-tolerance)
- [Security Best Practices](#security-best-practices)
- [Monitoring and Observability](#monitoring-and-observability)
- [Schema Management](#schema-management)
- [Testing Strategies](#testing-strategies)
- [Deployment and Operations](#deployment-and-operations)
- [Troubleshooting](#troubleshooting)

## 🔧 Configuration Best Practices

### Environment-Specific Configuration

Always use environment-specific configuration with proper externalization:

```java
// ✅ Good: Environment-specific configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(KafkaDatacenterEndpoint.builder()
        .id("prod-us-east-1")
        .bootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS_US_EAST"))
        .schemaRegistryUrl(System.getenv("SCHEMA_REGISTRY_URL_US_EAST"))
        .build())
    .build();

// ❌ Bad: Hardcoded values
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(KafkaDatacenterEndpoint.builder()
        .id("us-east-1")
        .bootstrapServers("localhost:9092")  // Don't hardcode
        .build())
    .build();
```

### Configuration Validation

Always validate configuration parameters before client creation:

```java
// ✅ Good: Comprehensive validation
public KafkaMultiDatacenterClient createClient(Properties props) {
    validateRequiredProperties(props);
    
    KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
        .validate(true)  // Enable validation
        .strictMode(true)  // Fail fast on invalid config
        .fromProperties(props)
        .build();
    
    return KafkaMultiDatacenterClientBuilder.create()
        .configuration(config)
        .validate()  // Validate before building
        .build();
}

private void validateRequiredProperties(Properties props) {
    String[] required = {"bootstrap.servers", "schema.registry.url"};
    for (String prop : required) {
        if (!props.containsKey(prop)) {
            throw new IllegalArgumentException("Missing required property: " + prop);
        }
    }
}
```

### Configuration Management

Use centralized configuration management with proper versioning:

```java
// ✅ Good: Centralized configuration factory
@Component
public class KafkaConfigurationFactory {
    
    @Value("${kafka.environment}")
    private String environment;
    
    @ConfigurationProperties(prefix = "kafka.datacenters")
    private Map<String, DatacenterProperties> datacenters;
    
    public KafkaDatacenterConfiguration createConfiguration() {
        KafkaDatacenterConfiguration.Builder builder = 
            KafkaDatacenterConfiguration.builder()
                .environment(environment);
        
        datacenters.forEach((id, props) -> 
            builder.addDatacenter(createDatacenterEndpoint(id, props)));
        
        return builder.build();
    }
}
```

## 📤 Producer Best Practices

### Batch Configuration

Optimize batching for your throughput requirements:

```java
// ✅ Good: Optimized batch configuration
ProducerConfig producerConfig = ProducerConfig.builder()
    .batchSize(32 * 1024)  // 32KB batches for high throughput
    .lingerMs(10)          // Small linger time for balance
    .bufferMemory(128 * 1024 * 1024)  // 128MB buffer
    .compressionType("lz4")  // Fast compression
    .build();

// For low-latency scenarios
ProducerConfig lowLatencyConfig = ProducerConfig.builder()
    .batchSize(1024)       // Smaller batches
    .lingerMs(0)           // No lingering
    .acks("1")             // Faster acknowledgment
    .build();
```

### Partitioning Strategy Selection

Choose the right partitioning strategy for your use case:

```java
// ✅ Good: Context-appropriate partitioning
ProducerPartitioningManager partitioningManager;

if (isMultiDatacenterDeployment()) {
    // Geographic partitioning for multi-DC efficiency
    partitioningManager = ProducerPartitioningManager.builder()
        .strategy(ProducerPartitioningType.GEOGRAPHIC)
        .datacenterAware(true)
        .fallbackStrategy(ProducerPartitioningType.LOAD_BALANCED)
        .build();
} else if (isHighThroughputWorkload()) {
    // Sticky partitioning for throughput optimization
    partitioningManager = ProducerPartitioningManager.builder()
        .strategy(ProducerPartitioningType.STICKY)
        .build();
} else {
    // Default key-hash for most cases
    partitioningManager = ProducerPartitioningManager.builder()
        .strategy(ProducerPartitioningType.KEY_HASH)
        .build();
}
```

### Async vs Sync Operations

Use async operations for high-throughput scenarios:

```java
// ✅ Good: Async with proper error handling
public CompletableFuture<Void> sendMessagesAsync(List<ProducerRecord<String, String>> records) {
    List<CompletableFuture<RecordMetadata>> futures = records.stream()
        .map(record -> client.producerAsync().sendAsync(record)
            .exceptionally(throwable -> {
                logger.error("Failed to send record: {}", record.key(), throwable);
                return null;  // Handle individual failures
            }))
        .collect(Collectors.toList());
    
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
}

// ✅ Good: Sync for critical operations
public void sendCriticalMessage(ProducerRecord<String, String> record) {
    try {
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("Message sent successfully: topic={}, partition={}, offset={}", 
            metadata.topic(), metadata.partition(), metadata.offset());
    } catch (Exception e) {
        logger.error("Critical message send failed", e);
        throw new RuntimeException("Failed to send critical message", e);
    }
}
```

### Producer Resource Management

Always properly close producers and manage resources:

```java
// ✅ Good: Proper resource management
@Component
@PreDestroy
public class ProducerService {
    private final KafkaMultiDatacenterClient client;
    
    public ProducerService(KafkaMultiDatacenterClient client) {
        this.client = client;
    }
    
    @PreDestroy
    public void cleanup() {
        try {
            // Flush pending messages
            client.producerSync().flush();
            // Close client gracefully
            client.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            logger.error("Error during producer cleanup", e);
        }
    }
}
```

## 📥 Consumer Best Practices

### Consumer Group Management

Design consumer groups for optimal parallelism and fault tolerance:

```java
// ✅ Good: Datacenter-aware consumer configuration
ConsumerConfig consumerConfig = ConsumerConfig.builder()
    .groupId("payment-processor-v1")  // Versioned group names
    .enableAutoCommit(false)  // Manual commit for reliability
    .autoOffsetReset("earliest")  // Consistent replay behavior
    .maxPollRecords(500)  // Reasonable batch size
    .sessionTimeout(Duration.ofSeconds(30))  // Appropriate timeout
    .partitioningStrategy(ConsumerPartitioningStrategy.DATACENTER_AWARE_RANGE)
    .build();
```

### Offset Management

Implement robust offset management strategies:

```java
// ✅ Good: Manual offset management with error handling
public void processMessages() {
    ConsumerRecords<String, String> records = client.consumerSync().poll(Duration.ofSeconds(5));
    
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    
    for (ConsumerRecord<String, String> record : records) {
        try {
            processRecord(record);
            
            // Track offsets for successful processing
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            offsetsToCommit.put(partition, new OffsetAndMetadata(record.offset() + 1));
            
        } catch (Exception e) {
            logger.error("Failed to process record: {}", record, e);
            // Handle individual record failures without stopping batch
            handleFailedRecord(record, e);
        }
    }
    
    // Commit only successfully processed offsets
    if (!offsetsToCommit.isEmpty()) {
        client.consumerSync().commitSync(offsetsToCommit);
    }
}
```

### Consumer Rebalancing

Handle rebalancing gracefully:

```java
// ✅ Good: Rebalance listener for graceful handling
public class GracefulRebalanceListener implements ConsumerRebalanceListener {
    
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked: {}", partitions);
        
        // Commit offsets before losing partitions
        try {
            client.consumerSync().commitSync();
            logger.info("Offsets committed successfully before rebalance");
        } catch (Exception e) {
            logger.error("Failed to commit offsets during rebalance", e);
        }
        
        // Clean up partition-specific resources
        cleanupPartitionResources(partitions);
    }
    
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned: {}", partitions);
        
        // Initialize partition-specific resources
        initializePartitionResources(partitions);
    }
}
```

### Consumer Error Handling

Implement comprehensive error handling with dead letter queues:

```java
// ✅ Good: Comprehensive error handling
public void processRecordSafely(ConsumerRecord<String, String> record) {
    try {
        // Primary processing logic
        businessLogic.process(record);
        
    } catch (TransientException e) {
        // Retry transient errors
        retryProcessor.scheduleRetry(record, e);
        
    } catch (PoisonMessageException e) {
        // Send to dead letter queue
        deadLetterQueueHandler.send(record, e);
        logger.warn("Poison message sent to DLQ: {}", record.key());
        
    } catch (Exception e) {
        // Log and decide based on error type
        logger.error("Unexpected error processing record: {}", record.key(), e);
        
        if (isRetryable(e)) {
            retryProcessor.scheduleRetry(record, e);
        } else {
            deadLetterQueueHandler.send(record, e);
        }
    }
}
```

## 🌐 Multi-Datacenter Architecture

### Datacenter Selection Strategy

Implement intelligent datacenter selection:

```java
// ✅ Good: Health-aware datacenter selection
@Component
public class DatacenterSelectionService {
    
    private final KafkaMultiDatacenterClient client;
    private final HealthMonitor healthMonitor;
    
    public String selectDatacenterForOperation(OperationType operation) {
        // Check datacenter health using actual API
        CompletableFuture<List<DatacenterInfo>> healthFuture = client.checkDatacenterHealth();
        List<DatacenterInfo> datacenters = healthFuture.join();
        
        List<String> healthyDatacenters = datacenters.stream()
            .filter(dc -> dc.isHealthy())
            .map(DatacenterInfo::getId)
            .collect(Collectors.toList());
        
        if (healthyDatacenters.isEmpty()) {
            throw new NoHealthyDatacentersException("No healthy datacenters available");
        }
        
        // Prefer local datacenter if healthy
        String localDatacenter = client.getLocalDatacenter().getId();
        if (healthyDatacenters.contains(localDatacenter)) {
            return localDatacenter;
        }
        
        // Fall back to first healthy datacenter
        return healthyDatacenters.get(0);
    }
}
```

### Data Locality Management

Optimize for data locality where possible:

```java
// ✅ Good: Data locality optimization
public class LocalityAwareProducer {
    
    public void sendWithLocality(String topic, String key, String value, String targetDatacenter) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        // Add locality hints
        record.headers().add("target.datacenter", targetDatacenter.getBytes());
        record.headers().add("source.datacenter", getCurrentDatacenter().getBytes());
        
        // Use geographic partitioning to respect locality
        client.producerAsync().sendAsync(targetDatacenter, record);
    }
}
```

### Cross-Datacenter Coordination

Design for eventual consistency and graceful degradation:

```java
// ✅ Good: Cross-datacenter coordination pattern
public class CrossDatacenterCoordinator {
    
    public void coordinateOperation(String operationId, Object data) {
        List<String> datacenters = getActiveDatacenters();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (String datacenter : datacenters) {
            CompletableFuture<Void> future = sendToDatacenter(datacenter, operationId, data)
                .orTimeout(Duration.ofSeconds(30))
                .exceptionally(throwable -> {
                    logger.warn("Failed to coordinate with datacenter: {}", datacenter, throwable);
                    return null;  // Continue with other datacenters
                });
            futures.add(future);
        }
        
        // Wait for majority success
        waitForMajoritySuccess(futures);
    }
}
```

## ⚡ Performance Optimization

### Connection Pool Tuning

Optimize connection pools for your workload:

```java
// ✅ Good: Workload-appropriate pool configuration
KafkaConnectionPoolManager poolManager = KafkaConnectionPoolManager.builder()
    .initialSize(5)        // Start with reasonable connections
    .maxSize(50)           // Allow scaling under load
    .maxIdleTime(Duration.ofMinutes(10))  // Clean up idle connections
    .validationQuery("SELECT 1")  // Health check query
    .validationInterval(Duration.ofSeconds(30))
    .build();
```

### Batch Processing Optimization

Implement efficient batch processing:

```java
// ✅ Good: Optimized batch processing
public class OptimizedBatchProcessor {
    
    private static final int OPTIMAL_BATCH_SIZE = 1000;
    private static final Duration BATCH_TIMEOUT = Duration.ofSeconds(5);
    
    public void processBatch() {
        List<ConsumerRecord<String, String>> batch = new ArrayList<>();
        Instant batchStart = Instant.now();
        
        while (batch.size() < OPTIMAL_BATCH_SIZE && 
               Duration.between(batchStart, Instant.now()).compareTo(BATCH_TIMEOUT) < 0) {
            
            ConsumerRecords<String, String> records = 
                client.consumerSync().poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, String> record : records) {
                batch.add(record);
                if (batch.size() >= OPTIMAL_BATCH_SIZE) break;
            }
        }
        
        if (!batch.isEmpty()) {
            processBatchEfficiently(batch);
        }
    }
}
```

### Memory Management

Implement proper memory management strategies:

```java
// ✅ Good: Memory-conscious processing
public class MemoryEfficientProcessor {
    
    private static final int MAX_MEMORY_USAGE_MB = 512;
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    
    public void processWithMemoryControl() {
        while (true) {
            long usedMemory = memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
            
            if (usedMemory > MAX_MEMORY_USAGE_MB) {
                logger.warn("Memory usage high: {}MB, triggering GC", usedMemory);
                System.gc();
                
                // Reduce batch size temporarily
                reduceBatchSize();
            }
            
            processNextBatch();
        }
    }
}
```

## 🛡️ Resilience and Fault Tolerance

### Circuit Breaker Configuration

Configure circuit breakers appropriately for your SLA:

```java
// ✅ Good: Environment-appropriate circuit breaker
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    .circuitBreaker(CircuitBreakerConfig.builder()
        .failureRateThreshold(50)  // 50% failure rate threshold
        .waitDurationInOpenState(Duration.ofSeconds(30))  // Wait before retry
        .slidingWindowSize(10)  // Sample size for failure rate
        .minimumNumberOfCalls(5)  // Minimum calls before evaluation
        .build())
    .retry(RetryConfig.builder()
        .maxAttempts(3)
        .waitDuration(Duration.ofSeconds(2))
        .exponentialBackoffMultiplier(2.0)
        .retryExceptions(TransientException.class)
        .ignoreExceptions(PoisonMessageException.class)
        .build())
    .rateLimiter(RateLimiterConfig.builder()
        .limitForPeriod(1000)  // 1000 calls per period
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .timeoutDuration(Duration.ofMilliseconds(100))
        .build())
    .build();
```

### Graceful Degradation

Implement graceful degradation patterns:

```java
// ✅ Good: Graceful degradation with fallbacks
public class ResilientMessageProcessor {
    
    public void processMessage(String message) {
        try {
            // Primary processing path
            primaryProcessor.process(message);
            
        } catch (PrimaryProcessorException e) {
            logger.warn("Primary processor failed, using fallback", e);
            
            try {
                // Fallback processing
                fallbackProcessor.process(message);
                
            } catch (FallbackProcessorException fe) {
                logger.error("Both processors failed, storing for later", fe);
                
                // Store for manual processing
                failureStore.store(message, e, fe);
            }
        }
    }
}
```

### Health Monitoring Integration

Integrate comprehensive health monitoring:

```java
// ✅ Good: Comprehensive health monitoring
@Component
public class KafkaHealthService implements HealthIndicator {
    
    private final KafkaMultiDatacenterClient client;
    private final MeterRegistry meterRegistry;
    
    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        
        try {
            // Check datacenter connectivity
            Map<String, Boolean> datacenterHealth = checkDatacenterHealth();
            builder.withDetail("datacenters", datacenterHealth);
            
            // Check connection pool health
            ConnectionPoolHealth poolHealth = checkConnectionPoolHealth();
            builder.withDetail("connectionPool", poolHealth);
            
            // Check schema registry health
            boolean schemaRegistryHealthy = checkSchemaRegistryHealth();
            builder.withDetail("schemaRegistry", schemaRegistryHealthy);
            
            // Overall health assessment
            if (hasHealthyDatacenters(datacenterHealth)) {
                return builder.build();
            } else {
                return builder.down().build();
            }
            
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }
}
```

## 🔒 Security Best Practices

### SSL/TLS Configuration

Implement proper SSL/TLS configuration:

```java
// ✅ Good: Secure SSL configuration in datacenter endpoint
KafkaDatacenterEndpoint secureEndpoint = KafkaDatacenterEndpoint.builder()
    .id("secure-datacenter")
    .bootstrapServers("kafka-secure.company.com:9093")
    .ssl(true)
    .securityProtocol("SSL")
    .keystorePath(System.getenv("KAFKA_KEYSTORE_PATH"))
    .keystorePassword(System.getenv("KAFKA_KEYSTORE_PASSWORD"))
    .truststorePath(System.getenv("KAFKA_TRUSTSTORE_PATH"))
    .truststorePassword(System.getenv("KAFKA_TRUSTSTORE_PASSWORD"))
    .build();

// ❌ Bad: Insecure configuration
KafkaDatacenterEndpoint insecureEndpoint = KafkaDatacenterEndpoint.builder()
    .id("insecure-datacenter")
    .bootstrapServers("kafka-insecure.company.com:9092")
    .ssl(false)  // Don't disable SSL in production
    .build();
```

### Credential Management

Use proper credential management:

```java
// ✅ Good: Secure credential management using datacenter endpoint configuration
@Configuration
public class SecurityConfiguration {
    
    @Bean
    public KafkaDatacenterEndpoint createSecureDatacenterEndpoint() {
        return KafkaDatacenterEndpoint.builder()
            .id("secure-datacenter")
            .bootstrapServers("kafka-secure.company.com:9093")
            .ssl(true)
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .saslUsername(credentialManager.getUsername("kafka"))
            .saslPassword(credentialManager.getPassword("kafka"))
            .truststorePath("/path/to/truststore.jks")
            .truststorePassword(credentialManager.getPassword("truststore"))
            .build();
    }
    
    // Use secure credential storage
    @Bean
    public CredentialManager credentialManager() {
        return new VaultCredentialManager(vaultClient);
    }
}

// ❌ Bad: Hardcoded credentials
KafkaDatacenterEndpoint insecure = KafkaDatacenterEndpoint.builder()
    .id("insecure")
    .saslUsername("admin")  // Don't hardcode
    .saslPassword("password123")  // Don't hardcode
    .build();
```

### Data Encryption

Implement proper data encryption:

```java
// ✅ Good: Field-level encryption for sensitive data
public class EncryptedMessageProcessor {
    
    private final FieldEncryption fieldEncryption;
    
    public ProducerRecord<String, String> encryptSensitiveFields(UserEvent event) {
        // Encrypt sensitive fields
        String encryptedPii = fieldEncryption.encrypt(event.getPii());
        String encryptedEmail = fieldEncryption.encrypt(event.getEmail());
        
        UserEvent encrypted = event.toBuilder()
            .pii(encryptedPii)
            .email(encryptedEmail)
            .build();
        
        return new ProducerRecord<>("user-events", event.getUserId(), 
            objectMapper.writeValueAsString(encrypted));
    }
}
```

## 📊 Monitoring and Observability

### Metrics Collection

Implement comprehensive metrics collection:

```java
// ✅ Good: Comprehensive metrics
@Component
public class KafkaMetricsCollector {
    
    private final MeterRegistry meterRegistry;
    private final KafkaMultiDatacenterClient client;
    
    @EventListener
    public void handleMessageSent(MessageSentEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        sample.stop(Timer.builder("kafka.producer.send.duration")
            .tag("datacenter", event.getDatacenter())
            .tag("topic", event.getTopic())
            .register(meterRegistry));
        
        meterRegistry.counter("kafka.producer.messages.sent",
            "datacenter", event.getDatacenter(),
            "topic", event.getTopic()).increment();
    }
    
    @Scheduled(fixedRate = 60000)  // Every minute
    public void collectConnectionPoolMetrics() {
        ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics();
        
        meterRegistry.gauge("kafka.pool.active.connections", metrics.getActiveConnections());
        meterRegistry.gauge("kafka.pool.idle.connections", metrics.getIdleConnections());
        meterRegistry.gauge("kafka.pool.pending.requests", metrics.getPendingRequests());
    }
}
```

### Distributed Tracing

Implement distributed tracing:

```java
// ✅ Good: Distributed tracing integration
@Component
public class TracingKafkaService {
    
    private final Tracer tracer;
    private final KafkaMultiDatacenterClient client;
    
    public void sendWithTracing(ProducerRecord<String, String> record) {
        Span span = tracer.nextSpan()
            .name("kafka.producer.send")
            .tag("messaging.system", "kafka")
            .tag("messaging.destination", record.topic())
            .tag("messaging.destination_kind", "topic")
            .start();
        
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            // Inject trace context into headers
            TraceContext.Injector<Headers> injector = tracing.propagation()
                .injector(Headers::add);
            injector.inject(span.context(), record.headers());
            
            client.producerSync().send(record);
            span.tag("success", "true");
            
        } catch (Exception e) {
            span.tag("error", e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
```

### Alerting Configuration

Set up proper alerting:

```java
// ✅ Good: Alert configuration
@Configuration
public class AlertingConfiguration {
    
    @Bean
    public AlertManager kafkaAlertManager() {
        return AlertManager.builder()
            .rule(AlertRule.builder()
                .name("High Error Rate")
                .condition("kafka.producer.errors.rate > 0.05")
                .duration(Duration.ofMinutes(5))
                .severity(AlertSeverity.WARNING)
                .build())
            .rule(AlertRule.builder()
                .name("Datacenter Unavailable")
                .condition("kafka.datacenter.health == false")
                .duration(Duration.ofMinutes(1))
                .severity(AlertSeverity.CRITICAL)
                .build())
            .rule(AlertRule.builder()
                .name("High Consumer Lag")
                .condition("kafka.consumer.lag > 10000")
                .duration(Duration.ofMinutes(3))
                .severity(AlertSeverity.WARNING)
                .build())
            .build();
    }
}
```

## 📄 Schema Management

### Schema Evolution Strategy

Implement backward-compatible schema evolution:

```java
// ✅ Good: Backward-compatible schema evolution
@Component
public class SchemaEvolutionManager {
    
    private final SchemaRegistryClient schemaRegistry;
    
    public void evolveSchema(String subject, Schema newSchema) {
        // Check compatibility before registration
        CompatibilityLevel compatibility = schemaRegistry.getCompatibility(subject);
        
        if (schemaRegistry.testCompatibility(subject, newSchema)) {
            schemaRegistry.register(subject, newSchema);
            logger.info("Schema evolved successfully for subject: {}", subject);
        } else {
            throw new SchemaIncompatibilityException(
                "New schema is not compatible with existing schema for subject: " + subject);
        }
    }
    
    // Handle schema evolution in consumers
    public Object deserializeWithEvolution(byte[] data, String writerSchemaId, Schema readerSchema) {
        Schema writerSchema = schemaRegistry.getSchemaById(writerSchemaId);
        
        if (isCompatible(writerSchema, readerSchema)) {
            return deserializeWithProjection(data, writerSchema, readerSchema);
        } else {
            throw new SchemaEvolutionException("Incompatible schema evolution detected");
        }
    }
}
```

### Schema Registry High Availability

Configure schema registry for high availability:

```java
// ✅ Good: HA schema registry configuration
SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
    .urls(Arrays.asList(
        "https://schema-registry-1.example.com",
        "https://schema-registry-2.example.com",
        "https://schema-registry-3.example.com"
    ))
    .maxCacheSize(1000)
    .cacheExpirationTime(Duration.ofMinutes(60))
    .retries(3, Duration.ofSeconds(2))
    .authentication(SchemaRegistryAuth.builder()
        .basic("username", "password")
        .build())
    .build();
```

## 🧪 Testing Strategies

### Integration Testing

Implement comprehensive integration testing:

```java
// ✅ Good: Comprehensive integration testing
@SpringBootTest
@TestContainers
class KafkaMultiDatacenterIntegrationTest {
    
    @Container
    static KafkaContainer kafka1 = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
        .withExternalZookeeper("zookeeper:2181");
    
    @Container
    static KafkaContainer kafka2 = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
        .withExternalZookeeper("zookeeper:2181");
    
    @Test
    void shouldHandleDatacenterFailover() {
        // Setup multi-datacenter client
        KafkaMultiDatacenterClient client = createTestClient();
        
        // Send messages to first datacenter
        sendTestMessages(client, "datacenter-1");
        
        // Simulate datacenter failure
        kafka1.stop();
        
        // Verify failover to second datacenter
        List<DatacenterInfo> healthyDatacenters = client.checkDatacenterHealth().join();
        List<String> healthyIds = healthyDatacenters.stream()
            .filter(DatacenterInfo::isHealthy)
            .map(DatacenterInfo::getId)
            .collect(Collectors.toList());
        assertThat(healthyIds).containsOnly("datacenter-2");
        
        // Verify continued operation
        sendTestMessages(client, "datacenter-2");
        verifyMessagesReceived();
    }
}
```

### Performance Testing

Implement performance testing:

```java
// ✅ Good: Performance testing with proper metrics
@Test
void performanceTest() {
    KafkaMultiDatacenterClient client = createHighThroughputClient();
    
    int messageCount = 100_000;
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    
    Instant start = Instant.now();
    
    List<CompletableFuture<Void>> futures = IntStream.range(0, threadCount)
        .mapToObj(threadId -> CompletableFuture.runAsync(() -> {
            for (int i = 0; i < messageCount / threadCount; i++) {
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>("perf-test", "key-" + i, "value-" + i);
                client.producerAsync().sendAsync(record);
            }
        }, executor))
        .collect(Collectors.toList());
    
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    
    Duration duration = Duration.between(start, Instant.now());
    double throughput = messageCount / duration.toSeconds();
    
    logger.info("Performance test completed: {} messages/second", throughput);
    assertThat(throughput).isGreaterThan(10_000);  // Minimum expected throughput
}
```

### Chaos Testing

Implement chaos engineering tests:

```java
// ✅ Good: Chaos testing for resilience
@Test
void chaosTest() {
    KafkaMultiDatacenterClient client = createResilientClient();
    ChaosMonkey chaosMonkey = new ChaosMonkey();
    
    // Start background message processing
    CompletableFuture<Void> processingTask = startBackgroundProcessing(client);
    
    // Introduce various failures
    chaosMonkey.scheduleRandomFailures(Arrays.asList(
        () -> disconnectRandomDatacenter(),
        () -> introduceNetworkLatency(),
        () -> corruptRandomMessages(),
        () -> overloadSchemaRegistry()
    ), Duration.ofMinutes(10));
    
    // Verify system resilience
    verifySystemContinuesOperating();
    verifyNoDataLoss();
    verifyRecoveryTime();
}
```

## 🚀 Deployment and Operations

### Deployment Configuration

Use proper deployment configurations:

```yaml
# ✅ Good: Production deployment configuration
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-client-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: kafka-client-app:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            secretKeyRef:
              name: kafka-config
              key: bootstrap-servers
        - name: JVM_OPTS
          value: "-Xmx2g -XX:+UseG1GC -XX:+UseStringDeduplication"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
```

### JVM Tuning

Optimize JVM settings for Kafka workloads:

```bash
# ✅ Good: JVM tuning for Kafka applications
JAVA_OPTS="-Xmx4g \
           -Xms4g \
           -XX:+UseG1GC \
           -XX:MaxGCPauseMillis=20 \
           -XX:+UseStringDeduplication \
           -XX:+UnlockExperimentalVMOptions \
           -XX:+UseCGroupMemoryLimitForHeap \
           -Djava.security.egd=file:/dev/./urandom"

# Enable JFR for performance monitoring
JFR_OPTS="-XX:+FlightRecorder \
          -XX:StartFlightRecording=duration=30m,filename=kafka-client.jfr"
```

### Monitoring Setup

Set up comprehensive monitoring:

```yaml
# ✅ Good: Monitoring configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  kafka-client-rules.yml: |
    groups:
    - name: kafka-client
      rules:
      - alert: KafkaHighErrorRate
        expr: rate(kafka_producer_errors_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High Kafka error rate detected"
          
      - alert: KafkaDatacenterDown
        expr: kafka_datacenter_health == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka datacenter is down"
```

## 🔧 Troubleshooting

### Common Issues and Solutions

#### High Consumer Lag

```java
// ✅ Good: Consumer lag monitoring and resolution
@Component
public class ConsumerLagMonitor {
    
    @Scheduled(fixedRate = 30000)  // Every 30 seconds
    public void monitorConsumerLag() {
        Map<TopicPartition, Long> lag = client.getConsumerLag();
        
        for (Map.Entry<TopicPartition, Long> entry : lag.entrySet()) {
            if (entry.getValue() > LAG_THRESHOLD) {
                logger.warn("High consumer lag detected: {} = {}", 
                    entry.getKey(), entry.getValue());
                
                // Auto-scale consumers if possible
                if (canAutoScale()) {
                    scaleConsumers();
                }
                
                // Alert operations team
                alertManager.sendAlert(AlertType.HIGH_CONSUMER_LAG, entry);
            }
        }
    }
}
```

#### Connection Pool Exhaustion

```java
// ✅ Good: Connection pool monitoring and management
@Component
public class ConnectionPoolManager {
    
    @EventListener
    public void handlePoolExhaustion(PoolExhaustionEvent event) {
        logger.error("Connection pool exhausted for datacenter: {}", event.getDatacenter());
        
        // Immediate actions
        forcePoolCleanup(event.getDatacenter());
        
        // Scale pool if possible
        if (canScalePool(event.getDatacenter())) {
            scaleConnectionPool(event.getDatacenter());
        }
        
        // Enable circuit breaker to protect pool
        enableCircuitBreaker(event.getDatacenter());
    }
}
```

#### Schema Registry Issues

```java
// ✅ Good: Schema registry troubleshooting
@Component
public class SchemaRegistryTroubleshooter {
    
    public void diagnoseSchemaIssues(String subject) {
        try {
            // Check schema registry connectivity
            if (!schemaRegistry.isHealthy()) {
                logger.error("Schema registry is not healthy");
                return;
            }
            
            // Check schema compatibility
            List<Schema> schemas = schemaRegistry.getAllVersions(subject);
            for (int i = 1; i < schemas.size(); i++) {
                if (!isCompatible(schemas.get(i-1), schemas.get(i))) {
                    logger.error("Schema incompatibility detected between versions {} and {}", 
                        i-1, i);
                }
            }
            
            // Check schema evolution
            analyzeSchemaEvolution(subject, schemas);
            
        } catch (Exception e) {
            logger.error("Schema diagnosis failed for subject: {}", subject, e);
        }
    }
}
```

### Debugging Tools

```java
// ✅ Good: Built-in debugging tools
@RestController
public class KafkaDebugController {
    
    @GetMapping("/debug/health")
    public Map<String, Object> getDetailedHealth() {
        Map<String, Object> health = new HashMap<>();
        
        health.put("datacenters", client.getDatacenterHealth());
        health.put("connectionPools", client.getConnectionPoolStatus());
        health.put("schemaRegistry", client.getSchemaRegistryHealth());
        health.put("circuitBreakers", client.getCircuitBreakerStatus());
        
        return health;
    }
    
    @GetMapping("/debug/metrics")
    public Map<String, Object> getDetailedMetrics() {
        return metricsCollector.getAllMetrics();
    }
    
    @PostMapping("/debug/force-failover/{datacenter}")
    public ResponseEntity<String> forceFailover(@PathVariable String datacenter) {
        try {
            client.forceDatacenterFailover(datacenter);
            return ResponseEntity.ok("Failover initiated for datacenter: " + datacenter);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Failover failed: " + e.getMessage());
        }
    }
}
```

---

## 📝 Summary

Following these best practices will help you:

1. **Build resilient applications** that can handle datacenter failures gracefully
2. **Optimize performance** for your specific workload patterns  
3. **Maintain security** with proper authentication and encryption
4. **Monitor effectively** with comprehensive metrics and alerting
5. **Test thoroughly** with integration, performance, and chaos testing
6. **Deploy confidently** with proper configuration and monitoring
7. **Troubleshoot efficiently** when issues arise

Remember that the Kafka Multi-Datacenter Client Library is designed for enterprise use cases where reliability, performance, and observability are paramount. Always test thoroughly in staging environments that mirror your production setup before deploying changes.

For specific implementation details, refer to the other documentation guides and example code provided with the library.
