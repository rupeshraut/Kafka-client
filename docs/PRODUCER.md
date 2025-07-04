# Producer Guide

This guide provides comprehensive information on using the Kafka Multi-Datacenter Client Library's producer capabilities across synchronous, asynchronous, and reactive programming models.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Synchronous Producer Operations](#synchronous-producer-operations)
- [Asynchronous Producer Operations](#asynchronous-producer-operations)
- [Reactive Producer Operations](#reactive-producer-operations)
- [Partitioning Strategies](#partitioning-strategies)
- [Error Handling and Resilience](#error-handling-and-resilience)
- [Serialization](#serialization)
- [Transactions](#transactions)
- [Performance Optimization](#performance-optimization)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Best Practices](#best-practices)

## Overview

The Kafka Multi-Datacenter Client Library provides three programming models for producing messages:

- **Synchronous**: Traditional blocking operations for simple use cases
- **Asynchronous**: CompletableFuture-based non-blocking operations
- **Reactive**: Reactive Streams (Project Reactor) for high-throughput scenarios

All producer operations support:
- Multi-datacenter routing and failover
- Advanced partitioning strategies
- Comprehensive error handling and resilience patterns
- Rich serialization options
- Transaction support
- Extensive monitoring and metrics

## Getting Started

### Basic Producer Setup

```java
// Create configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(KafkaDatacenterEndpoint.builder()
        .id("primary")
        .region("us-east-1")
        .bootstrapServers("localhost:9092")
        .build())
    .localDatacenter("primary")
    .producerConfig(ProducerConfig.defaultConfig())
    .build();

// Create client
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
    .configuration(config)
    .build()) {
    
    // Use producer operations
    sendMessages(client);
}
```

### Producer Configuration

```java
// Use default producer configuration
ProducerConfig producerConfig = ProducerConfig.defaultConfig();

// For custom producer settings, configure them in the main configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(endpoint)
    .producerConfig(producerConfig)
    .additionalProperties(Map.of(
        // Serializers
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer", "org.apache.kafka.common.serialization.JsonSerializer",
        
        // Performance settings
        "batch.size", 16384,
        "linger.ms", 5,
        "compression.type", "snappy",
        
        // Reliability settings
        "acks", "all",
        "retries", Integer.MAX_VALUE,
        "enable.idempotence", true
    ))
    .build();
```

## Synchronous Producer Operations

### Basic Synchronous Sending

```java
public void sendSynchronously(KafkaMultiDatacenterClient client) {
    try {
        // Create a record
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "my-topic",
            "message-key",
            Map.of("message", "Hello, Kafka!", "timestamp", Instant.now())
        );
        
        // Send synchronously
        RecordMetadata metadata = client.producerSync().send(record);
        
        logger.info("Message sent successfully to partition {} at offset {}", 
                   metadata.partition(), metadata.offset());
                   
    } catch (Exception e) {
        logger.error("Failed to send message", e);
    }
}
```

### Batch Synchronous Sending

```java
public void sendBatchSynchronously(KafkaMultiDatacenterClient client) {
    List<ProducerRecord<String, Object>> records = createBatchRecords();
    List<RecordMetadata> results = new ArrayList<>();
    
    for (ProducerRecord<String, Object> record : records) {
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            results.add(metadata);
            logger.info("Record sent to partition {} at offset {}", 
                       metadata.partition(), metadata.offset());
        } catch (Exception e) {
            logger.error("Failed to send record: {}", record.key(), e);
        }
    }
    
    logger.info("Batch send completed: {} out of {} records sent successfully", 
               results.size(), records.size());
}
```

### Synchronous Send with Timeout

```java
public void sendWithTimeout(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "my-topic",
        "timeout-key",
        Map.of("message", "Message with timeout")
    );
    
    try {
        // Send with timeout
        RecordMetadata metadata = client.producerSync().send(record, Duration.ofSeconds(10));
        logger.info("Message sent within timeout to partition {}", metadata.partition());
    } catch (TimeoutException e) {
        logger.error("Message send timed out", e);
    } catch (Exception e) {
        logger.error("Failed to send message", e);
    }
}
```

## Asynchronous Producer Operations

### Basic Asynchronous Sending

```java
public void sendAsynchronously(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "my-topic",
        "async-key",
        Map.of("message", "Async Hello, Kafka!", "timestamp", Instant.now())
    );
    
    // Send asynchronously
    CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
    
    future
        .thenAccept(metadata -> {
            logger.info("Async message sent to partition {} at offset {}", 
                       metadata.partition(), metadata.offset());
        })
        .exceptionally(throwable -> {
            logger.error("Failed to send async message", throwable);
            return null;
        });
}
```

### Asynchronous Batch Sending

```java
public void sendBatchAsynchronously(KafkaMultiDatacenterClient client) {
    List<ProducerRecord<String, Object>> records = createBatchRecords();
    
    // Send all records asynchronously
    List<CompletableFuture<RecordMetadata>> futures = records.stream()
        .map(record -> client.producerAsync().sendAsync(record))
        .collect(Collectors.toList());
    
    // Wait for all to complete
    CompletableFuture<List<RecordMetadata>> allFutures = CompletableFuture.allOf(
        futures.toArray(new CompletableFuture[0])
    ).thenApply(v -> futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList()));
    
    allFutures
        .thenAccept(results -> {
            logger.info("Batch async send completed: {} records sent", results.size());
            results.forEach(metadata -> 
                logger.info("Record sent to partition {} at offset {}", 
                           metadata.partition(), metadata.offset()));
        })
        .exceptionally(throwable -> {
            logger.error("Batch async send failed", throwable);
            return null;
        });
}
```

### Asynchronous Send with Callback

```java
public void sendWithCallback(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "my-topic",
        "callback-key",
        Map.of("message", "Message with callback", "id", UUID.randomUUID().toString())
    );
    
    // Send with custom callback handling
    CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
    
    future
        .whenComplete((metadata, throwable) -> {
            if (throwable == null) {
                // Success
                logger.info("Message sent successfully: partition={}, offset={}, timestamp={}", 
                           metadata.partition(), metadata.offset(), metadata.timestamp());
                
                // Update metrics or trigger dependent operations
                updateSuccessMetrics(metadata);
                triggerDownstreamProcessing(record, metadata);
            } else {
                // Error handling
                logger.error("Failed to send message: {}", record.key(), throwable);
                
                // Error recovery or alerting
                handleSendError(record, throwable);
                triggerErrorNotification(record, throwable);
            }
        });
}
```

### Async Send with Timeout

```java
public void sendAsyncWithTimeout(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "my-topic",
        "timeout-async-key",
        Map.of("message", "Async message with timeout")
    );
    
    CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
    
    // Apply timeout
    CompletableFuture<RecordMetadata> timeoutFuture = future.orTimeout(5, TimeUnit.SECONDS);
    
    timeoutFuture
        .thenAccept(metadata -> {
            logger.info("Async message sent within timeout to partition {}", metadata.partition());
        })
        .exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                logger.error("Async message send timed out");
            } else {
                logger.error("Failed to send async message", throwable);
            }
            return null;
        });
}
```

## Reactive Producer Operations

### Basic Reactive Sending

```java
public void sendReactively(KafkaMultiDatacenterClient client) {
    // Create a stream of records
    Flux<ProducerRecord<String, Object>> recordStream = Flux.range(1, 100)
        .map(i -> new ProducerRecord<>(
            "my-topic",
            "reactive-key-" + i,
            Map.of("message", "Reactive message " + i, "index", i)
        ));
    
    // Send reactively
    Flux<RecordMetadata> resultStream = client.producerReactive().send(recordStream);
    
    resultStream
        .doOnNext(metadata -> {
            logger.info("Reactive message sent to partition {} at offset {}", 
                       metadata.partition(), metadata.offset());
        })
        .doOnError(throwable -> {
            logger.error("Failed to send reactive message", throwable);
        })
        .doOnComplete(() -> {
            logger.info("Reactive sending completed");
        })
        .subscribe();
}
```

### Reactive Batch Processing

```java
public void sendReactiveBatches(KafkaMultiDatacenterClient client) {
    Flux<ProducerRecord<String, Object>> recordStream = createReactiveRecordStream();
    
    // Process in batches
    Flux<RecordMetadata> resultStream = recordStream
        .buffer(50) // Buffer 50 records at a time
        .flatMap(batch -> {
            logger.info("Processing batch of {} records", batch.size());
            
            // Send batch reactively
            return client.producerReactive().send(Flux.fromIterable(batch))
                .doOnNext(metadata -> updateBatchMetrics(metadata))
                .onErrorContinue((error, record) -> {
                    logger.error("Failed to send record in batch", error);
                });
        });
    
    resultStream
        .doOnComplete(() -> logger.info("All batches processed"))
        .subscribe();
}
```

### Reactive Send with Backpressure

```java
public void sendWithBackpressure(KafkaMultiDatacenterClient client) {
    Flux<ProducerRecord<String, Object>> recordStream = createHighVolumeRecordStream();
    
    // Apply backpressure and rate limiting
    Flux<RecordMetadata> resultStream = recordStream
        .onBackpressureBuffer(1000) // Buffer up to 1000 records
        .delayElements(Duration.ofMillis(10)) // Rate limit to 100 messages/second
        .flatMap(record -> 
            client.producerReactive().send(Mono.just(record))
                .retry(3) // Retry up to 3 times
                .onErrorReturn(createErrorMetadata(record)), // Fallback on error
            10 // Concurrency limit
        );
    
    resultStream
        .doOnNext(metadata -> logger.debug("Sent with backpressure control"))
        .subscribe();
}
```

### Reactive Send with Error Recovery

```java
public void sendWithErrorRecovery(KafkaMultiDatacenterClient client) {
    Flux<ProducerRecord<String, Object>> recordStream = createRecordStream();
    
    Flux<RecordMetadata> resultStream = recordStream
        .flatMap(record -> 
            client.producerReactive().send(Mono.just(record))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> isRetriableError(throwable)))
                .onErrorResume(throwable -> {
                    logger.error("Failed to send record after retries: {}", record.key(), throwable);
                    // Send to dead letter queue or alternative handling
                    return sendToDeadLetterQueue(record)
                        .map(dlqMetadata -> createErrorMetadata(record));
                })
        );
    
    resultStream
        .doOnNext(metadata -> logger.info("Processed record: {}", metadata))
        .subscribe();
}
```

## Partitioning Strategies

The Kafka Multi-Datacenter Client Library provides several built-in partitioning strategies to control how messages are distributed across topic partitions.

### Available Partitioning Strategies

#### DEFAULT

Uses Kafka's default partitioning behavior:

- For records with keys: hash-based partitioning
- For records without keys: round-robin distribution

#### ROUND_ROBIN

Distributes messages evenly across all partitions in a round-robin fashion.

- **Use case**: Even load distribution when keys are not important
- **Performance**: High throughput, minimal computation
- **Consistency**: No key-based consistency guarantees

#### KEY_HASH

Uses MD5-based consistent hashing of the message key.

- **Use case**: When you need consistent key-to-partition mapping with cryptographic distribution
- **Performance**: Moderate (due to MD5 computation)
- **Consistency**: Same key always maps to the same partition

#### MODULUS

Uses simple modulus operation on the key's hash code for partitioning.

- **Algorithm**: `partition = abs(key.hashCode()) % numPartitions`
- **Use case**: High-throughput scenarios requiring consistent key-based partitioning
- **Performance**: Very fast (no cryptographic hashing)
- **Consistency**: Same key always maps to the same partition
- **Benefits**:
  - Faster than KEY_HASH strategy
  - Handles `Integer.MIN_VALUE` edge case safely
  - Predictable and simple algorithm
- **Ideal for**: Applications with well-distributed keys where performance matters

#### RANDOM

Randomly selects a partition for each message.

- **Use case**: When partition distribution should be completely random
- **Performance**: High throughput
- **Consistency**: No consistency guarantees

#### STICKY

Sends messages to the same partition until a batch threshold is reached.

- **Use case**: Optimizing for batching efficiency
- **Performance**: Excellent for high-throughput scenarios
- **Configuration**: `sticky.batch.size` (default: 100 messages)

#### GEOGRAPHIC

Routes messages based on geographic regions or datacenter locations.

- **Use case**: Multi-region applications with data locality requirements
- **Configuration**: Region-to-partition mapping

#### TIME_BASED

Partitions messages based on timestamp information.

- **Use case**: Time-series data with temporal locality requirements
- **Configuration**: Time window and partition mapping

#### LOAD_BALANCED

Considers current partition load metrics for distribution.

- **Use case**: Dynamic load balancing based on real-time metrics
- **Performance**: Adaptive to current cluster state

### Using Built-in Partitioning Strategies

```java
public void demonstratePartitioningStrategies(KafkaMultiDatacenterClient client) {
    // Create partitioning manager
    ProducerPartitioningManager partitioningManager = new ProducerPartitioningManager();
    
    // Round-robin partitioning
    partitioningManager.setPartitioningStrategy(ProducerPartitioningType.ROUND_ROBIN);
    sendWithPartitioning(client, partitioningManager, "round-robin");
    
    // Key-hash partitioning (default for records with keys)
    partitioningManager.setPartitioningStrategy(ProducerPartitioningType.KEY_HASH);
    sendWithPartitioning(client, partitioningManager, "key-hash");
    
    // Modulus partitioning (faster alternative to key-hash)
    partitioningManager.setPartitioningStrategy(ProducerPartitioningType.MODULUS);
    sendWithPartitioning(client, partitioningManager, "modulus");
    
    // Load-balanced partitioning
    partitioningManager.setPartitioningStrategy(ProducerPartitioningType.LOAD_BALANCED);
    sendWithPartitioning(client, partitioningManager, "load-balanced");
    
    // Sticky partitioning
    Map<String, Object> stickyConfig = Map.of("batch.threshold", 10);
    partitioningManager.setPartitioningStrategy(ProducerPartitioningType.STICKY, stickyConfig);
    sendWithPartitioning(client, partitioningManager, "sticky");
}

private void sendWithPartitioning(KafkaMultiDatacenterClient client, 
                                 ProducerPartitioningManager manager,
                                 String strategyName) {
    for (int i = 0; i < 20; i++) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "partitioned-topic",
            "key-" + i,
            Map.of("message", "Partitioned message " + i, "strategy", strategyName)
        );
        
        // Determine partition using strategy
        Integer partition = manager.determinePartition(record, 5); // 5 partitions
        if (partition != null) {
            record = new ProducerRecord<>(
                record.topic(),
                partition,
                record.key(),
                record.value()
            );
        }
        
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("Strategy {}: Message {} sent to partition {}", 
                       strategyName, i, metadata.partition());
        } catch (Exception e) {
            logger.error("Failed to send partitioned message", e);
        }
    }
}
```

### Custom Partitioning Strategy

```java
public void demonstrateCustomPartitioning(KafkaMultiDatacenterClient client) {
    ProducerPartitioningManager manager = new ProducerPartitioningManager();
    
    // Create custom geographic partitioning strategy
    ProducerPartitioningStrategy<String, Object> geoStrategy = 
        new ProducerPartitioningStrategy<String, Object>() {
            @Override
            public Integer partition(ProducerRecord<String, Object> record, int numPartitions) {
                // Extract region from message headers or content
                String region = extractRegionFromRecord(record);
                
                switch (region) {
                    case "us-east": return 0;
                    case "us-west": return 1;
                    case "europe": return 2;
                    case "asia": return 3;
                    default: return null; // Use default partitioning
                }
            }
            
            @Override
            public String getStrategyName() {
                return "geographic-partitioning";
            }
        };
    
    // Register and use custom strategy
    manager.registerCustomStrategy("geographic", geoStrategy);
    manager.setCustomPartitioningStrategy("geographic");
    
    // Send messages with geographic distribution
    String[] regions = {"us-east", "us-west", "europe", "asia"};
    for (int i = 0; i < 20; i++) {
        String region = regions[i % regions.length];
        
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "geo-topic",
            "geo-key-" + i,
            Map.of("message", "Geographic message " + i, "region", region)
        );
        
        // Add region header for partitioning
        record.headers().add(new RecordHeader("region", region.getBytes(StandardCharsets.UTF_8)));
        
        Integer partition = manager.determinePartition(record, 4);
        if (partition != null) {
            record = new ProducerRecord<>(
                record.topic(),
                partition,
                record.key(),
                record.value(),
                record.headers()
            );
        }
        
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("Geographic message from {} sent to partition {}", 
                       region, metadata.partition());
        } catch (Exception e) {
            logger.error("Failed to send geographic message", e);
        }
    }
}

private String extractRegionFromRecord(ProducerRecord<String, Object> record) {
    // Check headers first
    Header regionHeader = record.headers().lastHeader("region");
    if (regionHeader != null) {
        return new String(regionHeader.value(), StandardCharsets.UTF_8);
    }
    
    // Check message content
    if (record.value() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) record.value();
        Object region = value.get("region");
        if (region instanceof String) {
            return (String) region;
        }
    }
    
    return "unknown";
}
```

## Error Handling and Resilience

### Retry Configuration

```java
public void sendWithRetryLogic(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "retry-topic",
        "retry-key",
        Map.of("message", "Message with retry logic")
    );
    
    // Configure retry behavior
    RetryPolicy retryPolicy = RetryPolicy.builder()
        .maxAttempts(5)
        .backoff(Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0)
        .retryOn(RetriableException.class, TimeoutException.class)
        .ignoreOn(AuthorizationException.class, SerializationException.class)
        .build();
    
    // Send with retry
    CompletableFuture<RecordMetadata> future = client.producerAsync()
        .sendWithRetry(record, retryPolicy);
    
    future
        .thenAccept(metadata -> {
            logger.info("Message sent successfully after retries to partition {}", 
                       metadata.partition());
        })
        .exceptionally(throwable -> {
            logger.error("Message failed after all retry attempts", throwable);
            // Send to dead letter queue or trigger alert
            handlePermanentFailure(record, throwable);
            return null;
        });
}
```

### Circuit Breaker Integration

```java
public void sendWithCircuitBreaker(KafkaMultiDatacenterClient client) {
    // Circuit breaker is automatically applied through resilience configuration
    // but you can also check circuit breaker state
    
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "cb-topic",
        "cb-key",
        Map.of("message", "Message with circuit breaker")
    );
    
    // Check if circuit breaker allows the call
    if (client.getHealthIndicator().getCircuitBreakerState() == CircuitBreaker.State.OPEN) {
        logger.warn("Circuit breaker is open, message will be rejected");
        handleCircuitBreakerOpen(record);
        return;
    }
    
    try {
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("Message sent successfully through circuit breaker to partition {}", 
                   metadata.partition());
    } catch (CallNotPermittedException e) {
        logger.error("Circuit breaker rejected the call", e);
        handleCircuitBreakerRejection(record, e);
    } catch (Exception e) {
        logger.error("Failed to send message", e);
    }
}
```

### Error Recovery Strategies

```java
public void sendWithErrorRecovery(KafkaMultiDatacenterClient client) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(
        "recovery-topic",
        "recovery-key",
        Map.of("message", "Message with error recovery")
    );
    
    CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
    
    future
        .handle((metadata, throwable) -> {
            if (throwable == null) {
                return CompletableFuture.completedFuture(metadata);
            }
            
            // Determine error recovery strategy
            if (isRetriableError(throwable)) {
                logger.warn("Retriable error, attempting recovery: {}", throwable.getMessage());
                return retryWithBackoff(client, record, 3);
            } else if (isSerializationError(throwable)) {
                logger.error("Serialization error, attempting alternative serialization", throwable);
                return sendWithAlternativeSerialization(client, record);
            } else if (isAuthenticationError(throwable)) {
                logger.error("Authentication error, refreshing credentials", throwable);
                return refreshCredentialsAndRetry(client, record);
            } else {
                logger.error("Permanent error, sending to dead letter queue", throwable);
                return sendToDeadLetterQueue(record)
                    .thenCompose(dlqMetadata -> CompletableFuture.failedFuture(throwable));
            }
        })
        .thenCompose(Function.identity())
        .thenAccept(metadata -> {
            logger.info("Message successfully sent after recovery to partition {}", 
                       metadata.partition());
        })
        .exceptionally(throwable -> {
            logger.error("All recovery attempts failed", throwable);
            triggerAlert(record, throwable);
            return null;
        });
}
```

## Serialization

### JSON Serialization

```java
public void sendJsonMessages(KafkaMultiDatacenterClient client) {
    // Configure JSON serialization
    ProducerConfig jsonConfig = ProducerConfig.builder()
        .keySerializer("org.apache.kafka.common.serialization.StringSerializer")
        .valueSerializer("org.springframework.kafka.support.serializer.JsonSerializer")
        .customProperties(Map.of(
            "spring.json.type.mapping", "user:com.example.User,order:com.example.Order"
        ))
        .build();
    
    // Update client configuration if needed
    // Or create messages with proper JSON content
    
    User user = new User("john.doe", "John Doe", "john@example.com");
    
    ProducerRecord<String, User> record = new ProducerRecord<>(
        "json-topic",
        user.getUsername(),
        user
    );
    
    try {
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("JSON message sent to partition {}", metadata.partition());
    } catch (Exception e) {
        logger.error("Failed to send JSON message", e);
    }
}
```

### Avro Serialization

```java
public void sendAvroMessages(KafkaMultiDatacenterClient client) {
    // Avro serialization is configured through schema registry
    ProducerRecord<String, GenericRecord> record = createAvroRecord();
    
    try {
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("Avro message sent to partition {}", metadata.partition());
    } catch (Exception e) {
        logger.error("Failed to send Avro message", e);
    }
}

private ProducerRecord<String, GenericRecord> createAvroRecord() {
    // Create Avro schema
    String schemaString = """
        {
          "type": "record",
          "name": "UserEvent",
          "fields": [
            {"name": "userId", "type": "string"},
            {"name": "eventType", "type": "string"},
            {"name": "timestamp", "type": "long"}
          ]
        }
        """;
    
    Schema schema = new Schema.Parser().parse(schemaString);
    
    // Create Avro record
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("userId", "user123");
    avroRecord.put("eventType", "LOGIN");
    avroRecord.put("timestamp", Instant.now().toEpochMilli());
    
    return new ProducerRecord<>("avro-topic", "user123", avroRecord);
}
```

### Custom Serialization

```java
public class CustomObjectSerializer implements Serializer<CustomObject> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(String topic, CustomObject data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing CustomObject", e);
        }
    }
}

public void sendCustomSerializedMessages(KafkaMultiDatacenterClient client) {
    // Custom serialization would be configured in ProducerConfig
    CustomObject customObject = new CustomObject("data", Instant.now());
    
    ProducerRecord<String, CustomObject> record = new ProducerRecord<>(
        "custom-topic",
        "custom-key",
        customObject
    );
    
    try {
        RecordMetadata metadata = client.producerSync().send(record);
        logger.info("Custom serialized message sent to partition {}", metadata.partition());
    } catch (Exception e) {
        logger.error("Failed to send custom serialized message", e);
    }
}
```

## Transactions

### Basic Transactional Sending

```java
public void sendTransactionally(KafkaMultiDatacenterClient client) {
    // Begin transaction
    client.producerTransactional().beginTransaction();
    
    try {
        // Send multiple messages in transaction
        ProducerRecord<String, Object> record1 = new ProducerRecord<>(
            "txn-topic-1", "key1", Map.of("message", "Transactional message 1"));
        ProducerRecord<String, Object> record2 = new ProducerRecord<>(
            "txn-topic-2", "key2", Map.of("message", "Transactional message 2"));
        
        client.producerTransactional().send(record1);
        client.producerTransactional().send(record2);
        
        // Perform some business logic
        performBusinessLogic();
        
        // Commit transaction
        client.producerTransactional().commitTransaction();
        logger.info("Transaction committed successfully");
        
    } catch (Exception e) {
        // Abort transaction on error
        client.producerTransactional().abortTransaction();
        logger.error("Transaction aborted due to error", e);
        throw e;
    }
}
```

### Transactional Send with Consumer Offsets

```java
public void sendTransactionallyWithOffsets(KafkaMultiDatacenterClient client) {
    String consumerGroupId = "txn-consumer-group";
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = getCurrentOffsets();
    
    client.producerTransactional().beginTransaction();
    
    try {
        // Send messages
        List<ProducerRecord<String, Object>> records = createTransactionalRecords();
        for (ProducerRecord<String, Object> record : records) {
            client.producerTransactional().send(record);
        }
        
        // Send consumer offsets as part of transaction
        client.producerTransactional().sendOffsetsToTransaction(offsetsToCommit, consumerGroupId);
        
        // Commit transaction
        client.producerTransactional().commitTransaction();
        logger.info("Transactional send with offsets committed successfully");
        
    } catch (Exception e) {
        client.producerTransactional().abortTransaction();
        logger.error("Transactional send with offsets failed", e);
        throw e;
    }
}
```

## Performance Optimization

### High-Throughput Configuration

```java
public void configureHighThroughput() {
    ProducerConfig highThroughputConfig = ProducerConfig.builder()
        // Increase batch size for better throughput
        .batchSize(65536) // 64KB
        .bufferMemory(134217728L) // 128MB
        
        // Allow more time for batching
        .lingerMs(20)
        
        // Use efficient compression
        .compressionType("lz4")
        
        // Increase in-flight requests
        .maxInFlightRequestsPerConnection(5)
        
        // Optimize for throughput over durability
        .acks("1") // Leader acknowledgment only
        .retries(0) // No retries for maximum throughput
        
        .build();
}
```

### Low-Latency Configuration

```java
public void configureLowLatency() {
    ProducerConfig lowLatencyConfig = ProducerConfig.builder()
        // Minimize batching
        .batchSize(1)
        .lingerMs(0)
        
        // No compression to reduce CPU overhead
        .compressionType("none")
        
        // Serialize requests for consistency
        .maxInFlightRequestsPerConnection(1)
        
        // Quick acknowledgment
        .acks("1")
        .requestTimeoutMs(5000)
        
        .build();
}
```

### Memory Management

```java
public void optimizeMemoryUsage(KafkaMultiDatacenterClient client) {
    // Monitor producer memory usage
    ProducerMetrics metrics = client.getProducerMetrics();
    
    long bufferAvailable = metrics.getBufferAvailableBytes();
    long bufferTotal = metrics.getBufferTotalBytes();
    double bufferUtilization = (double) (bufferTotal - bufferAvailable) / bufferTotal;
    
    if (bufferUtilization > 0.8) {
        logger.warn("Producer buffer utilization high: {:.2f}%", bufferUtilization * 100);
        
        // Implement backpressure or flow control
        implementBackpressure();
    }
    
    // Monitor batch size efficiency
    double avgBatchSize = metrics.getAverageBatchSize();
    long configuredBatchSize = client.getProducerConfig().getBatchSize();
    
    if (avgBatchSize < configuredBatchSize * 0.5) {
        logger.info("Consider reducing batch size for better memory efficiency");
    }
}
```

## Monitoring and Metrics

### Producer Metrics

```java
public void monitorProducerMetrics(KafkaMultiDatacenterClient client) {
    ProducerMetrics metrics = client.getProducerMetrics();
    
    // Throughput metrics
    logger.info("Records sent rate: {} records/sec", metrics.getRecordSendRate());
    logger.info("Bytes sent rate: {} bytes/sec", metrics.getBytesSendRate());
    
    // Latency metrics
    logger.info("Average record send latency: {} ms", metrics.getAverageRecordSendLatency());
    logger.info("Max record send latency: {} ms", metrics.getMaxRecordSendLatency());
    
    // Error metrics
    logger.info("Record error rate: {} errors/sec", metrics.getRecordErrorRate());
    logger.info("Record retry rate: {} retries/sec", metrics.getRecordRetryRate());
    
    // Resource utilization
    logger.info("Buffer utilization: {:.2f}%", metrics.getBufferUtilization() * 100);
    logger.info("Connection count: {}", metrics.getConnectionCount());
    
    // Partition metrics
    Map<String, Double> partitionMetrics = metrics.getPartitionMetrics();
    partitionMetrics.forEach((partition, rate) -> 
        logger.info("Partition {} send rate: {} records/sec", partition, rate));
}
```

### Custom Metrics Collection

```java
@Component
public class ProducerMetricsCollector {
    
    private final MeterRegistry meterRegistry;
    private final Timer sendLatencyTimer;
    private final Counter successCounter;
    private final Counter errorCounter;
    
    public ProducerMetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.sendLatencyTimer = Timer.builder("kafka.producer.send.latency")
            .description("Producer send latency")
            .register(meterRegistry);
        this.successCounter = Counter.builder("kafka.producer.send.success")
            .description("Successful producer sends")
            .register(meterRegistry);
        this.errorCounter = Counter.builder("kafka.producer.send.error")
            .description("Failed producer sends")
            .register(meterRegistry);
    }
    
    public void recordSendLatency(Duration latency) {
        sendLatencyTimer.record(latency);
    }
    
    public void recordSuccess() {
        successCounter.increment();
    }
    
    public void recordError(String errorType) {
        errorCounter.increment(Tags.of("error.type", errorType));
    }
}
```

## Best Practices

### 1. **Configuration Best Practices**

```java
// Use appropriate acknowledgment levels
ProducerConfig.builder()
    .acks("all")           // Maximum durability
    // .acks("1")          // Good balance
    // .acks("0")          // Maximum throughput, no durability
    
    // Enable idempotence for exactly-once semantics
    .enableIdempotence(true)
    .retries(Integer.MAX_VALUE)
    
    // Configure appropriate timeouts
    .requestTimeoutMs(30000)
    .deliveryTimeoutMs(120000)
    
    .build();
```

### 2. **Error Handling Best Practices**

```java
// Always handle errors appropriately
CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);

future
    .thenAccept(metadata -> {
        // Success handling
        updateSuccessMetrics();
        logSuccessfulSend(metadata);
    })
    .exceptionally(throwable -> {
        // Error handling
        logError(throwable);
        
        if (isRetriableError(throwable)) {
            scheduleRetry(record);
        } else {
            sendToDeadLetterQueue(record);
        }
        
        return null;
    });
```

### 3. **Resource Management Best Practices**

```java
// Always use try-with-resources
try (KafkaMultiDatacenterClient client = createClient()) {
    sendMessages(client);
} // Client is automatically closed

// Monitor resource usage
void monitorResources(KafkaMultiDatacenterClient client) {
    ScheduledExecutorService monitor = Executors.newScheduledThreadPool(1);
    monitor.scheduleAtFixedRate(() -> {
        ProducerMetrics metrics = client.getProducerMetrics();
        if (metrics.getBufferUtilization() > 0.9) {
            logger.warn("High buffer utilization detected");
            implementBackpressure();
        }
    }, 0, 30, TimeUnit.SECONDS);
}
```

### 4. **Performance Best Practices**

```java
// Use appropriate batching
ProducerConfig.builder()
    .batchSize(32768)      // 32KB for good throughput
    .lingerMs(10)          // Wait up to 10ms for batching
    .compressionType("snappy") // Good compression ratio and speed
    .build();

// Use async sending for better throughput
List<CompletableFuture<RecordMetadata>> futures = records.stream()
    .map(record -> client.producerAsync().sendAsync(record))
    .collect(Collectors.toList());

CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
    .thenRun(() -> logger.info("All messages sent"));
```

### 5. **Monitoring Best Practices**

```java
// Implement comprehensive monitoring
@Scheduled(fixedRate = 60000) // Every minute
public void collectMetrics() {
    ProducerMetrics metrics = client.getProducerMetrics();
    
    // Alert on high error rates
    if (metrics.getRecordErrorRate() > 10) {
        alertingService.sendAlert("High producer error rate: " + metrics.getRecordErrorRate());
    }
    
    // Alert on high latency
    if (metrics.getAverageRecordSendLatency() > 5000) {
        alertingService.sendAlert("High producer latency: " + metrics.getAverageRecordSendLatency());
    }
    
    // Log key metrics
    logger.info("Producer metrics: send_rate={}, error_rate={}, latency_avg={}",
               metrics.getRecordSendRate(),
               metrics.getRecordErrorRate(),
               metrics.getAverageRecordSendLatency());
}
```

For more examples and detailed implementation, see the [examples directory](../lib/src/main/java/com/kafka/multidc/example/).
