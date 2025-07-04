# Consumer Guide

This guide provides comprehensive information on using the Kafka Multi-Datacenter Client Library's consumer capabilities across synchronous, asynchronous, and reactive programming models.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Synchronous Consumer Operations](#synchronous-consumer-operations)
- [Asynchronous Consumer Operations](#asynchronous-consumer-operations)
- [Reactive Consumer Operations](#reactive-consumer-operations)
- [Partitioning and Assignment Strategies](#partitioning-and-assignment-strategies)
- [Offset Management](#offset-management)
- [Error Handling and Resilience](#error-handling-and-resilience)
- [Deserialization](#deserialization)
- [Consumer Groups](#consumer-groups)
- [Performance Optimization](#performance-optimization)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Best Practices](#best-practices)

## Overview

The Kafka Multi-Datacenter Client Library provides three programming models for consuming messages:

- **Synchronous**: Traditional blocking operations for simple use cases
- **Asynchronous**: CompletableFuture-based non-blocking operations  
- **Reactive**: Reactive Streams (Project Reactor) for high-throughput scenarios

All consumer operations support:

- Multi-datacenter routing and failover
- Advanced partitioning and assignment strategies
- Comprehensive error handling and resilience patterns
- Rich deserialization options
- Advanced offset management
- Extensive monitoring and metrics

## Getting Started

### Basic Consumer Setup

```java
// Create configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(KafkaDatacenterEndpoint.builder()
        .id("primary")
        .region("us-east-1")
        .bootstrapServers("localhost:9092")
        .build())
    .localDatacenter("primary")
    .consumerConfig(ConsumerConfig.defaultConfig())
    .build();

// Create client
try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
    .configuration(config)
    .build()) {
    
    // Use consumer operations
    consumeMessages(client);
}
```

### Consumer Configuration

```java
// Use default consumer configuration
ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();

// For custom consumer settings, configure them in the main configuration
KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(endpoint)
    .consumerConfig(consumerConfig)
    .additionalProperties(Map.of(
        // Required settings
        "group.id", "my-consumer-group",
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer", "org.apache.kafka.common.serialization.JsonDeserializer",
        
        // Offset management
        "auto.offset.reset", "earliest",
        "enable.auto.commit", false, // Manual commit for better control
        
        // Performance settings
        "max.poll.records", 500,
        "fetch.min.bytes", 1024,
        "fetch.max.wait.ms", 500,
        
        // Session management
        "session.timeout.ms", 30000,
        "heartbeat.interval.ms", 3000
    ))
    .build();
```

## Synchronous Consumer Operations

### Basic Synchronous Consumption

```java
public void consumeSynchronously(KafkaMultiDatacenterClient client) {
    try {
        // Subscribe to topics
        client.consumerSync().subscribe(List.of("my-topic"));
        
        while (true) {
            // Poll for records
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                logger.info("Consumed message: key={}, value={}, partition={}, offset={}", 
                           record.key(), record.value(), record.partition(), record.offset());
                
                // Process the message
                processMessage(record);
            }
            
            // Manually commit offsets
            if (!records.isEmpty()) {
                client.consumerSync().commitSync();
                logger.info("Committed offsets for {} records", records.count());
            }
        }
    } catch (Exception e) {
        logger.error("Consumer error", e);
    }
}
```

### Synchronous Consumption with Manual Partition Assignment

```java
public void consumeWithManualAssignment(KafkaMultiDatacenterClient client) {
    try {
        // Manual partition assignment
        List<TopicPartition> partitions = List.of(
            new TopicPartition("my-topic", 0),
            new TopicPartition("my-topic", 1)
        );
        
        client.consumerSync().assign(partitions);
        
        // Seek to specific offsets if needed
        client.consumerSync().seekToBeginning(partitions);
        
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                logger.info("Manual assignment - consumed: partition={}, offset={}", 
                           record.partition(), record.offset());
                
                processMessage(record);
                
                // Commit individual partition offsets
                Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)
                );
                client.consumerSync().commitSync(offsets);
            }
        }
    } catch (Exception e) {
        logger.error("Manual assignment consumer error", e);
    }
}
```

### Synchronous Consumption with Timeout

```java
public void consumeWithTimeout(KafkaMultiDatacenterClient client) {
    client.consumerSync().subscribe(List.of("timeout-topic"));
    
    long startTime = System.currentTimeMillis();
    long timeoutMs = 30000; // 30 seconds
    
    try {
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            if (records.isEmpty()) {
                continue;
            }
            
            for (ConsumerRecord<String, Object> record : records) {
                logger.info("Timeout consumer - consumed: {}", record.value());
                processMessage(record);
            }
            
            client.consumerSync().commitSync();
        }
        
        logger.info("Consumer timeout reached, stopping consumption");
    } catch (Exception e) {
        logger.error("Timeout consumer error", e);
    }
}
```

## Asynchronous Consumer Operations

### Basic Asynchronous Consumption

```java
public void consumeAsynchronously(KafkaMultiDatacenterClient client) {
    // Subscribe to topics
    CompletableFuture<Void> subscriptionFuture = client.consumerAsync().subscribeAsync(List.of("async-topic"));
    
    subscriptionFuture
        .thenCompose(v -> startAsyncPolling(client))
        .exceptionally(throwable -> {
            logger.error("Async consumer error", throwable);
            return null;
        });
}

private CompletableFuture<Void> startAsyncPolling(KafkaMultiDatacenterClient client) {
    return CompletableFuture.runAsync(() -> {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                CompletableFuture<ConsumerRecords<String, Object>> pollFuture = 
                    client.consumerAsync().pollAsync(Duration.ofMillis(1000));
                
                ConsumerRecords<String, Object> records = pollFuture.get();
                
                if (!records.isEmpty()) {
                    // Process records asynchronously
                    List<CompletableFuture<Void>> processingFutures = new ArrayList<>();
                    
                    for (ConsumerRecord<String, Object> record : records) {
                        CompletableFuture<Void> processingFuture = CompletableFuture
                            .supplyAsync(() -> {
                                processMessage(record);
                                return null;
                            });
                        processingFutures.add(processingFuture);
                    }
                    
                    // Wait for all processing to complete
                    CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]))
                        .thenCompose(v -> client.consumerAsync().commitAsync())
                        .thenRun(() -> logger.info("Async committed offsets for {} records", records.count()))
                        .exceptionally(commitThrowable -> {
                            logger.error("Failed to commit offsets", commitThrowable);
                            return null;
                        });
                }
            }
        } catch (Exception e) {
            logger.error("Async polling error", e);
        }
    });
}
```

### Asynchronous Consumption with Parallel Processing

```java
public void consumeAsyncWithParallelProcessing(KafkaMultiDatacenterClient client) {
    client.consumerAsync().subscribeAsync(List.of("parallel-topic"))
        .thenCompose(v -> startParallelProcessing(client))
        .exceptionally(throwable -> {
            logger.error("Parallel consumer error", throwable);
            return null;
        });
}

private CompletableFuture<Void> startParallelProcessing(KafkaMultiDatacenterClient client) {
    ExecutorService processingExecutor = Executors.newFixedThreadPool(10);
    
    return CompletableFuture.runAsync(() -> {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                client.consumerAsync().pollAsync(Duration.ofMillis(1000))
                    .thenCompose(records -> {
                        if (records.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        
                        // Group records by partition to maintain ordering within partitions
                        Map<TopicPartition, List<ConsumerRecord<String, Object>>> partitionRecords = 
                            records.records().stream()
                                .collect(Collectors.groupingBy(
                                    record -> new TopicPartition(record.topic(), record.partition())
                                ));
                        
                        // Process each partition sequentially, but partitions in parallel
                        List<CompletableFuture<Void>> partitionFutures = partitionRecords.entrySet()
                            .stream()
                            .map(entry -> CompletableFuture.runAsync(() -> {
                                entry.getValue().forEach(this::processMessage);
                            }, processingExecutor))
                            .collect(Collectors.toList());
                        
                        return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                            .thenCompose(v -> client.consumerAsync().commitAsync());
                    })
                    .exceptionally(throwable -> {
                        logger.error("Parallel processing error", throwable);
                        return null;
                    });
            }
        } catch (Exception e) {
            logger.error("Parallel consumer error", e);
        }
    });
}
```

### Asynchronous Consumption with Error Handling

```java
public void consumeAsyncWithErrorHandling(KafkaMultiDatacenterClient client) {
    client.consumerAsync().subscribeAsync(List.of("error-handling-topic"))
        .thenCompose(v -> startAsyncConsumptionWithRetry(client))
        .exceptionally(throwable -> {
            logger.error("Consumer with error handling failed", throwable);
            return null;
        });
}

private CompletableFuture<Void> startAsyncConsumptionWithRetry(KafkaMultiDatacenterClient client) {
    return CompletableFuture.runAsync(() -> {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                client.consumerAsync().pollAsync(Duration.ofMillis(1000))
                    .thenCompose(records -> processRecordsWithErrorHandling(client, records))
                    .exceptionally(throwable -> {
                        if (isRetriableError(throwable)) {
                            logger.warn("Retriable error, will retry: {}", throwable.getMessage());
                            // Implement backoff and retry logic
                            scheduleRetry(client);
                        } else {
                            logger.error("Non-retriable error", throwable);
                            handlePermanentError(throwable);
                        }
                        return null;
                    });
            }
        } catch (Exception e) {
            logger.error("Async consumption with error handling failed", e);
        }
    });
}

private CompletableFuture<Void> processRecordsWithErrorHandling(
    KafkaMultiDatacenterClient client, 
    ConsumerRecords<String, Object> records) {
    
    if (records.isEmpty()) {
        return CompletableFuture.completedFuture(null);
    }
    
    List<CompletableFuture<Void>> processingFutures = new ArrayList<>();
    
    for (ConsumerRecord<String, Object> record : records) {
        CompletableFuture<Void> processingFuture = CompletableFuture
            .supplyAsync(() -> {
                try {
                    processMessage(record);
                    return null;
                } catch (Exception e) {
                    logger.error("Failed to process record: {}", record.key(), e);
                    sendToDeadLetterQueue(record, e);
                    return null;
                }
            });
        processingFutures.add(processingFuture);
    }
    
    return CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]))
        .thenCompose(v -> client.consumerAsync().commitAsync());
}
```

## Reactive Consumer Operations

### Basic Reactive Consumption

```java
public void consumeReactively(KafkaMultiDatacenterClient client) {
    // Create reactive consumer stream
    Flux<ConsumerRecord<String, Object>> recordStream = client.consumerReactive()
        .consume(List.of("reactive-topic"));
    
    recordStream
        .doOnNext(record -> {
            logger.info("Reactive consumer - received: key={}, partition={}, offset={}", 
                       record.key(), record.partition(), record.offset());
        })
        .map(this::processMessage)
        .doOnNext(processedRecord -> {
            logger.info("Processed record: {}", processedRecord.key());
        })
        .doOnError(throwable -> {
            logger.error("Reactive consumer error", throwable);
        })
        .doOnComplete(() -> {
            logger.info("Reactive consumer completed");
        })
        .subscribe();
}
```

### Reactive Consumption with Batching

```java
public void consumeReactivelyWithBatching(KafkaMultiDatacenterClient client) {
    Flux<ConsumerRecord<String, Object>> recordStream = client.consumerReactive()
        .consume(List.of("batch-topic"));
    
    recordStream
        .buffer(Duration.ofSeconds(5), 100) // Batch by time (5s) or size (100 records)
        .flatMap(batch -> {
            logger.info("Processing batch of {} records", batch.size());
            
            return Flux.fromIterable(batch)
                .flatMap(record -> processRecordReactively(record))
                .collectList()
                .doOnNext(results -> {
                    logger.info("Batch processing completed: {} records processed", results.size());
                    // Commit offsets for the batch
                    commitBatchOffsets(client, batch);
                })
                .onErrorContinue((error, batch) -> {
                    logger.error("Batch processing failed", error);
                    handleBatchError(batch, error);
                });
        })
        .subscribe();
}

private Mono<ConsumerRecord<String, Object>> processRecordReactively(ConsumerRecord<String, Object> record) {
    return Mono.fromCallable(() -> {
        processMessage(record);
        return record;
    })
    .subscribeOn(Schedulers.boundedElastic())
    .onErrorResume(throwable -> {
        logger.error("Failed to process record reactively: {}", record.key(), throwable);
        return sendToDeadLetterQueueReactive(record, throwable)
            .thenReturn(record);
    });
}
```

### Reactive Consumption with Backpressure

```java
public void consumeReactivelyWithBackpressure(KafkaMultiDatacenterClient client) {
    Flux<ConsumerRecord<String, Object>> recordStream = client.consumerReactive()
        .consume(List.of("backpressure-topic"));
    
    recordStream
        .onBackpressureBuffer(1000) // Buffer up to 1000 records
        .flatMap(record -> 
            processRecordReactively(record)
                .delayElement(Duration.ofMillis(10)), // Simulate processing time
            10 // Concurrency limit
        )
        .doOnNext(record -> {
            logger.debug("Processed with backpressure control: {}", record.key());
        })
        .onErrorContinue((error, record) -> {
            logger.error("Error processing record with backpressure", error);
        })
        .subscribe();
}
```

### Reactive Consumption with Flow Control

```java
public void consumeReactivelyWithFlowControl(KafkaMultiDatacenterClient client) {
    AtomicInteger processingCount = new AtomicInteger(0);
    int maxConcurrentProcessing = 50;
    
    Flux<ConsumerRecord<String, Object>> recordStream = client.consumerReactive()
        .consume(List.of("flow-control-topic"));
    
    recordStream
        .filter(record -> {
            // Flow control - only process if under the limit
            if (processingCount.get() >= maxConcurrentProcessing) {
                logger.warn("Flow control activated - delaying record processing");
                return false;
            }
            return true;
        })
        .flatMap(record -> {
            processingCount.incrementAndGet();
            
            return processRecordReactively(record)
                .doFinally(signalType -> processingCount.decrementAndGet());
        })
        .subscribe();
}
```

## Partitioning and Assignment Strategies

### Using Built-in Assignment Strategies

```java
public void demonstrateAssignmentStrategies(KafkaMultiDatacenterClient client) {
    // Datacenter-aware range assignment
    ConsumerConfig datacenterAwareConfig = ConsumerConfig.builder()
        .groupId("datacenter-aware-group")
        .partitionAssignmentStrategy(List.of(
            "com.kafka.multidc.consumer.DatacenterAwareRangeAssignor"
        ))
        .build();
    
    // Load-balanced assignment
    ConsumerConfig loadBalancedConfig = ConsumerConfig.builder()
        .groupId("load-balanced-group")
        .partitionAssignmentStrategy(List.of(
            "com.kafka.multidc.consumer.LoadBalancedAssignor"
        ))
        .build();
    
    // Sticky assignment for minimal rebalancing
    ConsumerConfig stickyConfig = ConsumerConfig.builder()
        .groupId("sticky-group")
        .partitionAssignmentStrategy(List.of(
            "org.apache.kafka.clients.consumer.StickyAssignor"
        ))
        .build();
}
```

### Custom Partition Assignment Strategy

```java
public class CustomDatacenterAssignor implements ConsumerPartitionAssignor {
    
    @Override
    public Map<String, List<TopicPartition>> assign(
        Map<String, Integer> partitionsPerTopic,
        Map<String, Subscription> subscriptions) {
        
        Map<String, List<TopicPartition>> assignments = new HashMap<>();
        
        // Get datacenter information for each consumer
        Map<String, String> consumerDatacenters = extractDatacenterInfo(subscriptions);
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int numPartitions = topicEntry.getValue();
            
            // Create partitions list
            List<TopicPartition> partitions = IntStream.range(0, numPartitions)
                .mapToObj(i -> new TopicPartition(topic, i))
                .collect(Collectors.toList());
            
            // Assign partitions based on datacenter locality
            assignPartitionsByDatacenter(partitions, consumerDatacenters, assignments);
        }
        
        return assignments;
    }
    
    private void assignPartitionsByDatacenter(
        List<TopicPartition> partitions,
        Map<String, String> consumerDatacenters,
        Map<String, List<TopicPartition>> assignments) {
        
        // Group consumers by datacenter
        Map<String, List<String>> datacenterConsumers = consumerDatacenters.entrySet()
            .stream()
            .collect(Collectors.groupingBy(
                Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toList())
            ));
        
        // Distribute partitions evenly within each datacenter
        int partitionIndex = 0;
        for (Map.Entry<String, List<String>> dcEntry : datacenterConsumers.entrySet()) {
            List<String> consumers = dcEntry.getValue();
            int partitionsPerDC = partitions.size() / datacenterConsumers.size();
            
            for (int i = 0; i < partitionsPerDC && partitionIndex < partitions.size(); i++) {
                String consumer = consumers.get(i % consumers.size());
                assignments.computeIfAbsent(consumer, k -> new ArrayList<>())
                    .add(partitions.get(partitionIndex++));
            }
        }
    }
    
    @Override
    public String name() {
        return "datacenter-locality";
    }
}
```

### Partition-Aware Consumption

```java
public void consumeWithPartitionAwareness(KafkaMultiDatacenterClient client) {
    client.consumerSync().subscribe(List.of("partition-aware-topic"));
    
    // Track partition-specific metrics
    Map<Integer, PartitionMetrics> partitionMetrics = new ConcurrentHashMap<>();
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            // Process records by partition
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
                
                PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
                    partition.partition(), 
                    k -> new PartitionMetrics()
                );
                
                logger.info("Processing {} records from partition {}", 
                           partitionRecords.size(), partition.partition());
                
                long startTime = System.currentTimeMillis();
                
                for (ConsumerRecord<String, Object> record : partitionRecords) {
                    processMessage(record);
                    metrics.incrementProcessedCount();
                }
                
                long processingTime = System.currentTimeMillis() - startTime;
                metrics.addProcessingTime(processingTime);
                
                logger.info("Partition {} metrics: processed={}, avg_time={}ms", 
                           partition.partition(), 
                           metrics.getProcessedCount(),
                           metrics.getAverageProcessingTime());
            }
            
            if (!records.isEmpty()) {
                client.consumerSync().commitSync();
            }
        }
    } catch (Exception e) {
        logger.error("Partition-aware consumer error", e);
    }
}
```

## Offset Management

### Manual Offset Management

```java
public void manualOffsetManagement(KafkaMultiDatacenterClient client) {
    client.consumerSync().subscribe(List.of("manual-offset-topic"));
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            
            for (ConsumerRecord<String, Object> record : records) {
                try {
                    processMessage(record);
                    
                    // Track successful processing
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    
                } catch (Exception e) {
                    logger.error("Failed to process record, skipping offset commit: {}", 
                               record.key(), e);
                    // Don't commit offset for failed records
                }
            }
            
            // Commit only successfully processed offsets
            if (!offsetsToCommit.isEmpty()) {
                client.consumerSync().commitSync(offsetsToCommit);
                logger.info("Committed offsets for {} partitions", offsetsToCommit.size());
            }
        }
    } catch (Exception e) {
        logger.error("Manual offset management error", e);
    }
}
```

### Offset Storage and Recovery

```java
public void offsetStorageAndRecovery(KafkaMultiDatacenterClient client) {
    String topicName = "offset-recovery-topic";
    client.consumerSync().subscribe(List.of(topicName));
    
    // Custom offset storage
    OffsetStorage offsetStorage = new DatabaseOffsetStorage();
    
    try {
        // Restore offsets from custom storage
        Map<TopicPartition, Long> storedOffsets = offsetStorage.loadOffsets();
        for (Map.Entry<TopicPartition, Long> entry : storedOffsets.entrySet()) {
            client.consumerSync().seek(entry.getKey(), entry.getValue());
            logger.info("Restored offset for {}: {}", entry.getKey(), entry.getValue());
        }
        
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                processMessage(record);
                
                // Store offset in custom storage
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                offsetStorage.storeOffset(tp, record.offset() + 1);
            }
            
            if (!records.isEmpty()) {
                // Also commit to Kafka for compatibility
                client.consumerSync().commitSync();
                
                // Periodically sync custom storage
                offsetStorage.sync();
            }
        }
    } catch (Exception e) {
        logger.error("Offset storage and recovery error", e);
    }
}
```

### Transactional Offset Management

```java
public void transactionalOffsetManagement(KafkaMultiDatacenterClient client) {
    client.consumerSync().subscribe(List.of("transactional-offset-topic"));
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            if (!records.isEmpty()) {
                // Begin transaction
                client.producerTransactional().beginTransaction();
                
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    
                    for (ConsumerRecord<String, Object> record : records) {
                        // Process message
                        Object processedResult = processMessage(record);
                        
                        // Send result to output topic within transaction
                        ProducerRecord<String, Object> outputRecord = new ProducerRecord<>(
                            "processed-output-topic",
                            record.key(),
                            processedResult
                        );
                        client.producerTransactional().send(outputRecord);
                        
                        // Track offset to commit
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    }
                    
                    // Send offsets to transaction
                    client.producerTransactional().sendOffsetsToTransaction(
                        offsetsToCommit, 
                        client.getConsumerConfig().getGroupId()
                    );
                    
                    // Commit transaction
                    client.producerTransactional().commitTransaction();
                    logger.info("Transactional processing completed for {} records", records.count());
                    
                } catch (Exception e) {
                    // Abort transaction on error
                    client.producerTransactional().abortTransaction();
                    logger.error("Transaction aborted due to processing error", e);
                    throw e;
                }
            }
        }
    } catch (Exception e) {
        logger.error("Transactional offset management error", e);
    }
}
```

## Error Handling and Resilience

### Error Classification and Handling

```java
public void handleErrorsWithClassification(KafkaMultiDatacenterClient client) {
    client.consumerSync().subscribe(List.of("error-classification-topic"));
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                try {
                    processMessage(record);
                } catch (Exception e) {
                    handleProcessingError(record, e);
                }
            }
            
            if (!records.isEmpty()) {
                client.consumerSync().commitSync();
            }
        }
    } catch (Exception e) {
        logger.error("Consumer error classification failed", e);
    }
}

private void handleProcessingError(ConsumerRecord<String, Object> record, Exception error) {
    if (isRetriableError(error)) {
        logger.warn("Retriable error processing record {}: {}", record.key(), error.getMessage());
        
        // Add to retry queue with backoff
        scheduleRetry(record, calculateBackoff(record));
        
    } else if (isDeserializationError(error)) {
        logger.error("Deserialization error for record {}: {}", record.key(), error.getMessage());
        
        // Send to dead letter queue with original bytes
        sendToDeadLetterQueue(record, error);
        
    } else if (isBusinessLogicError(error)) {
        logger.error("Business logic error processing record {}: {}", record.key(), error.getMessage());
        
        // Send to error topic for manual review
        sendToErrorTopic(record, error);
        
    } else {
        logger.error("Unknown error processing record {}: {}", record.key(), error.getMessage());
        
        // Send to general error handling
        sendToGeneralErrorHandler(record, error);
    }
}
```

### Dead Letter Queue Implementation

```java
public void implementDeadLetterQueue(KafkaMultiDatacenterClient client) {
    String mainTopic = "main-processing-topic";
    String dlqTopic = "main-processing-topic-dlq";
    
    client.consumerSync().subscribe(List.of(mainTopic));
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                try {
                    // Attempt processing
                    processMessage(record);
                    
                } catch (Exception e) {
                    logger.error("Processing failed for record {}, sending to DLQ", record.key(), e);
                    
                    // Create DLQ record with error information
                    Map<String, Object> dlqPayload = Map.of(
                        "originalTopic", record.topic(),
                        "originalPartition", record.partition(),
                        "originalOffset", record.offset(),
                        "originalKey", record.key(),
                        "originalValue", record.value(),
                        "errorMessage", e.getMessage(),
                        "errorClass", e.getClass().getName(),
                        "timestamp", Instant.now().toString(),
                        "retryCount", getRetryCount(record)
                    );
                    
                    ProducerRecord<String, Object> dlqRecord = new ProducerRecord<>(
                        dlqTopic,
                        record.key(),
                        dlqPayload
                    );
                    
                    // Add error headers
                    dlqRecord.headers().add(new RecordHeader("error.class", 
                        e.getClass().getName().getBytes(StandardCharsets.UTF_8)));
                    dlqRecord.headers().add(new RecordHeader("error.message", 
                        e.getMessage().getBytes(StandardCharsets.UTF_8)));
                    dlqRecord.headers().add(new RecordHeader("error.timestamp", 
                        Instant.now().toString().getBytes(StandardCharsets.UTF_8)));
                    
                    try {
                        client.producerSync().send(dlqRecord);
                        logger.info("Sent record {} to DLQ", record.key());
                    } catch (Exception dlqError) {
                        logger.error("Failed to send record to DLQ", dlqError);
                        // Could implement local DLQ storage or alerting
                    }
                }
            }
            
            if (!records.isEmpty()) {
                client.consumerSync().commitSync();
            }
        }
    } catch (Exception e) {
        logger.error("DLQ consumer error", e);
    }
}
```

### Retry Mechanism with Exponential Backoff

```java
public void implementRetryMechanism(KafkaMultiDatacenterClient client) {
    String mainTopic = "retry-mechanism-topic";
    String retryTopic = "retry-mechanism-topic-retry";
    
    client.consumerSync().subscribe(List.of(mainTopic, retryTopic));
    
    try {
        while (true) {
            ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, Object> record : records) {
                try {
                    processMessage(record);
                    
                } catch (Exception e) {
                    if (isRetriableError(e)) {
                        handleRetryableError(client, record, e);
                    } else {
                        sendToDeadLetterQueue(record, e);
                    }
                }
            }
            
            if (!records.isEmpty()) {
                client.consumerSync().commitSync();
            }
        }
    } catch (Exception e) {
        logger.error("Retry mechanism consumer error", e);
    }
}

private void handleRetryableError(KafkaMultiDatacenterClient client, 
                                 ConsumerRecord<String, Object> record, 
                                 Exception error) {
    int retryCount = getRetryCount(record);
    int maxRetries = 3;
    
    if (retryCount >= maxRetries) {
        logger.error("Max retries exceeded for record {}, sending to DLQ", record.key());
        sendToDeadLetterQueue(record, error);
        return;
    }
    
    // Calculate exponential backoff delay
    long delayMs = calculateExponentialBackoff(retryCount);
    
    // Create retry record
    Map<String, Object> retryPayload = Map.of(
        "originalTopic", record.topic(),
        "originalKey", record.key(),
        "originalValue", record.value(),
        "retryCount", retryCount + 1,
        "errorMessage", error.getMessage(),
        "retryAfter", Instant.now().plusMillis(delayMs).toString()
    );
    
    ProducerRecord<String, Object> retryRecord = new ProducerRecord<>(
        "retry-mechanism-topic-retry",
        record.key(),
        retryPayload
    );
    
    // Add retry headers
    retryRecord.headers().add(new RecordHeader("retry.count", 
        String.valueOf(retryCount + 1).getBytes(StandardCharsets.UTF_8)));
    retryRecord.headers().add(new RecordHeader("retry.delay.ms", 
        String.valueOf(delayMs).getBytes(StandardCharsets.UTF_8)));
    
    try {
        client.producerSync().send(retryRecord);
        logger.info("Sent record {} to retry topic with delay {}ms", record.key(), delayMs);
    } catch (Exception retryError) {
        logger.error("Failed to send record to retry topic", retryError);
        sendToDeadLetterQueue(record, error);
    }
}

private long calculateExponentialBackoff(int retryCount) {
    long baseDelayMs = 1000; // 1 second
    double multiplier = 2.0;
    long maxDelayMs = 300000; // 5 minutes
    
    long delay = (long) (baseDelayMs * Math.pow(multiplier, retryCount));
    return Math.min(delay, maxDelayMs);
}
```

## Performance Optimization

### High-Throughput Configuration

```java
public void configureHighThroughput() {
    ConsumerConfig highThroughputConfig = ConsumerConfig.builder()
        // Increase fetch sizes
        .maxPollRecords(2000)
        .fetchMinBytes(100000) // 100KB
        .fetchMaxBytes(104857600) // 100MB
        .fetchMaxWaitMs(100)
        
        // Optimize session management
        .sessionTimeoutMs(45000)
        .heartbeatIntervalMs(15000)
        .maxPollIntervalMs(600000) // 10 minutes
        
        // Auto-commit for throughput
        .enableAutoCommit(true)
        .autoCommitIntervalMs(1000)
        
        .build();
}
```

### Low-Latency Configuration

```java
public void configureLowLatency() {
    ConsumerConfig lowLatencyConfig = ConsumerConfig.builder()
        // Minimize fetch sizes
        .maxPollRecords(1)
        .fetchMinBytes(1)
        .fetchMaxWaitMs(1)
        
        // Quick session management
        .sessionTimeoutMs(6000)
        .heartbeatIntervalMs(2000)
        
        // Manual commit for control
        .enableAutoCommit(false)
        
        .build();
}
```

### Memory Optimization

```java
public void optimizeMemoryUsage(KafkaMultiDatacenterClient client) {
    // Monitor consumer memory usage
    ConsumerMetrics metrics = client.getConsumerMetrics();
    
    long memoryUsed = metrics.getMemoryUsageBytes();
    long maxMemory = Runtime.getRuntime().maxMemory();
    double memoryUtilization = (double) memoryUsed / maxMemory;
    
    if (memoryUtilization > 0.8) {
        logger.warn("High memory utilization: {:.2f}%", memoryUtilization * 100);
        
        // Reduce batch sizes
        adjustFetchSizes(client);
        
        // Force garbage collection
        System.gc();
    }
    
    // Monitor fetch buffer usage
    long fetchBufferUsed = metrics.getFetchBufferUsageBytes();
    long fetchBufferSize = client.getConsumerConfig().getFetchMaxBytes();
    
    if (fetchBufferUsed > fetchBufferSize * 0.9) {
        logger.warn("Fetch buffer nearly full, consider increasing fetch.max.bytes");
    }
}
```

## Monitoring and Metrics

### Consumer Metrics

```java
public void monitorConsumerMetrics(KafkaMultiDatacenterClient client) {
    ConsumerMetrics metrics = client.getConsumerMetrics();
    
    // Consumption metrics
    logger.info("Records consumed rate: {} records/sec", metrics.getRecordsConsumedRate());
    logger.info("Bytes consumed rate: {} bytes/sec", metrics.getBytesConsumedRate());
    
    // Latency metrics
    logger.info("Average fetch latency: {} ms", metrics.getAverageFetchLatency());
    logger.info("Max fetch latency: {} ms", metrics.getMaxFetchLatency());
    
    // Offset metrics
    logger.info("Consumer lag: {} records", metrics.getConsumerLag());
    logger.info("Offset commit rate: {} commits/sec", metrics.getOffsetCommitRate());
    
    // Partition metrics
    Map<TopicPartition, ConsumerPartitionMetrics> partitionMetrics = metrics.getPartitionMetrics();
    partitionMetrics.forEach((partition, partitionMetric) -> {
        logger.info("Partition {}: lag={}, rate={} records/sec", 
                   partition, partitionMetric.getLag(), partitionMetric.getConsumeRate());
    });
}
```

### Custom Metrics Collection

```java
@Component
public class ConsumerMetricsCollector {
    
    private final MeterRegistry meterRegistry;
    private final Timer processingLatencyTimer;
    private final Counter recordsProcessedCounter;
    private final Gauge consumerLagGauge;
    
    public ConsumerMetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.processingLatencyTimer = Timer.builder("kafka.consumer.processing.latency")
            .description("Consumer message processing latency")
            .register(meterRegistry);
        this.recordsProcessedCounter = Counter.builder("kafka.consumer.records.processed")
            .description("Number of records processed")
            .register(meterRegistry);
        this.consumerLagGauge = Gauge.builder("kafka.consumer.lag")
            .description("Consumer lag")
            .register(meterRegistry, this, ConsumerMetricsCollector::getCurrentLag);
    }
    
    public void recordProcessingLatency(Duration latency) {
        processingLatencyTimer.record(latency);
    }
    
    public void recordProcessedMessage() {
        recordsProcessedCounter.increment();
    }
    
    private double getCurrentLag() {
        // Implementation to get current consumer lag
        return 0.0; // Placeholder
    }
}
```

## Best Practices

### 1. **Configuration Best Practices**

```java
// Use appropriate consumer group coordination
ConsumerConfig.builder()
    .groupId("production-consumer-group")
    .sessionTimeoutMs(30000)        // Reasonable timeout
    .heartbeatIntervalMs(10000)     // 1/3 of session timeout
    .maxPollIntervalMs(300000)      // 5 minutes for processing
    
    // Control offset management
    .enableAutoCommit(false)        // Manual control for reliability
    .autoOffsetReset("latest")      // Safe default for production
    
    // Optimize for your use case
    .maxPollRecords(500)            // Balance between throughput and latency
    .fetchMinBytes(50000)           // Reasonable batch size
    
    .build();
```

### 2. **Error Handling Best Practices**

```java
// Comprehensive error handling
public void processRecord(ConsumerRecord<String, Object> record) {
    try {
        // Process the record
        handleMessage(record);
        
    } catch (RetriableException e) {
        // Retry with backoff
        scheduleRetry(record, e);
        
    } catch (DeserializationException e) {
        // Send to DLQ for manual inspection
        sendToDeadLetterQueue(record, e);
        
    } catch (BusinessLogicException e) {
        // Log and possibly send to error topic
        logger.error("Business logic error", e);
        sendToErrorTopic(record, e);
        
    } catch (Exception e) {
        // Catch-all for unknown errors
        logger.error("Unknown error processing record", e);
        sendToDeadLetterQueue(record, e);
    }
}
```

### 3. **Resource Management Best Practices**

```java
// Proper resource management
try (KafkaMultiDatacenterClient client = createClient()) {
    // Set up shutdown hook for graceful shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Shutting down consumer gracefully");
        client.close();
    }));
    
    consumeMessages(client);
    
} catch (Exception e) {
    logger.error("Consumer error", e);
}

// Monitor resource usage
@Scheduled(fixedRate = 60000)
public void monitorResources() {
    ConsumerMetrics metrics = client.getConsumerMetrics();
    
    if (metrics.getConsumerLag() > 10000) {
        logger.warn("High consumer lag detected: {}", metrics.getConsumerLag());
        alertingService.sendAlert("High consumer lag");
    }
}
```

### 4. **Performance Best Practices**

```java
// Optimize for your specific use case
public void optimizeForUseCase(String useCase) {
    switch (useCase) {
        case "high-throughput":
            // Large batches, auto-commit
            ConsumerConfig.builder()
                .maxPollRecords(2000)
                .fetchMinBytes(100000)
                .enableAutoCommit(true)
                .autoCommitIntervalMs(1000)
                .build();
            break;
            
        case "low-latency":
            // Small batches, immediate processing
            ConsumerConfig.builder()
                .maxPollRecords(1)
                .fetchMinBytes(1)
                .fetchMaxWaitMs(1)
                .enableAutoCommit(false)
                .build();
            break;
            
        case "exactly-once":
            // Transactional processing
            ConsumerConfig.builder()
                .isolationLevel("read_committed")
                .enableAutoCommit(false)
                .build();
            break;
    }
}
```

### 5. **Monitoring Best Practices**

```java
// Implement comprehensive monitoring
@Scheduled(fixedRate = 30000) // Every 30 seconds
public void collectMetrics() {
    ConsumerMetrics metrics = client.getConsumerMetrics();
    
    // Alert on concerning metrics
    if (metrics.getConsumerLag() > getAlertThreshold()) {
        alertingService.sendAlert("Consumer lag exceeded threshold: " + metrics.getConsumerLag());
    }
    
    if (metrics.getRecordsConsumedRate() < getMinimumThroughput()) {
        alertingService.sendAlert("Consumer throughput below minimum: " + metrics.getRecordsConsumedRate());
    }
    
    // Log key metrics
    logger.info("Consumer metrics: lag={}, rate={} records/sec, latency={}ms",
               metrics.getConsumerLag(),
               metrics.getRecordsConsumedRate(),
               metrics.getAverageFetchLatency());
}
```

For more examples and detailed implementation, see the [examples directory](../lib/src/main/java/com/kafka/multidc/example/).
