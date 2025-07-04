package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating comprehensive consumer operations with the Kafka Multi-Datacenter Client.
 * Shows synchronous, asynchronous, and reactive consumer patterns with real-world scenarios.
 */
public class ConsumerExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumerExample.class);
    private static final ScheduledExecutorService producerScheduler = Executors.newScheduledThreadPool(2);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Consumer Example ===");
        
        try {
            // Create configuration optimized for consumer operations
            KafkaDatacenterConfiguration config = createConsumerConfiguration();
            
            // Build the client with consumer capabilities
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Consumer client created successfully");
                
                // Start producing messages for consumption
                startMessageProduction(client);
                
                // Demonstrate synchronous consumer operations
                demonstrateSyncConsumer(client);
                
                // Demonstrate asynchronous consumer operations
                demonstrateAsyncConsumer(client);
                
                // Demonstrate reactive consumer operations
                demonstrateReactiveConsumer(client);
                
                // Demonstrate consumer patterns
                demonstrateConsumerPatterns(client);
                
                // Demonstrate partition-aware consumption
                demonstratePartitionAwareConsumption(client);
                
                // Run for a while to consume messages
                logger.info("Running consumer demo for 20 seconds...");
                Thread.sleep(20000);
                
                logger.info("Consumer example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Consumer example failed", e);
        } finally {
            producerScheduler.shutdown();
        }
    }
    
    /**
     * Create configuration optimized for consumer operations.
     */
    private static KafkaDatacenterConfiguration createConsumerConfiguration() {
        logger.info("Creating consumer configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .datacenters(List.of(
                KafkaDatacenterEndpoint.builder()
                    .id("consumer-primary")
                    .region("us-east-1")
                    .bootstrapServers("kafka-consumer-1.company.com:9092,kafka-consumer-2.company.com:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(20)
                    .minConnections(5)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("consumer-secondary")
                    .region("us-west-2")
                    .bootstrapServers("kafka-consumer-west-1.company.com:9092,kafka-consumer-west-2.company.com:9092")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(15)
                    .minConnections(3)
                    .build()
            ))
            .localDatacenter("consumer-primary")
            .routingStrategy(RoutingStrategy.PRIMARY_PREFERRED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(15))
            .requestTimeout(Duration.ofMinutes(2))
            .enableMetrics(true)
            .metricsPrefix("consumer.kafka.multidc")
            .build();
    }
    
    /**
     * Start background message production for consumer testing.
     */
    private static void startMessageProduction(KafkaMultiDatacenterClient client) {
        logger.info("Starting background message production...");
        
        // Produce user events periodically
        producerScheduler.scheduleAtFixedRate(() -> {
            try {
                ProducerRecord<String, Object> userEvent = new ProducerRecord<>(
                    "consumer-user-events",
                    "user-" + System.currentTimeMillis(),
                    Map.of(
                        "userId", "user-" + (int)(Math.random() * 1000),
                        "eventType", Math.random() > 0.5 ? "LOGIN" : "LOGOUT",
                        "timestamp", Instant.now().toString(),
                        "source", "user-service"
                    )
                );
                
                client.producerSync().send(userEvent);
                
            } catch (Exception e) {
                logger.warn("Failed to produce user event: {}", e.getMessage());
            }
        }, 1, 2, TimeUnit.SECONDS);
        
        // Produce order events periodically
        producerScheduler.scheduleAtFixedRate(() -> {
            try {
                ProducerRecord<String, Object> orderEvent = new ProducerRecord<>(
                    "consumer-order-events",
                    "order-" + System.currentTimeMillis(),
                    Map.of(
                        "orderId", "order-" + (int)(Math.random() * 10000),
                        "customerId", "customer-" + (int)(Math.random() * 500),
                        "status", List.of("CREATED", "CONFIRMED", "SHIPPED", "DELIVERED").get((int)(Math.random() * 4)),
                        "amount", Math.round(Math.random() * 1000 * 100.0) / 100.0,
                        "timestamp", Instant.now().toString(),
                        "source", "order-service"
                    )
                );
                
                client.producerSync().send(orderEvent);
                
            } catch (Exception e) {
                logger.warn("Failed to produce order event: {}", e.getMessage());
            }
        }, 2, 3, TimeUnit.SECONDS);
    }
    
    /**
     * Demonstrate synchronous consumer operations.
     */
    private static void demonstrateSyncConsumer(KafkaMultiDatacenterClient client) {
        logger.info("=== Synchronous Consumer Demo ===");
        
        try {
            // Create a separate thread for sync consumer to avoid blocking
            Thread syncConsumerThread = new Thread(() -> {
                try {
                    logger.info("Starting synchronous consumer for user events...");
                    
                    // Subscribe to user events topic
                    client.consumerSync().subscribe(List.of("consumer-user-events"));
                    
                    // Poll for messages
                    for (int i = 0; i < 5; i++) {
                        ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(3));
                        
                        logger.info("Sync consumer polled {} records", records.count());
                        
                        for (ConsumerRecord<String, Object> record : records) {
                            logger.info("✅ Sync consumed: topic={}, partition={}, offset={}, key={}", 
                                       record.topic(), record.partition(), record.offset(), record.key());
                            
                            // Process the record
                            if (record.value() instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> eventData = (Map<String, Object>) record.value();
                                logger.info("   Event data: userId={}, eventType={}", 
                                           eventData.get("userId"), eventData.get("eventType"));
                            }
                        }
                        
                        // Commit after processing
                        client.consumerSync().commitSync();
                    }
                    
                    logger.info("✅ Synchronous consumer completed");
                    
                } catch (Exception e) {
                    logger.error("❌ Synchronous consumer failed", e);
                }
            });
            
            syncConsumerThread.setDaemon(true);
            syncConsumerThread.start();
            
            // Wait a bit for the consumer to process
            Thread.sleep(8000);
            
        } catch (Exception e) {
            logger.error("❌ Synchronous consumer demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate asynchronous consumer operations.
     */
    private static void demonstrateAsyncConsumer(KafkaMultiDatacenterClient client) {
        logger.info("=== Asynchronous Consumer Demo ===");
        
        try {
            logger.info("Starting asynchronous consumer for order events...");
            
            // Subscribe to order events asynchronously
            CompletableFuture<Void> subscriptionFuture = client.consumerAsync().subscribeAsync(List.of("consumer-order-events"));
            
            subscriptionFuture.thenRun(() -> {
                logger.info("Async consumer subscribed successfully");
                
                // Start async polling
                CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
                    try {
                        for (int i = 0; i < 4; i++) {
                            client.consumerAsync().pollAsync(Duration.ofSeconds(3))
                                .thenAccept(records -> {
                                    logger.info("Async consumer polled {} records", records.count());
                                    
                                    for (ConsumerRecord<Object, Object> record : records) {
                                        logger.info("✅ Async consumed: topic={}, partition={}, offset={}, key={}", 
                                                   record.topic(), record.partition(), record.offset(), record.key());
                                        
                                        // Process the record asynchronously
                                        if (record.value() instanceof Map) {
                                            @SuppressWarnings("unchecked")
                                            Map<String, Object> eventData = (Map<String, Object>) record.value();
                                            logger.info("   Order data: orderId={}, status={}, amount={}", 
                                                       eventData.get("orderId"), eventData.get("status"), eventData.get("amount"));
                                        }
                                    }
                                })
                                .exceptionally(error -> {
                                    logger.error("Async polling failed", error);
                                    return null;
                                });
                            
                            Thread.sleep(4000);
                        }
                    } catch (Exception e) {
                        logger.error("Async consumer polling failed", e);
                    }
                });
                
                pollingFuture.thenRun(() -> {
                    // Commit asynchronously
                    client.consumerAsync().commitAsync()
                        .thenRun(() -> logger.info("✅ Async consumer committed successfully"))
                        .exceptionally(error -> {
                            logger.error("Async commit failed", error);
                            return null;
                        });
                });
            }).exceptionally(error -> {
                logger.error("❌ Async consumer subscription failed", error);
                return null;
            });
            
        } catch (Exception e) {
            logger.error("❌ Asynchronous consumer demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate reactive consumer operations.
     */
    private static void demonstrateReactiveConsumer(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Consumer Demo ===");
        
        try {
            logger.info("Demonstrating reactive consumer concepts...");
            
            // Note: This demonstrates the reactive consumer API concepts
            // The actual implementation would provide reactive streams
            
            logger.info("Reactive consumer would provide:");
            logger.info("- Flux<ConsumerRecord<K,V>> for continuous streaming");
            logger.info("- Automatic backpressure handling");
            logger.info("- Non-blocking message processing pipelines");
            logger.info("- Reactive commit strategies");
            
            // Simulated reactive consumer behavior
            Thread reactiveConsumerThread = new Thread(() -> {
                try {
                    logger.info("🔄 Simulating reactive consumer stream...");
                    
                    // In a real implementation, this would look like:
                    // client.consumerReactive()
                    //   .subscribe(List.of("consumer-reactive-events"))
                    //   .map(record -> transformRecord(record))
                    //   .filter(transformed -> transformed.isValid())
                    //   .buffer(Duration.ofSeconds(2))
                    //   .flatMap(batch -> processBatchReactively(batch))
                    //   .subscribe(result -> handleResult(result));
                    
                    for (int i = 0; i < 6; i++) {
                        logger.info("📦 Reactive consumer would process batch {} with backpressure", i + 1);
                        Thread.sleep(2000);
                    }
                    
                    logger.info("✅ Reactive consumer simulation completed");
                    
                } catch (Exception e) {
                    logger.error("❌ Reactive consumer simulation failed", e);
                }
            });
            
            reactiveConsumerThread.setDaemon(true);
            reactiveConsumerThread.start();
            
        } catch (Exception e) {
            logger.error("❌ Reactive consumer demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate common consumer patterns and best practices.
     */
    private static void demonstrateConsumerPatterns(KafkaMultiDatacenterClient client) {
        logger.info("=== Consumer Patterns Demo ===");
        
        try {
            // Pattern 1: Manual offset management
            logger.info("Pattern 1: Manual offset management");
            demonstrateManualOffsetManagement(client);
            
            // Pattern 2: Error handling and retry
            logger.info("Pattern 2: Error handling and retry");
            demonstrateErrorHandling(client);
            
            // Pattern 3: Batch processing
            logger.info("Pattern 3: Batch processing");
            demonstrateBatchProcessing(client);
            
        } catch (Exception e) {
            logger.error("❌ Consumer patterns demonstration failed", e);
        }
    }
    
    private static void demonstrateManualOffsetManagement(KafkaMultiDatacenterClient client) {
        try {
            logger.info("🎯 Manual offset management allows precise control over message processing");
            
            // Simulate manual offset management
            Thread offsetThread = new Thread(() -> {
                try {
                    // Subscribe with manual commit
                    client.consumerSync().subscribe(List.of("consumer-user-events"));
                    
                    ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(2));
                    
                    for (ConsumerRecord<String, Object> record : records) {
                        try {
                            // Process record
                            logger.info("Processing record: partition={}, offset={}", 
                                       record.partition(), record.offset());
                            
                            // Simulate processing
                            Thread.sleep(100);
                            
                            // Manually commit this specific record
                            // In real implementation: client.consumerSync().commitOffset(record.topic(), record.partition(), record.offset() + 1);
                            logger.info("✅ Manually committed offset {}", record.offset());
                            
                        } catch (Exception e) {
                            logger.warn("Failed to process record at offset {}: {}", record.offset(), e.getMessage());
                            // Don't commit on failure - message will be reprocessed
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Manual offset management failed", e);
                }
            });
            
            offsetThread.setDaemon(true);
            offsetThread.start();
            offsetThread.join(3000);
            
        } catch (Exception e) {
            logger.error("Manual offset management demonstration failed", e);
        }
    }
    
    private static void demonstrateErrorHandling(KafkaMultiDatacenterClient client) {
        try {
            logger.info("🔧 Error handling ensures reliable message processing");
            
            Thread errorHandlingThread = new Thread(() -> {
                try {
                    client.consumerSync().subscribe(List.of("consumer-order-events"));
                    
                    ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(2));
                    
                    for (ConsumerRecord<String, Object> record : records) {
                        int retryCount = 0;
                        boolean processed = false;
                        
                        while (!processed && retryCount < 3) {
                            try {
                                // Simulate processing that might fail
                                if (Math.random() > 0.7) {
                                    throw new RuntimeException("Simulated processing error");
                                }
                                
                                logger.info("✅ Successfully processed record: partition={}, offset={}", 
                                           record.partition(), record.offset());
                                processed = true;
                                
                            } catch (Exception e) {
                                retryCount++;
                                logger.warn("Processing failed (attempt {}): {}", retryCount, e.getMessage());
                                
                                if (retryCount < 3) {
                                    // Exponential backoff
                                    Thread.sleep(1000 * retryCount);
                                } else {
                                    logger.error("❌ Max retries exceeded for record at offset {}", record.offset());
                                    // Send to dead letter queue in real implementation
                                }
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Error handling demonstration failed", e);
                }
            });
            
            errorHandlingThread.setDaemon(true);
            errorHandlingThread.start();
            errorHandlingThread.join(5000);
            
        } catch (Exception e) {
            logger.error("Error handling demonstration failed", e);
        }
    }
    
    private static void demonstrateBatchProcessing(KafkaMultiDatacenterClient client) {
        try {
            logger.info("📦 Batch processing improves throughput for high-volume scenarios");
            
            Thread batchThread = new Thread(() -> {
                try {
                    client.consumerSync().subscribe(List.of("consumer-order-events"));
                    
                    for (int batch = 0; batch < 2; batch++) {
                        ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(3));
                        
                        if (records.count() > 0) {
                            logger.info("📦 Processing batch {} with {} records", batch + 1, records.count());
                            
                            // Process records in batch
                            long startTime = System.currentTimeMillis();
                            
                            for (ConsumerRecord<String, Object> record : records) {
                                // Batch processing logic
                                if (record.value() instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> data = (Map<String, Object>) record.value();
                                    // Process the data - e.g., add to batch processor
                                    logger.debug("Processing batch record: key={}, data={}", record.key(), data);
                                }
                            }
                            
                            long processingTime = System.currentTimeMillis() - startTime;
                            double throughput = (records.count() * 1000.0) / processingTime;
                            
                            logger.info("✅ Batch {} processed: {} records in {}ms ({:.2f} records/sec)", 
                                       batch + 1, records.count(), processingTime, throughput);
                            
                            // Commit the entire batch
                            client.consumerSync().commitSync();
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Batch processing demonstration failed", e);
                }
            });
            
            batchThread.setDaemon(true);
            batchThread.start();
            batchThread.join(8000);
            
        } catch (Exception e) {
            logger.error("Batch processing demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate partition-aware consumer patterns.
     */
    private static void demonstratePartitionAwareConsumption(KafkaMultiDatacenterClient client) {
        logger.info("=== Partition-Aware Consumer Patterns ===");
        
        try {
            // Create partition-aware consumer thread
            Thread partitionConsumerThread = new Thread(() -> {
                try {
                    logger.info("Starting partition-aware consumer...");
                    
                    // Subscribe to topics with partition strategy information
                    client.consumerSync().subscribe(List.of("consumer-user-events", "consumer-order-events"));
                    
                    for (int poll = 0; poll < 4; poll++) {
                        ConsumerRecords<String, Object> records = client.consumerSync().poll(Duration.ofSeconds(3));
                        
                        if (records.count() > 0) {
                            // Group records by partition for analysis
                            Map<Integer, List<ConsumerRecord<String, Object>>> recordsByPartition = new HashMap<>();
                            
                            for (ConsumerRecord<String, Object> record : records) {
                                recordsByPartition.computeIfAbsent(record.partition(), k -> new ArrayList<>()).add(record);
                            }
                            
                            // Process records partition by partition
                            for (Map.Entry<Integer, List<ConsumerRecord<String, Object>>> entry : recordsByPartition.entrySet()) {
                                Integer partition = entry.getKey();
                                List<ConsumerRecord<String, Object>> partitionRecords = entry.getValue();
                                
                                logger.info("📊 Processing {} records from partition {}", 
                                           partitionRecords.size(), partition);
                                
                                // Partition-specific processing logic
                                processPartitionBatch(partition, partitionRecords);
                            }
                            
                            logger.info("✅ Partition-aware consumption completed for {} total records", records.count());
                        }
                        
                        client.consumerSync().commitSync();
                    }
                    
                } catch (Exception e) {
                    logger.error("Partition-aware consumer failed", e);
                }
            });
            
            partitionConsumerThread.setDaemon(true);
            partitionConsumerThread.start();
            partitionConsumerThread.join(15000);
            
        } catch (Exception e) {
            logger.error("Partition-aware consumption demonstration failed", e);
        }
    }
    
    private static void processPartitionBatch(Integer partition, List<ConsumerRecord<String, Object>> records) {
        logger.info("🔧 Processing batch for partition {} with {} records", partition, records.size());
        
        // Partition-specific logic
        if (partition == 0) {
            logger.info("  🎯 High-priority partition processing");
        } else if (partition == 1) {
            logger.info("  ⚖️ Standard partition processing");  
        } else {
            logger.info("  📦 Batch partition processing");
        }
        
        // Process each record in the partition batch
        for (ConsumerRecord<String, Object> record : records) {
            if (record.value() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) record.value();
                logger.debug("    Record: offset={}, key={}, data={}", 
                           record.offset(), record.key(), data.get("source"));
            }
        }
    }
}
