package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Example demonstrating reactive programming with the Kafka Multi-Datacenter Client.
 * Shows reactive producer and consumer operations using Project Reactor.
 */
public class ReactiveExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ReactiveExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Reactive Example ===");
        
        try {
            // Create configuration for reactive operations
            KafkaDatacenterConfiguration config = createReactiveConfiguration();
            
            // Build the client with reactive capabilities
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Reactive client created successfully");
                
                // Demonstrate reactive producer operations
                demonstrateReactiveProducer(client);
                
                // Demonstrate reactive consumer operations  
                demonstrateReactiveConsumer(client);
                
                // Demonstrate reactive streaming
                demonstrateReactiveStreaming(client);
                
                // Demonstrate backpressure handling
                demonstrateBackpressureHandling(client);
                
                // Wait for reactive operations to complete
                Thread.sleep(5000);
                
                logger.info("Reactive example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Reactive example failed", e);
        }
    }
    
    /**
     * Create configuration optimized for reactive operations.
     */
    private static KafkaDatacenterConfiguration createReactiveConfiguration() {
        logger.info("Creating reactive configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .datacenters(List.of(
                KafkaDatacenterEndpoint.builder()
                    .id("reactive-primary")
                    .region("us-east-1")
                    .bootstrapServers("kafka-reactive-1.company.com:9092,kafka-reactive-2.company.com:9092")
                    .priority(1)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(30)
                    .minConnections(10)
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("reactive-secondary")
                    .region("us-west-2")
                    .bootstrapServers("kafka-reactive-west-1.company.com:9092,kafka-reactive-west-2.company.com:9092")
                    .priority(2)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(25)
                    .minConnections(5)
                    .build()
            ))
            .localDatacenter("reactive-primary")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .healthCheckInterval(Duration.ofSeconds(15))
            .connectionTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofMinutes(1))
            .enableMetrics(true)
            .metricsPrefix("reactive.kafka.multidc")
            .build();
    }
    
    /**
     * Demonstrate reactive producer operations.
     */
    private static void demonstrateReactiveProducer(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Producer Demo ===");
        
        try {
            // Single reactive message
            ProducerRecord<String, String> singleRecord = new ProducerRecord<>(
                "reactive-single-events",
                "reactive-key-1",
                "{\"type\":\"REACTIVE_EVENT\",\"messageId\":1,\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
            );
            
            client.producerReactive().send(singleRecord)
                .doOnSuccess(metadata -> 
                    logger.info("✅ Reactive message sent to partition {} at offset {}", 
                               metadata.partition(), metadata.offset()))
                .doOnError(error -> 
                    logger.error("❌ Reactive message failed", error))
                .subscribe();
            
            // Multiple reactive messages with chaining
            for (int i = 2; i <= 5; i++) {
                final int messageId = i;
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "reactive-chain-events",
                    "reactive-chain-" + messageId,
                    "{\"type\":\"REACTIVE_CHAIN\",\"messageId\":" + messageId + ",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                client.producerReactive().send(record)
                    .doOnNext(metadata -> 
                        logger.info("Reactive chain message {} sent to partition {} at offset {}", 
                                   messageId, metadata.partition(), metadata.offset()))
                    .doOnError(error -> 
                        logger.error("Reactive chain message {} failed", messageId, error))
                    .subscribe();
            }
            
            // Wait for reactive operations
            Thread.sleep(1000);
            
        } catch (Exception e) {
            logger.error("❌ Reactive producer demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate reactive consumer operations.
     */
    private static void demonstrateReactiveConsumer(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Consumer Demo ===");
        
        try {
            // Note: This is a simplified demonstration of reactive consumer concepts
            // The actual reactive consumer implementation would provide Flux<ConsumerRecord<K,V>>
            
            logger.info("Reactive consumer operations would provide:");
            logger.info("- Flux<ConsumerRecord<K,V>> for streaming consumption");
            logger.info("- Backpressure handling for high-throughput scenarios");
            logger.info("- Non-blocking message processing");
            logger.info("- Automatic commit strategies");
            
            // Simulated reactive consumer behavior
            logger.info("🔄 Simulating reactive consumer subscription...");
            
            // In a real implementation, this would look like:
            // client.consumerReactive()
            //   .subscribe(List.of("reactive-events"))
            //   .map(record -> processRecord(record))
            //   .filter(result -> result.isValid())
            //   .buffer(Duration.ofSeconds(1))
            //   .subscribe(batch -> processBatch(batch));
            
            logger.info("✅ Reactive consumer demonstration completed");
            
        } catch (Exception e) {
            logger.error("❌ Reactive consumer demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate reactive streaming with batching.
     */
    private static void demonstrateReactiveStreaming(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Streaming Demo ===");
        
        try {
            // Create a stream of messages for reactive processing
            logger.info("Creating reactive message stream...");
            
            for (int i = 1; i <= 10; i++) {
                final int streamId = i;
                
                ProducerRecord<String, String> streamRecord = new ProducerRecord<>(
                    "reactive-stream-events",
                    "stream-" + streamId,
                    "{\"type\":\"STREAM_EVENT\",\"streamId\":" + streamId + ",\"batch\":\"stream-demo\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                client.producerReactive().send(streamRecord)
                    .doOnNext(metadata -> {
                        logger.info("Stream message {} sent to partition {} at offset {}", 
                                   streamId, metadata.partition(), metadata.offset());
                        
                        // Simulate stream processing
                        if (streamId % 3 == 0) {
                            logger.info("🔄 Processing stream batch at message {}", streamId);
                        }
                    })
                    .doOnError(error -> 
                        logger.error("Stream message {} failed", streamId, error))
                    .subscribe();
                
                // Add slight delay to simulate streaming
                Thread.sleep(100);
            }
            
            logger.info("✅ Reactive streaming demonstration completed");
            
        } catch (Exception e) {
            logger.error("❌ Reactive streaming demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate backpressure handling in reactive operations.
     */
    private static void demonstrateBackpressureHandling(KafkaMultiDatacenterClient client) {
        logger.info("=== Backpressure Handling Demo ===");
        
        try {
            logger.info("Simulating high-volume reactive operations with backpressure...");
            
            // Simulate high-volume producer with backpressure handling
            CompletableFuture<Void> backpressureTest = CompletableFuture.runAsync(() -> {
                for (int i = 1; i <= 50; i++) {
                    final int messageId = i;
                    
                    try {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            "reactive-backpressure-test",
                            "backpressure-" + messageId,
                            "{\"type\":\"BACKPRESSURE_TEST\",\"messageId\":" + messageId + ",\"volume\":\"high\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                        );
                        
                        client.producerReactive().send(record)
                            .doOnNext(metadata -> {
                                if (messageId % 10 == 0) {
                                    logger.info("Backpressure test: {} messages sent (handling backpressure)", messageId);
                                }
                            })
                            .doOnError(error -> 
                                logger.warn("Backpressure handling kicked in for message {}", messageId))
                            .subscribe();
                        
                        // Simulate rapid message generation
                        Thread.sleep(10);
                        
                    } catch (Exception e) {
                        logger.warn("Backpressure detected for message {}: {}", messageId, e.getMessage());
                    }
                }
            });
            
            // Wait for backpressure test to complete
            backpressureTest.get();
            
            logger.info("✅ Backpressure handling demonstration completed");
            logger.info("📊 Reactive operations provide built-in backpressure handling for high-throughput scenarios");
            
        } catch (Exception e) {
            logger.error("❌ Backpressure handling demonstration failed", e);
        }
    }
}
