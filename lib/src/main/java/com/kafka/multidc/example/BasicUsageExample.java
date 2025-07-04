package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Basic usage examples for the Kafka Multi-Datacenter Client.
 * Demonstrates simple producer and consumer operations across multiple datacenters.
 */
public class BasicUsageExample {
    
    private static final Logger logger = LoggerFactory.getLogger(BasicUsageExample.class);
    
    public static void main(String[] args) {
        // Configure multiple datacenters
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .bootstrapServers("localhost:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .bootstrapServers("localhost:9093")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .build())
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.HEALTH_AWARE)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(10))
            .enableMetrics(true)
            .build();

        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            
            logger.info("Starting Kafka Multi-Datacenter Client Basic Usage Examples");
            
            // Run all basic examples
            basicProducerExample(client);
            basicConsumerExample(client);
            asyncProducerExample(client);
            reactiveProducerExample(client);
            reactiveConsumerExample(client);
            
            logger.info("All basic usage examples completed successfully!");
            
        } catch (Exception e) {
            logger.error("Error running basic usage examples", e);
        }
    }
    
    /**
     * Basic synchronous producer example
     */
    private static void basicProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Basic Producer Example ===");
        
        try {
            // Send simple messages
            for (int i = 0; i < 5; i++) {
                String key = "user-" + i;
                String value = "event-data-" + i;
                
                ProducerRecord<String, String> record = new ProducerRecord<>("user-events", key, value);
                RecordMetadata metadata = client.producerSync().send(record);
                logger.info("Message sent: key={}, partition={}, offset={}", 
                           key, metadata.partition(), metadata.offset());
            }
            
            // Send message with headers
            RecordHeaders headers = new RecordHeaders();
            headers.add("source", "basic-example".getBytes());
            headers.add("version", "1.0".getBytes());
            headers.add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
            
            ProducerRecord<String, String> recordWithHeaders = new ProducerRecord<>(
                "user-events", null, "header-user", "header-data", headers);
            RecordMetadata metadata = client.producerSync().send(recordWithHeaders);
            logger.info("Message with headers sent: partition={}, offset={}", 
                       metadata.partition(), metadata.offset());
            
        } catch (Exception e) {
            logger.error("Basic producer example failed", e);
        }
    }
    
    /**
     * Basic synchronous consumer example
     */
    private static void basicConsumerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Basic Consumer Example ===");
        
        try {
            // Subscribe to topics
            client.consumerSync().subscribe(List.of("user-events"));
            
            // Poll for messages (limited for example)
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<String, String> records = client.consumerSync().poll(Duration.ofSeconds(2));
                
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumed message: key={}, value={}, topic={}, partition={}, offset={}", 
                               record.key(), record.value(), record.topic(), 
                               record.partition(), record.offset());
                    
                    // Process the message
                    processMessage(record);
                }
                
                // Commit offsets
                client.consumerSync().commitSync();
                logger.info("Offsets committed for poll {}", i + 1);
            }
            
        } catch (Exception e) {
            logger.error("Basic consumer example failed", e);
        }
    }
    
    /**
     * Asynchronous producer example
     */
    private static void asyncProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Async Producer Example ===");
        
        try {
            // Send multiple messages asynchronously
            List<CompletableFuture<RecordMetadata>> futures = new java.util.ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                String key = "async-user-" + i;
                String value = "async-event-data-" + i;
                
                ProducerRecord<String, String> record = new ProducerRecord<>("async-events", key, value);
                CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
                
                future.thenAccept(metadata -> 
                    logger.info("Async message sent: key={}, partition={}, offset={}", 
                               key, metadata.partition(), metadata.offset())
                ).exceptionally(throwable -> {
                    logger.error("Failed to send async message: key={}", key, throwable);
                    return null;
                });
                
                futures.add(future);
            }
            
            // Wait for all messages to be sent
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> logger.info("All async messages sent successfully!"))
                .join();
            
        } catch (Exception e) {
            logger.error("Async producer example failed", e);
        }
    }
    
    /**
     * Reactive producer example with backpressure handling
     */
    private static void reactiveProducerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Producer Example ===");
        
        try {
            // Create a stream of messages
            Flux<String> messageStream = Flux.range(1, 20)
                .map(i -> "reactive-message-" + i);
            
            // Send with backpressure control
            messageStream
                .flatMap(message -> {
                    String key = "reactive-key-" + message.split("-")[2];
                    ProducerRecord<String, String> record = new ProducerRecord<>("reactive-topic", key, message);
                    return client.producerReactive().send(record);
                }, 5) // Control concurrency
                .doOnNext(metadata -> 
                    logger.info("Reactive message sent to partition: {}", metadata.partition()))
                .doOnError(error -> 
                    logger.error("Reactive send error", error))
                .doOnComplete(() -> 
                    logger.info("All reactive messages sent!"))
                .blockLast(); // Wait for completion in this example
            
        } catch (Exception e) {
            logger.error("Reactive producer example failed", e);
        }
    }
    
    /**
     * Reactive consumer example with stream processing
     */
    private static void reactiveConsumerExample(KafkaMultiDatacenterClient client) {
        logger.info("=== Reactive Consumer Example ===");
        
        try {
            // Subscribe and process records as a stream
            reactor.core.Disposable subscription = client.consumerReactive()
                .subscribe("reactive-topic")
                .take(10) // Take only 10 messages for demo
                .doOnNext(record -> {
                    logger.info("Reactive consumed: key={}, value={}, partition={}, offset={}", 
                               record.key(), record.value(), record.partition(), record.offset());
                    
                    // Your processing logic here
                    processReactiveMessageGeneric(record);
                })
                .doOnError(error -> logger.error("Reactive consumer error", error))
                .doOnComplete(() -> logger.info("Reactive consumer completed"))
                .subscribe();
            
            // Wait a bit for messages
            Thread.sleep(5000);
            subscription.dispose();
            
        } catch (Exception e) {
            logger.error("Reactive consumer example failed", e);
        }
    }
    
    /**
     * Process a consumed message
     */
    private static void processMessage(ConsumerRecord<String, String> record) {
        logger.info("Processing message: {}", record.value());
        
        // Simulate processing time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        logger.info("Message processed successfully: {}", record.key());
    }
    
    /**
     * Process a reactive message
     */
    private static void processReactiveMessage(ConsumerRecord<String, String> record) {
        logger.info("Processing reactive message: {}", record.value());
        // Add your reactive processing logic here
    }
    
    /**
     * Process a reactive message (generic version)
     */
    private static void processReactiveMessageGeneric(ConsumerRecord<Object, Object> record) {
        logger.info("Processing reactive message: key={}, value={}", record.key(), record.value());
        // Add your reactive processing logic here
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
