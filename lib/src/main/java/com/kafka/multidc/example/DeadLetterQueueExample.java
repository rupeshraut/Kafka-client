package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.deadletter.DefaultDeadLetterConfig;
import com.kafka.multidc.deadletter.DeadLetterQueueHandler;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating the Dead Letter Queue functionality in the Kafka Multi-Datacenter Client.
 * Shows how to configure and handle failed messages with comprehensive error management.
 */
public class DeadLetterQueueExample {
    
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueExample.class);
    
    public static void main(String[] args) {
        logger.info("Starting Dead Letter Queue Example");
        
        try {
            // Configure dead letter queue handling
            DeadLetterQueueHandler.DeadLetterConfig dlqConfig = DefaultDeadLetterConfig.builder()
                .deadLetterTopicSuffix(".dlq")
                .maxRetryAttempts(3)
                .enabled(true)
                .strategy(DeadLetterQueueHandler.DeadLetterStrategy.TOPIC_BASED)
                .build();
            
            // Configure multiple datacenters with DLQ support
            KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
                .addDatacenter(
                    KafkaDatacenterEndpoint.builder()
                        .id("us-east-1")
                        .region("us-east")
                        .bootstrapServers("localhost:9092")
                        .priority(1)
                        .build())
                .addDatacenter(
                    KafkaDatacenterEndpoint.builder()
                        .id("us-west-1")
                        .region("us-west")
                        .bootstrapServers("localhost:9093")
                        .priority(2)
                        .build())
                .localDatacenter("us-east-1")
                .routingStrategy(RoutingStrategy.NEAREST)
                .deadLetterConfig(dlqConfig)
                .healthCheckInterval(Duration.ofSeconds(10))
                .build();
            
            // Create the client
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Client created successfully");
                
                // Demonstrate producer operations
                demonstrateProducerOperations(client);
                
                // Demonstrate consumer operations with error handling
                demonstrateConsumerOperations(client);
                
                // Demonstrate DLQ metrics
                demonstrateMetrics(client);
                
                logger.info("Dead Letter Queue Example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Error in Dead Letter Queue Example", e);
        }
    }
    
    private static void demonstrateProducerOperations(KafkaMultiDatacenterClient client) {
        logger.info("=== Producer Operations Demo ===");
        
        try {
            // Send some successful messages
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "user-events",
                    "user-" + i,
                    "{\"userId\":\"user-" + i + "\",\"action\":\"login\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                client.producerSync().send(record);
                logger.info("Sent message: {}", record.key());
            }
            
            // Simulate some problematic messages that might fail
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "problematic-topic",
                    "problem-" + i,
                    "{\"data\":\"This might cause processing issues\"}"
                );
                
                try {
                    client.producerSync().send(record);
                    logger.info("Sent potentially problematic message: {}", record.key());
                } catch (Exception e) {
                    logger.warn("Failed to send message: {}, error: {}", record.key(), e.getMessage());
                }
            }
            
        } catch (Exception e) {
            logger.error("Error in producer operations", e);
        }
    }
    
    private static void demonstrateConsumerOperations(KafkaMultiDatacenterClient client) {
        logger.info("=== Consumer Operations Demo ===");
        
        try {
            // Subscribe to topics including dead letter topics
            List<String> topics = Arrays.asList("user-events", "user-events.dlq", "problematic-topic", "problematic-topic.dlq");
            logger.info("Would subscribe to topics: {}", topics);
            
            // Demonstrate synchronous consumption with error handling
            logger.info("Starting message consumption...");
            
            // In a real application, you would process messages in a loop
            // For this demo, we'll simulate processing a few messages
            simulateMessageProcessing();
            
        } catch (Exception e) {
            logger.error("Error in consumer operations", e);
        }
    }
    
    private static void simulateMessageProcessing() {
        logger.info("Simulating message processing scenarios...");
        
        // Simulate successful processing
        logger.info("✓ Successfully processed message: user-1");
        logger.info("✓ Successfully processed message: user-2");
        
        // Simulate processing failures that would trigger DLQ
        logger.warn("⚠ Failed to process message: problem-1 (attempt 1/3)");
        logger.warn("⚠ Failed to process message: problem-1 (attempt 2/3)");
        logger.warn("⚠ Failed to process message: problem-1 (attempt 3/3)");
        logger.error("✗ Message problem-1 sent to dead letter queue: problematic-topic.dlq");
        
        // Simulate retry success
        logger.warn("⚠ Failed to process message: problem-2 (attempt 1/3)");
        logger.warn("⚠ Failed to process message: problem-2 (attempt 2/3)");
        logger.info("✓ Successfully processed message: problem-2 on retry");
    }
    
    private static void demonstrateMetrics(KafkaMultiDatacenterClient client) {
        logger.info("=== Dead Letter Queue Metrics Demo ===");
        
        try {
            // In a real implementation, you would access DLQ metrics through the client
            // For this demo, we'll show what metrics would be available
            
            logger.info("Dead Letter Queue Metrics:");
            logger.info("- Total Failed Messages: 5");
            logger.info("- Total Retried Messages: 4");
            logger.info("- Total Dead Letter Messages: 1");
            logger.info("- Failure Rate: 20.0%");
            logger.info("- Retry Success Rate: 75.0%");
            
            logger.info("Connection Pool Metrics:");
            logger.info("- Active Connections: 2");
            logger.info("- Idle Connections: 0");
            logger.info("- Total Connections Created: 2");
            logger.info("- Failed Connection Attempts: 0");
            
            logger.info("Health Check Metrics:");
            logger.info("- us-east-1: HEALTHY");
            logger.info("- us-west-1: HEALTHY");
            
        } catch (Exception e) {
            logger.error("Error getting metrics", e);
        }
    }
}
