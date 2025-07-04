package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Working examples demonstrating the security features of the Kafka Multi-Datacenter Client.
 * This example focuses on SSL/TLS and SASL authentication configuration.
 */
public class SecurityExample {
    
    private static final Logger logger = LoggerFactory.getLogger(SecurityExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Security Example ===");
        
        try {
            // Create secure configuration
            KafkaDatacenterConfiguration config = createSecureConfiguration();
            
            // Build the client with security features enabled
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Secure client created successfully");
                
                // Demonstrate secure messaging
                demonstrateSecureMessaging(client);
                
                // Demonstrate secure async messaging
                demonstrateSecureAsyncMessaging(client);
                
                // Demonstrate secure reactive messaging
                demonstrateSecureReactiveMessaging(client);
                
                logger.info("Security example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Security example failed", e);
        }
    }
    
    /**
     * Create a secure configuration with SSL/TLS and SASL authentication.
     */
    private static KafkaDatacenterConfiguration createSecureConfiguration() {
        logger.info("Creating secure configuration...");
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("secure-east")
                    .region("us-east-1")
                    .bootstrapServers("secure-kafka-east-1.company.com:9093,secure-kafka-east-2.company.com:9093")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .ssl(true)
                    .securityProtocol("SASL_SSL")
                    .saslMechanism("SCRAM-SHA-512")
                    .saslCredentials("kafka-client-east", "secure-password-east")
                    .keystore("/path/to/client-east.keystore.jks", "keystore-password")
                    .truststore("/path/to/client-east.truststore.jks", "truststore-password")
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("secure-west")
                    .region("us-west-2")
                    .bootstrapServers("secure-kafka-west-1.company.com:9093,secure-kafka-west-2.company.com:9093")
                    .priority(2)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .ssl(true)
                    .securityProtocol("SASL_SSL")
                    .saslMechanism("SCRAM-SHA-512")
                    .saslCredentials("kafka-client-west", "secure-password-west")
                    .keystore("/path/to/client-west.keystore.jks", "keystore-password")
                    .truststore("/path/to/client-west.truststore.jks", "truststore-password")
                    .build())
            .localDatacenter("secure-east")
            .routingStrategy(RoutingStrategy.NEAREST)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(30))
            .requestTimeout(Duration.ofMinutes(2))
            .enableMetrics(true)
            .build();
    }
    
    /**
     * Demonstrate secure synchronous messaging.
     */
    private static void demonstrateSecureMessaging(KafkaMultiDatacenterClient client) {
        logger.info("=== Secure Synchronous Messaging ===");
        
        try {
            // Create secure headers
            RecordHeaders secureHeaders = new RecordHeaders();
            secureHeaders.add("security-level", "HIGH".getBytes());
            secureHeaders.add("encrypted", "true".getBytes());
            secureHeaders.add("source-system", "secure-enterprise-app".getBytes());
            
            // Create secure message
            ProducerRecord<String, String> secureRecord = new ProducerRecord<>(
                "secure-transactions",
                null, // auto-assign partition
                "secure-user-123",
                "{\"userId\":\"secure-user-123\",\"action\":\"secure-login\",\"timestamp\":\"" + System.currentTimeMillis() + "\",\"sensitiveData\":\"encrypted-payload\"}",
                secureHeaders
            );
            
            // Send secure message
            RecordMetadata metadata = client.producerSync().send(secureRecord);
            logger.info("✅ Secure message sent to partition {} at offset {}", 
                       metadata.partition(), metadata.offset());
            
            // Send multiple secure messages
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "secure-audit-log",
                    "audit-" + i,
                    "{\"auditId\":\"audit-" + i + "\",\"action\":\"DATA_ACCESS\",\"user\":\"admin\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                RecordMetadata auditMetadata = client.producerSync().send(record);
                logger.info("Audit message {} sent to partition {} at offset {}", 
                           i, auditMetadata.partition(), auditMetadata.offset());
            }
            
            // Force flush to ensure all messages are sent
            client.producerSync().flush();
            logger.info("All secure messages flushed successfully");
            
        } catch (Exception e) {
            logger.error("❌ Secure messaging failed", e);
        }
    }
    
    /**
     * Demonstrate secure asynchronous messaging.
     */
    private static void demonstrateSecureAsyncMessaging(KafkaMultiDatacenterClient client) {
        logger.info("=== Secure Asynchronous Messaging ===");
        
        try {
            // Send secure async messages
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "secure-async-events",
                    "async-key-" + i,
                    "{\"eventId\":\"async-" + i + "\",\"type\":\"SECURE_EVENT\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
                );
                
                final int messageIndex = i;
                client.producerAsync().sendAsync(record)
                    .thenAccept(metadata -> 
                        logger.info("✅ Async secure message {} sent to partition {} at offset {}", 
                                   messageIndex, metadata.partition(), metadata.offset()))
                    .exceptionally(error -> {
                        logger.error("❌ Async secure message {} failed", messageIndex, error);
                        return null;
                    });
            }
            
            // Wait a bit for async operations to complete
            Thread.sleep(2000);
            
        } catch (Exception e) {
            logger.error("❌ Secure async messaging failed", e);
        }
    }
    
    /**
     * Demonstrate secure reactive messaging.
     */
    private static void demonstrateSecureReactiveMessaging(KafkaMultiDatacenterClient client) {
        logger.info("=== Secure Reactive Messaging ===");
        
        try {
            // Send reactive secure message
            ProducerRecord<String, String> reactiveRecord = new ProducerRecord<>(
                "secure-reactive-stream",
                "reactive-key",
                "{\"streamId\":\"reactive-stream-1\",\"type\":\"SECURE_STREAM_EVENT\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"
            );
            
            client.producerReactive().send(reactiveRecord)
                .doOnSuccess(metadata -> 
                    logger.info("✅ Reactive secure message sent to partition {} at offset {}", 
                               metadata.partition(), metadata.offset()))
                .doOnError(error -> 
                    logger.error("❌ Reactive secure message failed", error))
                .subscribe();
            
            // Wait for reactive operation to complete
            Thread.sleep(1000);
            
        } catch (Exception e) {
            logger.error("❌ Secure reactive messaging failed", e);
        }
    }
}
