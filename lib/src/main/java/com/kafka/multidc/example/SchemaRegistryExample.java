package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating Schema Registry integration with the Kafka Multi-Datacenter Client.
 * Shows schema validation, evolution, and multi-datacenter schema management.
 */
public class SchemaRegistryExample {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryExample.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Schema Registry Example ===");
        
        try {
            // Create configuration with Schema Registry
            KafkaDatacenterConfiguration config = createSchemaRegistryConfiguration();
            
            // Build the client with Schema Registry integration
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Schema Registry client created successfully");
                
                // Check schema registry health
                demonstrateSchemaRegistryHealth(client);
                
                // Demonstrate Avro schema usage
                demonstrateAvroSchemaUsage(client);
                
                // Demonstrate JSON schema usage
                demonstrateJsonSchemaUsage(client);
                
                // Demonstrate schema evolution
                demonstrateSchemaEvolution(client);
                
                // Demonstrate multi-datacenter schema coordination
                demonstrateMultiDatacenterSchemas(client);
                
                logger.info("Schema Registry example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Schema Registry example failed", e);
        }
    }
    
    /**
     * Create configuration with Schema Registry integration.
     */
    private static KafkaDatacenterConfiguration createSchemaRegistryConfiguration() {
        logger.info("Creating Schema Registry configuration...");
        
        // Configure schema registry for each datacenter
        SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
            .urls("http://schema-registry-primary.company.com:8081")
            .addDatacenterUrls("schema-secondary", "http://schema-registry-secondary.company.com:8081")
            .basicAuth("schema-user", "schema-password")
            .connectionTimeout(Duration.ofSeconds(30))
            .readTimeout(Duration.ofSeconds(60))
            .retries(3, Duration.ofMillis(1000))
            .cache(true, 500, Duration.ofMinutes(15))
            .healthCheckInterval(Duration.ofSeconds(30))
            .failover(true)
            .build();
        
        return KafkaDatacenterConfiguration.builder()
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("schema-primary")
                    .region("us-east-1")
                    .bootstrapServers("kafka-schema-1.company.com:9092,kafka-schema-2.company.com:9092")
                    .priority(1)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .build())
            .addDatacenter(
                KafkaDatacenterEndpoint.builder()
                    .id("schema-secondary")
                    .region("us-west-2")
                    .bootstrapServers("kafka-schema-west-1.company.com:9092,kafka-schema-west-2.company.com:9092")
                    .priority(2)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .build())
            .localDatacenter("schema-primary")
            .routingStrategy(RoutingStrategy.PRIMARY_PREFERRED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .schemaRegistryConfig(schemaConfig)
            .enableMetrics(true)
            .build();
    }
    
    /**
     * Demonstrate Schema Registry health monitoring.
     */
    private static void demonstrateSchemaRegistryHealth(KafkaMultiDatacenterClient client) {
        logger.info("=== Schema Registry Health Demo ===");
        
        try {
            // Check schema registry health across datacenters
            var healthFuture = client.checkSchemaRegistryHealth();
            var healthStatus = healthFuture.get();
            
            logger.info("Schema Registry health status:");
            healthStatus.forEach((datacenter, healthy) -> 
                logger.info("  {}: {}", datacenter, healthy ? "HEALTHY" : "UNHEALTHY"));
            
            // Refresh schema registry connections
            client.refreshSchemaRegistryConnections().get();
            logger.info("✅ Schema Registry connections refreshed successfully");
            
        } catch (Exception e) {
            logger.error("❌ Schema Registry health check failed", e);
        }
    }
    
    /**
     * Demonstrate Avro schema usage for structured data.
     */
    private static void demonstrateAvroSchemaUsage(KafkaMultiDatacenterClient client) {
        logger.info("=== Avro Schema Usage Demo ===");
        
        try {
            // Create Avro-compatible user event
            Map<String, Object> userEvent = Map.of(
                "userId", "user-avro-123",
                "eventType", "USER_REGISTRATION",
                "timestamp", Instant.now().toEpochMilli(),
                "userDetails", Map.of(
                    "email", "user123@company.com",
                    "firstName", "John",
                    "lastName", "Doe",
                    "age", 30,
                    "preferences", Map.of(
                        "newsletter", true,
                        "notifications", true,
                        "theme", "dark"
                    )
                ),
                "metadata", Map.of(
                    "source", "web-registration",
                    "version", "1.0",
                    "schemaType", "avro"
                )
            );
            
            // Send Avro message with schema validation
            ProducerRecord<String, Object> avroRecord = new ProducerRecord<>(
                "user-events-avro",
                "user-avro-123",
                userEvent
            );
            
            RecordMetadata metadata = client.producerSync().send(avroRecord);
            logger.info("✅ Avro message sent to partition {} at offset {} (schema validated)", 
                       metadata.partition(), metadata.offset());
            
            // Send multiple Avro messages
            for (int i = 0; i < 3; i++) {
                Map<String, Object> batchEvent = Map.of(
                    "userId", "user-avro-" + (200 + i),
                    "eventType", "USER_UPDATE",
                    "timestamp", Instant.now().toEpochMilli(),
                    "userDetails", Map.of(
                        "email", "user" + (200 + i) + "@company.com",
                        "preferences", Map.of("theme", i % 2 == 0 ? "light" : "dark")
                    ),
                    "metadata", Map.of(
                        "source", "batch-update",
                        "batchId", "batch-001",
                        "schemaType", "avro"
                    )
                );
                
                ProducerRecord<String, Object> batchRecord = new ProducerRecord<>(
                    "user-events-avro",
                    "user-avro-" + (200 + i),
                    batchEvent
                );
                
                RecordMetadata batchMetadata = client.producerSync().send(batchRecord);
                logger.info("Avro batch message {} sent to partition {} at offset {}", 
                           i, batchMetadata.partition(), batchMetadata.offset());
            }
            
        } catch (Exception e) {
            logger.error("❌ Avro schema demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate JSON schema usage for flexible data structures.
     */
    private static void demonstrateJsonSchemaUsage(KafkaMultiDatacenterClient client) {
        logger.info("=== JSON Schema Usage Demo ===");
        
        try {
            // Create JSON schema-compliant product event
            Map<String, Object> productEvent = Map.of(
                "productId", "product-json-456",
                "eventType", "PRODUCT_CREATED",
                "timestamp", Instant.now().toEpochMilli(),
                "productDetails", Map.of(
                    "name", "Enterprise Software License",
                    "category", "SOFTWARE",
                    "price", Map.of(
                        "amount", 999.99,
                        "currency", "USD"
                    ),
                    "features", List.of("Multi-user", "Cloud-based", "24/7 Support"),
                    "tags", List.of("enterprise", "software", "license")
                ),
                "metadata", Map.of(
                    "source", "product-management-system",
                    "version", "2.0",
                    "schemaType", "json",
                    "validationLevel", "strict"
                )
            );
            
            // Send JSON schema message
            ProducerRecord<String, Object> jsonRecord = new ProducerRecord<>(
                "product-events-json",
                "product-json-456",
                productEvent
            );
            
            RecordMetadata metadata = client.producerSync().send(jsonRecord);
            logger.info("✅ JSON schema message sent to partition {} at offset {} (schema validated)", 
                       metadata.partition(), metadata.offset());
            
            // Send flexible JSON message with optional fields
            Map<String, Object> flexibleEvent = Map.of(
                "productId", "product-json-789",
                "eventType", "PRODUCT_UPDATED",
                "timestamp", Instant.now().toEpochMilli(),
                "productDetails", Map.of(
                    "name", "Updated Enterprise License",
                    "category", "SOFTWARE",
                    "price", Map.of(
                        "amount", 1199.99,
                        "currency", "USD",
                        "discount", Map.of(
                            "percentage", 10,
                            "validUntil", Instant.now().plus(Duration.ofDays(30)).toEpochMilli()
                        )
                    )
                ),
                "updateDetails", Map.of(
                    "fieldsChanged", List.of("price", "features"),
                    "updatedBy", "admin-user",
                    "reason", "pricing-update"
                ),
                "metadata", Map.of(
                    "source", "product-management-system",
                    "version", "2.1",
                    "schemaType", "json",
                    "backward-compatible", true
                )
            );
            
            ProducerRecord<String, Object> flexibleRecord = new ProducerRecord<>(
                "product-events-json",
                "product-json-789",
                flexibleEvent
            );
            
            RecordMetadata flexibleMetadata = client.producerSync().send(flexibleRecord);
            logger.info("✅ Flexible JSON message sent to partition {} at offset {} (backward compatible)", 
                       flexibleMetadata.partition(), flexibleMetadata.offset());
            
        } catch (Exception e) {
            logger.error("❌ JSON schema demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate schema evolution and compatibility checking.
     */
    private static void demonstrateSchemaEvolution(KafkaMultiDatacenterClient client) {
        logger.info("=== Schema Evolution Demo ===");
        
        try {
            // Original schema version (v1)
            Map<String, Object> orderV1 = Map.of(
                "orderId", "order-evolution-001",
                "customerId", "customer-456",
                "timestamp", Instant.now().toEpochMilli(),
                "items", List.of(
                    Map.of(
                        "productId", "product-123",
                        "quantity", 2,
                        "price", 29.99
                    )
                ),
                "total", 59.98,
                "status", "PENDING",
                "metadata", Map.of(
                    "version", "1.0",
                    "schemaEvolution", "original"
                )
            );
            
            ProducerRecord<String, Object> v1Record = new ProducerRecord<>(
                "orders-evolution",
                "order-evolution-001",
                orderV1
            );
            
            RecordMetadata v1Metadata = client.producerSync().send(v1Record);
            logger.info("✅ Schema v1 message sent to partition {} at offset {}", 
                       v1Metadata.partition(), v1Metadata.offset());
            
            // Evolved schema version (v2) with additional fields
            Map<String, Object> orderV2 = Map.of(
                "orderId", "order-evolution-002",
                "customerId", "customer-789",
                "timestamp", Instant.now().toEpochMilli(),
                "items", List.of(
                    Map.of(
                        "productId", "product-456",
                        "quantity", 1,
                        "price", 99.99,
                        "discount", 10.0  // New field in v2
                    )
                ),
                "total", 89.99,
                "status", "CONFIRMED",
                "shippingAddress", Map.of(  // New field in v2
                    "street", "123 Main St",
                    "city", "Anytown",
                    "state", "CA",
                    "zipCode", "12345",
                    "country", "USA"
                ),
                "paymentMethod", Map.of(  // New field in v2
                    "type", "CREDIT_CARD",
                    "last4", "1234"
                ),
                "metadata", Map.of(
                    "version", "2.0",
                    "schemaEvolution", "enhanced",
                    "backwardCompatible", true
                )
            );
            
            ProducerRecord<String, Object> v2Record = new ProducerRecord<>(
                "orders-evolution",
                "order-evolution-002",
                orderV2
            );
            
            RecordMetadata v2Metadata = client.producerSync().send(v2Record);
            logger.info("✅ Schema v2 message sent to partition {} at offset {} (backward compatible evolution)", 
                       v2Metadata.partition(), v2Metadata.offset());
            
            // Future schema version (v3) with further evolution
            Map<String, Object> orderV3 = Map.of(
                "orderId", "order-evolution-003",
                "customerId", "customer-101112",
                "timestamp", Instant.now().toEpochMilli(),
                "items", List.of(
                    Map.of(
                        "productId", "product-789",
                        "quantity", 3,
                        "price", 149.99,
                        "discount", 15.0,
                        "taxInfo", Map.of(  // New in v3
                            "taxRate", 0.0875,
                            "taxAmount", 39.37
                        )
                    )
                ),
                "total", 449.97,
                "status", "SHIPPED",
                "shippingAddress", Map.of(
                    "street", "456 Oak Ave",
                    "city", "Somewhere",
                    "state", "NY",
                    "zipCode", "67890",
                    "country", "USA"
                ),
                "paymentMethod", Map.of(
                    "type", "DIGITAL_WALLET",
                    "provider", "PayPal"
                ),
                "fulfillment", Map.of(  // New in v3
                    "warehouseId", "warehouse-east-001",
                    "shippingCarrier", "FedEx",
                    "trackingNumber", "1234567890123456",
                    "estimatedDelivery", Instant.now().plus(Duration.ofDays(3)).toEpochMilli()
                ),
                "metadata", Map.of(
                    "version", "3.0",
                    "schemaEvolution", "advanced",
                    "backwardCompatible", true,
                    "forwardCompatible", false
                )
            );
            
            ProducerRecord<String, Object> v3Record = new ProducerRecord<>(
                "orders-evolution",
                "order-evolution-003",
                orderV3
            );
            
            RecordMetadata v3Metadata = client.producerSync().send(v3Record);
            logger.info("✅ Schema v3 message sent to partition {} at offset {} (advanced evolution)", 
                       v3Metadata.partition(), v3Metadata.offset());
            
            logger.info("Schema evolution demonstrates backward compatibility across versions");
            
        } catch (Exception e) {
            logger.error("❌ Schema evolution demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate multi-datacenter schema coordination and synchronization.
     */
    private static void demonstrateMultiDatacenterSchemas(KafkaMultiDatacenterClient client) {
        logger.info("=== Multi-Datacenter Schema Coordination Demo ===");
        
        try {
            // Send schema-validated message to multiple datacenters
            Map<String, Object> globalEvent = Map.of(
                "eventId", "global-schema-001",
                "eventType", "GLOBAL_CONFIGURATION_UPDATE",
                "timestamp", Instant.now().toEpochMilli(),
                "configurationData", Map.of(
                    "feature", "schema-registry",
                    "settings", Map.of(
                        "compatibility", "BACKWARD",
                        "validation", "FULL",
                        "caching", true
                    ),
                    "appliedDatacenters", List.of("schema-primary", "schema-secondary")
                ),
                "metadata", Map.of(
                    "source", "global-config-service",
                    "multiDatacenter", true,
                    "schemaCoordination", "synchronized",
                    "version", "1.0"
                )
            );
            
            // Send to primary datacenter first
            ProducerRecord<String, Object> primaryRecord = new ProducerRecord<>(
                "global-events",
                "global-schema-001-primary",
                globalEvent
            );
            
            RecordMetadata primaryMetadata = client.producerSync().send("schema-primary", primaryRecord);
            logger.info("✅ Global schema message sent to PRIMARY datacenter: partition {} offset {}", 
                       primaryMetadata.partition(), primaryMetadata.offset());
            
            // Send to secondary datacenter for coordination
            ProducerRecord<String, Object> secondaryRecord = new ProducerRecord<>(
                "global-events",
                "global-schema-001-secondary",
                globalEvent
            );
            
            RecordMetadata secondaryMetadata = client.producerSync().send("schema-secondary", secondaryRecord);
            logger.info("✅ Global schema message sent to SECONDARY datacenter: partition {} offset {}", 
                       secondaryMetadata.partition(), secondaryMetadata.offset());
            
            // Demonstrate schema registry failover
            logger.info("Testing schema registry failover capabilities...");
            
            Map<String, Object> failoverEvent = Map.of(
                "eventId", "failover-schema-002",
                "eventType", "SCHEMA_REGISTRY_FAILOVER_TEST",
                "timestamp", Instant.now().toEpochMilli(),
                "failoverData", Map.of(
                    "primaryUnavailable", true,
                    "fallbackActive", true,
                    "schemaValidation", "maintained"
                ),
                "metadata", Map.of(
                    "source", "failover-test",
                    "resilience", "schema-registry-failover"
                )
            );
            
            ProducerRecord<String, Object> failoverRecord = new ProducerRecord<>(
                "failover-events",
                "failover-schema-002",
                failoverEvent
            );
            
            RecordMetadata failoverMetadata = client.producerSync().send(failoverRecord);
            logger.info("✅ Schema registry failover test successful: partition {} offset {}", 
                       failoverMetadata.partition(), failoverMetadata.offset());
            
            logger.info("Multi-datacenter schema coordination completed successfully");
            
        } catch (Exception e) {
            logger.error("❌ Multi-datacenter schema coordination failed", e);
        }
    }
}
