package com.kafka.multidc.example;

import com.kafka.multidc.KafkaMultiDatacenterClient;
import com.kafka.multidc.KafkaMultiDatacenterClientBuilder;
import com.kafka.multidc.config.*;
import com.kafka.multidc.routing.RoutingStrategy;
import com.kafka.multidc.schema.SchemaRegistryClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Comprehensive example demonstrating advanced serialization features
 * with the Kafka Multi-Datacenter Client including JSON, Avro, Protobuf,
 * compression, encryption, and schema evolution.
 */
public class AdvancedSerializationExample {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedSerializationExample.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Sample data models for serialization
    public static class UserEvent {
        public String userId;
        public String eventType;
        public String timestamp;
        public Map<String, Object> metadata;
        public String region;
        public double value;
        
        public UserEvent() {}
        
        public UserEvent(String userId, String eventType, String region, double value) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = Instant.now().toString();
            this.metadata = new HashMap<>();
            this.region = region;
            this.value = value;
        }
        
        // Getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        public Map<String, Object> getMetadata() { return metadata; }
        public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public double getValue() { return value; }
        public void setValue(double value) { this.value = value; }
        
        @Override
        public String toString() {
            return String.format("UserEvent{userId='%s', eventType='%s', region='%s', value=%.2f, timestamp='%s'}", 
                               userId, eventType, region, value, timestamp);
        }
    }
    
    public static class OrderEvent {
        public String orderId;
        public String customerId;
        public List<String> items;
        public double totalAmount;
        public String status;
        public String datacenter;
        public String timestamp;
        
        public OrderEvent() {}
        
        public OrderEvent(String orderId, String customerId, List<String> items, double totalAmount, String datacenter) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.items = items;
            this.totalAmount = totalAmount;
            this.status = "CREATED";
            this.datacenter = datacenter;
            this.timestamp = Instant.now().toString();
        }
        
        // Getters and setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public List<String> getItems() { return items; }
        public void setItems(List<String> items) { this.items = items; }
        public double getTotalAmount() { return totalAmount; }
        public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getDatacenter() { return datacenter; }
        public void setDatacenter(String datacenter) { this.datacenter = datacenter; }
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        
        @Override
        public String toString() {
            return String.format("OrderEvent{orderId='%s', customerId='%s', items=%s, totalAmount=%.2f, status='%s', datacenter='%s'}", 
                               orderId, customerId, items, totalAmount, status, datacenter);
        }
    }
    
    public static void main(String[] args) {
        logger.info("=== Kafka Multi-Datacenter Client Advanced Serialization Example ===");
        
        try {
            // Create configuration for serialization demonstration
            KafkaDatacenterConfiguration config = createSerializationConfiguration();
            
            try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create()
                .configuration(config)
                .build()) {
                
                logger.info("Serialization client created successfully");
                
                // Demonstrate different serialization formats
                demonstrateJsonSerialization(client);
                
                // Demonstrate compression strategies
                demonstrateCompressionSerialization(client);
                
                // Demonstrate encryption
                demonstrateEncryptionSerialization(client);
                
                // Demonstrate Avro serialization (simulated)
                demonstrateAvroSerialization(client);
                
                // Demonstrate schema evolution
                demonstrateSchemaEvolution(client);
                
                // Demonstrate per-datacenter serialization
                demonstratePerDatacenterSerialization(client);
                
                // Wait for demonstration to complete
                Thread.sleep(10000);
                
                logger.info("Advanced serialization example completed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Advanced serialization example failed", e);
        } finally {
            scheduler.shutdown();
        }
    }
    
    /**
     * Create configuration optimized for serialization demonstrations.
     */
    private static KafkaDatacenterConfiguration createSerializationConfiguration() {
        logger.info("Creating serialization configuration...");
        
        // Schema Registry configuration
        SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
            .urls("http://schema-registry.company.com:8081")
            .basicAuth("schema-user", "schema-password")
            .connectionTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(30))
            .retries(3, Duration.ofMillis(1000))
            .build();
        
        return KafkaDatacenterConfiguration.builder()
            .datacenters(List.of(
                KafkaDatacenterEndpoint.builder()
                    .id("serialization-us-east")
                    .region("us-east-1")
                    .bootstrapServers("kafka-us-east.company.com:9092")
                    .priority(1)
                    .compressionType("lz4")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(20)
                    .minConnections(3)
                    .additionalProperties(Map.of(
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "com.kafka.multidc.example.AdvancedSerializationExample$JsonUserEventSerializer",
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", "com.kafka.multidc.example.AdvancedSerializationExample$JsonUserEventDeserializer"
                    ))
                    .build(),
                KafkaDatacenterEndpoint.builder()
                    .id("serialization-eu-west")
                    .region("eu-west-1")
                    .bootstrapServers("kafka-eu-west.company.com:9092")
                    .priority(2)
                    .compressionType("snappy")
                    .enableIdempotence(true)
                    .connectionTimeout(Duration.ofSeconds(12))
                    .requestTimeout(Duration.ofSeconds(30))
                    .maxConnections(15)
                    .minConnections(2)
                    .additionalProperties(Map.of(
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "com.kafka.multidc.example.AdvancedSerializationExample$CompressedJsonSerializer",
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", "com.kafka.multidc.example.AdvancedSerializationExample$CompressedJsonDeserializer"
                    ))
                    .build()
            ))
            .localDatacenter("serialization-us-east")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(15))
            .requestTimeout(Duration.ofMinutes(2))
            .enableMetrics(true)
            .metricsPrefix("serialization.kafka.multidc")
            .schemaRegistryConfig(schemaConfig)
            .build();
    }
    
    /**
     * Demonstrate JSON serialization with different patterns.
     */
    private static void demonstrateJsonSerialization(KafkaMultiDatacenterClient client) {
        logger.info("=== JSON Serialization Demo ===");
        
        try {
            // 1. Simple JSON serialization
            demonstrateSimpleJson(client);
            
            // 2. Complex nested JSON
            demonstrateComplexJson(client);
            
            // 3. JSON with custom serializers
            demonstrateCustomJsonSerialization(client);
            
        } catch (Exception e) {
            logger.error("JSON serialization demonstration failed", e);
        }
    }
    
    private static void demonstrateSimpleJson(KafkaMultiDatacenterClient client) {
        logger.info("📄 Simple JSON Serialization");
        
        try {
            UserEvent event = new UserEvent("user-123", "page_view", "us-east", 1.0);
            event.metadata.put("page", "/dashboard");
            event.metadata.put("sessionId", "session-456");
            
            String jsonValue = objectMapper.writeValueAsString(event);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-simple-json",
                event.userId,
                jsonValue
            );
            
            // Add serialization metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("schema-version", "1.0".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Simple JSON event sent: {} to partition {}", event, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Simple JSON serialization failed", e);
        }
    }
    
    private static void demonstrateComplexJson(KafkaMultiDatacenterClient client) {
        logger.info("🔗 Complex Nested JSON Serialization");
        
        try {
            OrderEvent order = new OrderEvent(
                "order-789", 
                "customer-456", 
                Arrays.asList("laptop", "mouse", "keyboard"), 
                1299.99, 
                "us-east"
            );
            
            // Add complex nested metadata
            Map<String, Object> orderMetadata = new HashMap<>();
            orderMetadata.put("paymentMethod", Map.of(
                "type", "credit_card",
                "last4", "1234",
                "network", "visa"
            ));
            orderMetadata.put("shipping", Map.of(
                "address", Map.of(
                    "street", "123 Main St",
                    "city", "New York",
                    "state", "NY",
                    "zipCode", "10001"
                ),
                "method", "standard",
                "estimatedDays", 3
            ));
            orderMetadata.put("promotions", Arrays.asList(
                Map.of("code", "SAVE10", "discount", 10.0),
                Map.of("code", "FREESHIP", "discount", 15.0)
            ));
            
            Map<String, Object> complexOrder = Map.of(
                "order", order,
                "metadata", orderMetadata,
                "timestamp", Instant.now().toString(),
                "version", "2.1"
            );
            
            String jsonValue = objectMapper.writeValueAsString(complexOrder);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-complex-json",
                order.orderId,
                jsonValue
            );
            
            // Add complex serialization headers
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("schema-version", "2.1".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("data-classification", "PII".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("serialization-strategy", "nested-json".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Complex JSON order sent: {} bytes to partition {}", 
                       jsonValue.length(), metadata.partition());
            
        } catch (Exception e) {
            logger.error("Complex JSON serialization failed", e);
        }
    }
    
    private static void demonstrateCustomJsonSerialization(KafkaMultiDatacenterClient client) {
        logger.info("🎯 Custom JSON Serialization with Validation");
        
        try {
            // Create event with validation
            UserEvent event = new UserEvent("user-789", "purchase", "eu-west", 299.99);
            event.metadata.put("productId", "prod-123");
            event.metadata.put("category", "electronics");
            event.metadata.put("validated", true);
            
            // Custom JSON with validation and formatting
            Map<String, Object> validatedEvent = Map.of(
                "schema", "user-event-v1",
                "data", event,
                "validation", Map.of(
                    "validated", true,
                    "validatedAt", Instant.now().toString(),
                    "validator", "advanced-serialization-validator"
                ),
                "serialization", Map.of(
                    "format", "json",
                    "version", "1.0",
                    "compressed", false,
                    "encrypted", false
                )
            );
            
            String jsonValue = objectMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(validatedEvent);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-custom-json",
                event.userId,
                jsonValue
            );
            
            // Add custom serialization headers
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("schema-name", "user-event-v1".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("validation-status", "validated".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("serializer", "custom-json-validator".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Custom validated JSON event sent: {} to partition {}", 
                       event, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Custom JSON serialization failed", e);
        }
    }
    
    /**
     * Demonstrate compression serialization strategies.
     */
    private static void demonstrateCompressionSerialization(KafkaMultiDatacenterClient client) {
        logger.info("=== Compression Serialization Demo ===");
        
        try {
            // Demonstrate different compression algorithms
            demonstrateGzipCompression(client);
            demonstrateSnappyCompression(client);
            demonstrateLz4Compression(client);
            demonstrateZstdCompression(client);
            
        } catch (Exception e) {
            logger.error("Compression serialization demonstration failed", e);
        }
    }
    
    private static void demonstrateGzipCompression(KafkaMultiDatacenterClient client) {
        logger.info("🗜️ GZIP Compression Serialization");
        
        try {
            // Create large payload for compression demonstration
            UserEvent event = new UserEvent("user-compress-123", "bulk_data", "us-east", 1500.0);
            
            // Add large metadata to demonstrate compression benefits
            for (int i = 0; i < 100; i++) {
                event.metadata.put("field_" + i, "This is a repeated field with substantial content that will benefit from compression - iteration " + i);
            }
            
            String jsonValue = objectMapper.writeValueAsString(event);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Compress using GZIP
            byte[] compressedBytes = gzipCompress(originalBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-gzip-compression",
                event.userId,
                compressedBytes
            );
            
            // Add compression metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "gzip".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("original-size", String.valueOf(originalBytes.length).getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compressed-size", String.valueOf(compressedBytes.length).getBytes(StandardCharsets.UTF_8)));
            
            double compressionRatio = (double) compressedBytes.length / originalBytes.length;
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ GZIP compressed event sent: {} -> {} bytes ({:.1f}% compression) to partition {}", 
                       originalBytes.length, compressedBytes.length, (1 - compressionRatio) * 100, metadata.partition());
            
        } catch (Exception e) {
            logger.error("GZIP compression serialization failed", e);
        }
    }
    
    private static void demonstrateSnappyCompression(KafkaMultiDatacenterClient client) {
        logger.info("⚡ Snappy Compression Serialization (Simulated)");
        
        try {
            OrderEvent order = new OrderEvent(
                "order-snappy-456", 
                "customer-compress-789", 
                Arrays.asList("smartphone", "case", "charger", "headphones"), 
                899.99, 
                "eu-west"
            );
            
            String jsonValue = objectMapper.writeValueAsString(order);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Simulate Snappy compression (faster but lower compression ratio)
            byte[] simulatedSnappyBytes = simulateSnappyCompression(originalBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-snappy-compression",
                order.orderId,
                simulatedSnappyBytes
            );
            
            // Add Snappy compression metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "snappy".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression-speed", "fast".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("original-size", String.valueOf(originalBytes.length).getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Snappy compressed order sent: {} -> {} bytes (fast compression) to partition {}", 
                       originalBytes.length, simulatedSnappyBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Snappy compression serialization failed", e);
        }
    }
    
    private static void demonstrateLz4Compression(KafkaMultiDatacenterClient client) {
        logger.info("🚀 LZ4 Compression Serialization (Simulated)");
        
        try {
            // Create time-series like data that compresses well
            List<Map<String, Object>> timeSeriesData = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            
            for (int i = 0; i < 50; i++) {
                timeSeriesData.add(Map.of(
                    "timestamp", baseTime + (i * 1000),
                    "metric", "cpu_usage",
                    "value", 45.5 + (Math.random() * 20),
                    "host", "server-" + (i % 5),
                    "datacenter", "us-east",
                    "tags", Map.of("environment", "production", "service", "api")
                ));
            }
            
            Map<String, Object> timeSeriesEvent = Map.of(
                "eventType", "metrics_batch",
                "metrics", timeSeriesData,
                "batchSize", timeSeriesData.size(),
                "compression", "lz4"
            );
            
            String jsonValue = objectMapper.writeValueAsString(timeSeriesEvent);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Simulate LZ4 compression (balanced speed and compression)
            byte[] simulatedLz4Bytes = simulateLz4Compression(originalBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-lz4-compression",
                "metrics-batch-" + System.currentTimeMillis(),
                simulatedLz4Bytes
            );
            
            // Add LZ4 compression metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "lz4".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression-profile", "balanced".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("data-type", "time-series".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ LZ4 compressed time-series sent: {} -> {} bytes (balanced compression) to partition {}", 
                       originalBytes.length, simulatedLz4Bytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("LZ4 compression serialization failed", e);
        }
    }
    
    private static void demonstrateZstdCompression(KafkaMultiDatacenterClient client) {
        logger.info("🎯 ZSTD Compression Serialization (Simulated)");
        
        try {
            // Create structured data with repetitive patterns
            Map<String, Object> structuredData = Map.of(
                "eventType", "user_analytics",
                "users", generateRepeatedUserData(30),
                "compressionAlgorithm", "zstd",
                "compressionLevel", "high"
            );
            
            String jsonValue = objectMapper.writeValueAsString(structuredData);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Simulate ZSTD compression (highest compression ratio)
            byte[] simulatedZstdBytes = simulateZstdCompression(originalBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-zstd-compression",
                "analytics-" + System.currentTimeMillis(),
                simulatedZstdBytes
            );
            
            // Add ZSTD compression metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "zstd".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression-level", "high".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression-ratio", String.format("%.2f", (double) simulatedZstdBytes.length / originalBytes.length).getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ ZSTD compressed analytics sent: {} -> {} bytes (highest compression) to partition {}", 
                       originalBytes.length, simulatedZstdBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("ZSTD compression serialization failed", e);
        }
    }
    
    /**
     * Demonstrate encryption serialization.
     */
    private static void demonstrateEncryptionSerialization(KafkaMultiDatacenterClient client) {
        logger.info("=== Encryption Serialization Demo ===");
        
        try {
            // Demonstrate AES-128 encryption
            demonstrateAes128Encryption(client);
            
            // Demonstrate AES-256 encryption
            demonstrateAes256Encryption(client);
            
        } catch (Exception e) {
            logger.error("Encryption serialization demonstration failed", e);
        }
    }
    
    private static void demonstrateAes128Encryption(KafkaMultiDatacenterClient client) {
        logger.info("🔐 AES-128 Encryption Serialization");
        
        try {
            // Create sensitive user data
            UserEvent sensitiveEvent = new UserEvent("user-sensitive-123", "payment", "us-east", 999.99);
            sensitiveEvent.metadata.put("creditCardNumber", "4111-1111-1111-1111");
            sensitiveEvent.metadata.put("cvv", "123");
            sensitiveEvent.metadata.put("expiryDate", "12/25");
            sensitiveEvent.metadata.put("bankAccount", "1234567890");
            
            String jsonValue = objectMapper.writeValueAsString(sensitiveEvent);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Generate AES-128 key
            SecretKey aes128Key = generateAESKey(128);
            byte[] encryptedBytes = encryptAES(originalBytes, aes128Key);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-aes128-encryption",
                sensitiveEvent.userId,
                encryptedBytes
            );
            
            // Add encryption metadata (never include actual key!)
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption", "aes-128".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption-mode", "cbc".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("key-id", "key-123".getBytes(StandardCharsets.UTF_8))); // Reference to key, not actual key
            record.headers().add(new RecordHeader("data-classification", "sensitive".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encrypted-size", String.valueOf(encryptedBytes.length).getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ AES-128 encrypted sensitive data sent: {} -> {} bytes (encrypted) to partition {}", 
                       originalBytes.length, encryptedBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("AES-128 encryption serialization failed", e);
        }
    }
    
    private static void demonstrateAes256Encryption(KafkaMultiDatacenterClient client) {
        logger.info("🛡️ AES-256 Encryption Serialization");
        
        try {
            // Create highly sensitive financial data
            Map<String, Object> financialData = Map.of(
                "transactionId", "txn-" + System.currentTimeMillis(),
                "accountDetails", Map.of(
                    "accountNumber", "987654321",
                    "routingNumber", "123456789",
                    "bankName", "Secure Bank"
                ),
                "transaction", Map.of(
                    "amount", 5000.00,
                    "currency", "USD",
                    "type", "wire_transfer",
                    "destination", Map.of(
                        "accountNumber", "111222333",
                        "routingNumber", "987654321"
                    )
                ),
                "compliance", Map.of(
                    "kycVerified", true,
                    "amlChecked", true,
                    "riskScore", "low"
                )
            );
            
            String jsonValue = objectMapper.writeValueAsString(financialData);
            byte[] originalBytes = jsonValue.getBytes(StandardCharsets.UTF_8);
            
            // Generate AES-256 key for maximum security
            SecretKey aes256Key = generateAESKey(256);
            byte[] encryptedBytes = encryptAES(originalBytes, aes256Key);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-aes256-encryption",
                (String) financialData.get("transactionId"),
                encryptedBytes
            );
            
            // Add strong encryption metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption", "aes-256".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption-mode", "cbc".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("key-id", "financial-key-456".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("data-classification", "highly-sensitive".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compliance", "pci-dss".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption-strength", "256-bit".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ AES-256 encrypted financial data sent: {} -> {} bytes (max security) to partition {}", 
                       originalBytes.length, encryptedBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("AES-256 encryption serialization failed", e);
        }
    }
    
    /**
     * Demonstrate Avro serialization (simulated).
     */
    private static void demonstrateAvroSerialization(KafkaMultiDatacenterClient client) {
        logger.info("=== Avro Serialization Demo (Simulated) ===");
        
        try {
            // Simulate Avro schema registration and serialization
            demonstrateAvroSchemaEvolution(client);
            demonstrateAvroWithCompression(client);
            
        } catch (Exception e) {
            logger.error("Avro serialization demonstration failed", e);
        }
    }
    
    private static void demonstrateAvroSchemaEvolution(KafkaMultiDatacenterClient client) {
        logger.info("🔄 Avro Schema Evolution (Simulated)");
        
        try {
            // Simulate Avro schema definition
            String avroSchemaV1 = """
                {
                  "type": "record",
                  "name": "UserEventV1",
                  "fields": [
                    {"name": "userId", "type": "string"},
                    {"name": "eventType", "type": "string"},
                    {"name": "timestamp", "type": "long"}
                  ]
                }
                """;
            
            String avroSchemaV2 = """
                {
                  "type": "record",
                  "name": "UserEventV2",
                  "fields": [
                    {"name": "userId", "type": "string"},
                    {"name": "eventType", "type": "string"},
                    {"name": "timestamp", "type": "long"},
                    {"name": "region", "type": ["null", "string"], "default": null},
                    {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null}
                  ]
                }
                """;
            
            // Simulate Avro binary serialization (we'll use JSON simulation)
            Map<String, Object> avroEventV1 = Map.of(
                "userId", "user-avro-123",
                "eventType", "click",
                "timestamp", System.currentTimeMillis()
            );
            
            Map<String, Object> avroEventV2 = Map.of(
                "userId", "user-avro-456",
                "eventType", "purchase",
                "timestamp", System.currentTimeMillis(),
                "region", "us-west",
                "metadata", Map.of("productId", "prod-789", "amount", "299.99")
            );
            
            // Send V1 format
            byte[] avroV1Bytes = objectMapper.writeValueAsBytes(avroEventV1);
            ProducerRecord<String, Object> recordV1 = new ProducerRecord<>(
                "serialization-avro-evolution",
                (String) avroEventV1.get("userId"),
                avroV1Bytes
            );
            recordV1.headers().add(new RecordHeader("content-type", "application/avro".getBytes(StandardCharsets.UTF_8)));
            recordV1.headers().add(new RecordHeader("avro-schema-version", "1".getBytes(StandardCharsets.UTF_8)));
            recordV1.headers().add(new RecordHeader("avro-schema-id", "user-event-v1".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadataV1 = client.producerSync().send(recordV1);
            logger.info("✅ Avro V1 event sent: {} to partition {}", avroEventV1, metadataV1.partition());
            
            // Send V2 format (backward compatible)
            byte[] avroV2Bytes = objectMapper.writeValueAsBytes(avroEventV2);
            ProducerRecord<String, Object> recordV2 = new ProducerRecord<>(
                "serialization-avro-evolution",
                (String) avroEventV2.get("userId"),
                avroV2Bytes
            );
            recordV2.headers().add(new RecordHeader("content-type", "application/avro".getBytes(StandardCharsets.UTF_8)));
            recordV2.headers().add(new RecordHeader("avro-schema-version", "2".getBytes(StandardCharsets.UTF_8)));
            recordV2.headers().add(new RecordHeader("avro-schema-id", "user-event-v2".getBytes(StandardCharsets.UTF_8)));
            recordV2.headers().add(new RecordHeader("backward-compatible", "true".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadataV2 = client.producerSync().send(recordV2);
            logger.info("✅ Avro V2 event sent: {} to partition {}", avroEventV2, metadataV2.partition());
            
        } catch (Exception e) {
            logger.error("Avro schema evolution demonstration failed", e);
        }
    }
    
    private static void demonstrateAvroWithCompression(KafkaMultiDatacenterClient client) {
        logger.info("📦 Avro with Compression (Simulated)");
        
        try {
            // Create large Avro-like dataset
            List<Map<String, Object>> avroRecords = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                avroRecords.add(Map.of(
                    "id", "record-" + i,
                    "timestamp", System.currentTimeMillis() + i,
                    "sensor_data", Map.of(
                        "temperature", 20.0 + Math.random() * 10,
                        "humidity", 40.0 + Math.random() * 20,
                        "pressure", 1013.25 + Math.random() * 10
                    ),
                    "location", Map.of(
                        "datacenter", "us-east",
                        "rack", "rack-" + (i % 5),
                        "server", "server-" + i
                    )
                ));
            }
            
            Map<String, Object> avroBatch = Map.of(
                "batchId", "batch-" + System.currentTimeMillis(),
                "records", avroRecords,
                "schema", "sensor-data-v1"
            );
            
            byte[] originalAvroBytes = objectMapper.writeValueAsBytes(avroBatch);
            byte[] compressedAvroBytes = gzipCompress(originalAvroBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-avro-compressed",
                (String) avroBatch.get("batchId"),
                compressedAvroBytes
            );
            
            // Add Avro + compression metadata
            record.headers().add(new RecordHeader("content-type", "application/avro".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "gzip".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("avro-schema-id", "sensor-data-v1".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("record-count", String.valueOf(avroRecords.size()).getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression-ratio", String.format("%.2f", (double) compressedAvroBytes.length / originalAvroBytes.length).getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Compressed Avro batch sent: {} records, {} -> {} bytes to partition {}", 
                       avroRecords.size(), originalAvroBytes.length, compressedAvroBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Avro with compression demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate schema evolution patterns.
     */
    private static void demonstrateSchemaEvolution(KafkaMultiDatacenterClient client) {
        logger.info("=== Schema Evolution Demo ===");
        
        try {
            demonstrateBackwardCompatibility(client);
            demonstrateForwardCompatibility(client);
            
        } catch (Exception e) {
            logger.error("Schema evolution demonstration failed", e);
        }
    }
    
    private static void demonstrateBackwardCompatibility(KafkaMultiDatacenterClient client) {
        logger.info("⬅️ Backward Compatibility Schema Evolution");
        
        try {
            // Old schema format
            Map<String, Object> oldFormatEvent = Map.of(
                "userId", "user-old-123",
                "action", "login",
                "timestamp", System.currentTimeMillis()
            );
            
            // New schema format (backward compatible - adds optional fields)
            Map<String, Object> newFormatEvent = Map.of(
                "userId", "user-new-456",
                "action", "login",
                "timestamp", System.currentTimeMillis(),
                "sessionId", "session-789", // New optional field
                "userAgent", "Mozilla/5.0...", // New optional field
                "ipAddress", "192.168.1.1", // New optional field
                "metadata", Map.of( // New optional nested object
                    "version", "2.0",
                    "features", Arrays.asList("feature1", "feature2")
                )
            );
            
            // Send old format
            String oldJson = objectMapper.writeValueAsString(oldFormatEvent);
            ProducerRecord<String, Object> oldRecord = new ProducerRecord<>(
                "serialization-backward-compatibility",
                (String) oldFormatEvent.get("userId"),
                oldJson
            );
            oldRecord.headers().add(new RecordHeader("schema-version", "1.0".getBytes(StandardCharsets.UTF_8)));
            oldRecord.headers().add(new RecordHeader("compatibility", "backward".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata oldMetadata = client.producerSync().send(oldRecord);
            logger.info("✅ Old format event sent: {} to partition {}", oldFormatEvent, oldMetadata.partition());
            
            // Send new format
            String newJson = objectMapper.writeValueAsString(newFormatEvent);
            ProducerRecord<String, Object> newRecord = new ProducerRecord<>(
                "serialization-backward-compatibility",
                (String) newFormatEvent.get("userId"),
                newJson
            );
            newRecord.headers().add(new RecordHeader("schema-version", "2.0".getBytes(StandardCharsets.UTF_8)));
            newRecord.headers().add(new RecordHeader("compatibility", "backward".getBytes(StandardCharsets.UTF_8)));
            newRecord.headers().add(new RecordHeader("migration-safe", "true".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata newMetadata = client.producerSync().send(newRecord);
            logger.info("✅ New format event sent: {} to partition {}", newFormatEvent, newMetadata.partition());
            
        } catch (Exception e) {
            logger.error("Backward compatibility demonstration failed", e);
        }
    }
    
    private static void demonstrateForwardCompatibility(KafkaMultiDatacenterClient client) {
        logger.info("➡️ Forward Compatibility Schema Evolution");
        
        try {
            // Future schema format that current consumers should be able to handle
            Map<String, Object> futureFormatEvent = Map.of(
                "userId", "user-future-789",
                "eventType", "advanced_interaction",
                "timestamp", System.currentTimeMillis(),
                "version", "3.0",
                // Core fields that current consumers understand
                "coreData", Map.of(
                    "action", "purchase",
                    "amount", 149.99,
                    "currency", "USD"
                ),
                // Future fields that current consumers should ignore
                "futureFeatures", Map.of(
                    "aiPrediction", Map.of(
                        "nextAction", "recommend_product",
                        "confidence", 0.85
                    ),
                    "advancedMetrics", Arrays.asList(
                        Map.of("metric", "engagement_score", "value", 8.5),
                        Map.of("metric", "loyalty_index", "value", 7.2)
                    )
                ),
                "experimentalData", "This field may not be supported by all consumers"
            );
            
            String futureJson = objectMapper.writeValueAsString(futureFormatEvent);
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-forward-compatibility",
                (String) futureFormatEvent.get("userId"),
                futureJson
            );
            
            // Add forward compatibility metadata
            record.headers().add(new RecordHeader("schema-version", "3.0".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compatibility", "forward".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("core-fields", "userId,eventType,timestamp,coreData".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("optional-fields", "futureFeatures,experimentalData".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("consumer-guidance", "ignore-unknown-fields".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Future format event sent: {} to partition {}", futureFormatEvent, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Forward compatibility demonstration failed", e);
        }
    }
    
    /**
     * Demonstrate per-datacenter serialization configuration.
     */
    private static void demonstratePerDatacenterSerialization(KafkaMultiDatacenterClient client) {
        logger.info("=== Per-Datacenter Serialization Demo ===");
        
        try {
            // Different serialization strategies per datacenter
            demonstrateRegionalSerializationPreferences(client);
            demonstrateComplianceBasedSerialization(client);
            
        } catch (Exception e) {
            logger.error("Per-datacenter serialization demonstration failed", e);
        }
    }
    
    private static void demonstrateRegionalSerializationPreferences(KafkaMultiDatacenterClient client) {
        logger.info("🌍 Regional Serialization Preferences");
        
        try {
            // US East: JSON with GZIP compression (bandwidth optimization)
            UserEvent usEvent = new UserEvent("user-us-123", "page_view", "us-east", 1.0);
            String usJson = objectMapper.writeValueAsString(usEvent);
            byte[] usCompressed = gzipCompress(usJson.getBytes(StandardCharsets.UTF_8));
            
            ProducerRecord<String, Object> usRecord = new ProducerRecord<>(
                "serialization-regional-us",
                usEvent.userId,
                usCompressed
            );
            usRecord.headers().add(new RecordHeader("datacenter", "us-east".getBytes(StandardCharsets.UTF_8)));
            usRecord.headers().add(new RecordHeader("serialization-strategy", "json-gzip".getBytes(StandardCharsets.UTF_8)));
            usRecord.headers().add(new RecordHeader("optimization", "bandwidth".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata usMetadata = client.producerSync().send(usRecord);
            logger.info("✅ US regional event sent: JSON+GZIP to partition {}", usMetadata.partition());
            
            // EU West: JSON with Snappy compression (GDPR compliance with fast processing)
            UserEvent euEvent = new UserEvent("user-eu-456", "data_access", "eu-west", 0.0);
            euEvent.metadata.put("gdprCompliant", true);
            euEvent.metadata.put("dataProcessor", "eu-processor");
            
            String euJson = objectMapper.writeValueAsString(euEvent);
            byte[] euCompressed = simulateSnappyCompression(euJson.getBytes(StandardCharsets.UTF_8));
            
            ProducerRecord<String, Object> euRecord = new ProducerRecord<>(
                "serialization-regional-eu",
                euEvent.userId,
                euCompressed
            );
            euRecord.headers().add(new RecordHeader("datacenter", "eu-west".getBytes(StandardCharsets.UTF_8)));
            euRecord.headers().add(new RecordHeader("serialization-strategy", "json-snappy".getBytes(StandardCharsets.UTF_8)));
            euRecord.headers().add(new RecordHeader("optimization", "processing-speed".getBytes(StandardCharsets.UTF_8)));
            euRecord.headers().add(new RecordHeader("compliance", "gdpr".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata euMetadata = client.producerSync().send(euRecord);
            logger.info("✅ EU regional event sent: JSON+Snappy (GDPR compliant) to partition {}", euMetadata.partition());
            
        } catch (Exception e) {
            logger.error("Regional serialization preferences demonstration failed", e);
        }
    }
    
    private static void demonstrateComplianceBasedSerialization(KafkaMultiDatacenterClient client) {
        logger.info("⚖️ Compliance-Based Serialization");
        
        try {
            // Financial data with encryption + compression
            Map<String, Object> financialEvent = Map.of(
                "transactionId", "txn-compliance-789",
                "customerId", "customer-456",
                "amount", 2500.00,
                "currency", "USD",
                "type", "international_transfer",
                "compliance", Map.of(
                    "amlChecked", true,
                    "kycVerified", true,
                    "sanctionsScreened", true,
                    "riskLevel", "medium"
                )
            );
            
            String financialJson = objectMapper.writeValueAsString(financialEvent);
            byte[] financialBytes = financialJson.getBytes(StandardCharsets.UTF_8);
            
            // Apply encryption for sensitive financial data
            SecretKey key = generateAESKey(256);
            byte[] encryptedBytes = encryptAES(financialBytes, key);
            
            // Then compress the encrypted data
            byte[] compressedEncryptedBytes = gzipCompress(encryptedBytes);
            
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "serialization-compliance-financial",
                (String) financialEvent.get("transactionId"),
                compressedEncryptedBytes
            );
            
            // Add compliance metadata
            record.headers().add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("serialization-strategy", "encrypt-then-compress".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("encryption", "aes-256".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compression", "gzip".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("compliance", "pci-dss,sox,aml".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("data-classification", "restricted".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("retention-policy", "7-years".getBytes(StandardCharsets.UTF_8)));
            
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("✅ Compliance financial event sent: {} -> {} -> {} bytes (encrypt->compress) to partition {}", 
                       financialBytes.length, encryptedBytes.length, compressedEncryptedBytes.length, metadata.partition());
            
        } catch (Exception e) {
            logger.error("Compliance-based serialization demonstration failed", e);
        }
    }
    
    // Utility methods for compression simulation
    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(data);
        }
        return baos.toByteArray();
    }
    
    private static byte[] gzipDecompress(byte[] compressedData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
        }
        return baos.toByteArray();
    }
    
    private static byte[] simulateSnappyCompression(byte[] data) {
        // Simulate Snappy compression (fast, moderate compression ratio)
        return Arrays.copyOf(data, (int) (data.length * 0.75)); // ~25% compression
    }
    
    private static byte[] simulateLz4Compression(byte[] data) {
        // Simulate LZ4 compression (balanced speed and compression)
        return Arrays.copyOf(data, (int) (data.length * 0.68)); // ~32% compression
    }
    
    private static byte[] simulateZstdCompression(byte[] data) {
        // Simulate ZSTD compression (high compression ratio)
        return Arrays.copyOf(data, (int) (data.length * 0.55)); // ~45% compression
    }
    
    // Utility methods for encryption
    private static SecretKey generateAESKey(int keySize) throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(keySize);
        return keyGenerator.generateKey();
    }
    
    private static byte[] encryptAES(byte[] data, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }
    
    private static byte[] decryptAES(byte[] encryptedData, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(encryptedData);
    }
    
    // Utility method for generating test data
    private static List<Map<String, Object>> generateRepeatedUserData(int count) {
        List<Map<String, Object>> users = new ArrayList<>();
        String[] actions = {"login", "logout", "page_view", "purchase", "search"};
        String[] regions = {"us-east", "us-west", "eu-west", "asia-pacific"};
        
        for (int i = 0; i < count; i++) {
            users.add(Map.of(
                "userId", "user-" + i,
                "action", actions[i % actions.length],
                "region", regions[i % regions.length],
                "timestamp", System.currentTimeMillis() + i,
                "sessionId", "session-" + (i / 5), // Group sessions
                "metadata", Map.of(
                    "browser", "Chrome",
                    "os", "Windows",
                    "device", "desktop"
                )
            ));
        }
        return users;
    }
    
    // Custom serializers for demonstration
    public static class JsonUserEventSerializer implements Serializer<UserEvent> {
        @Override
        public byte[] serialize(String topic, UserEvent data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize UserEvent", e);
            }
        }
    }
    
    public static class JsonUserEventDeserializer implements Deserializer<UserEvent> {
        @Override
        public UserEvent deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, UserEvent.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize UserEvent", e);
            }
        }
    }
    
    public static class CompressedJsonSerializer implements Serializer<Object> {
        @Override
        public byte[] serialize(String topic, Object data) {
            try {
                byte[] jsonBytes = objectMapper.writeValueAsBytes(data);
                return gzipCompress(jsonBytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize and compress", e);
            }
        }
    }
    
    public static class CompressedJsonDeserializer implements Deserializer<Object> {
        @Override
        public Object deserialize(String topic, byte[] data) {
            try {
                byte[] decompressed = gzipDecompress(data);
                return objectMapper.readValue(decompressed, Object.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to decompress and deserialize", e);
            }
        }
    }
}
