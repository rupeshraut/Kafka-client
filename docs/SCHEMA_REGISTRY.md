# Schema Registry Guide

This guide provides comprehensive information on using Schema Registry integration with the Kafka Multi-Datacenter Client Library, including Avro, JSON Schema, and Protobuf support.

## Table of Contents

- [Overview](#overview)
- [Configuration](#configuration)
- [Avro Integration](#avro-integration)
- [JSON Schema Integration](#json-schema-integration)
- [Protobuf Integration](#protobuf-integration)
- [Schema Evolution](#schema-evolution)
- [Multi-Datacenter Schema Registry](#multi-datacenter-schema-registry)
- [Security and Authentication](#security-and-authentication)
- [Performance Optimization](#performance-optimization)
- [Monitoring and Health Checks](#monitoring-and-health-checks)
- [Best Practices](#best-practices)

## Overview

The Kafka Multi-Datacenter Client Library provides comprehensive Schema Registry integration supporting:

- **Multiple Schema Formats**: Avro, JSON Schema, and Protobuf
- **Schema Evolution**: Forward, backward, and full compatibility
- **Multi-Datacenter Support**: Schema registry failover and locality
- **Caching**: Intelligent schema caching for performance
- **Security**: SSL/TLS and authentication support
- **Health Monitoring**: Schema registry health checks and metrics

## Configuration

### Basic Schema Registry Configuration

```java
SchemaRegistryConfig schemaConfig = SchemaRegistryConfig.builder()
    .urls("http://schema-registry:8081")
    .basicAuth("schema-user", "schema-password")
    .connectionTimeout(Duration.ofSeconds(10))
    .readTimeout(Duration.ofSeconds(30))
    .retries(3, Duration.ofMillis(1000))
    .build();

KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(createDatacenterEndpoint())
    .localDatacenter("primary")
    .schemaRegistryConfig(schemaConfig)
    .build();
```

### Advanced Schema Registry Configuration

```java
SchemaRegistryConfig advancedConfig = SchemaRegistryConfig.builder()
    // Multiple URLs for high availability
    .urls("http://schema-registry-1:8081", "http://schema-registry-2:8081")
    
    // Authentication
    .basicAuth("schema-client", loadPassword("schema-registry"))
    
    // SSL/TLS security
    .security(SecurityConfig.builder()
        .securityProtocol("SSL")
        .sslTruststoreLocation("/etc/kafka/ssl/truststore.jks")
        .sslTruststorePassword(loadPassword("truststore"))
        .build())
    
    // Connection and retry settings
    .connectionTimeout(Duration.ofSeconds(15))
    .readTimeout(Duration.ofMinutes(1))
    .retries(5, Duration.ofSeconds(2))
    
    // Schema caching
    .cache(true, 2000, Duration.ofMinutes(30))
    .subjectNameStrategy("io.confluent.kafka.serializers.subject.TopicNameStrategy")
    
    // Failover configuration
    .failoverEnabled(true)
    .healthCheckInterval(Duration.ofSeconds(30))
    
    // Datacenter-specific URLs
    .datacenterUrls(Map.of(
        "us-east-1", List.of("http://schema-east-1:8081", "http://schema-east-2:8081"),
        "us-west-2", List.of("http://schema-west-1:8081", "http://schema-west-2:8081")
    ))
    
    .build();
```

### Integration with Kafka Configuration

```java
// Producer configuration with schema registry - use additional properties
ProducerConfig producerConfig = ProducerConfig.defaultConfig();

KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .producerConfig(producerConfig)
    .schemaRegistryConfig(schemaRegistryConfig)
    .additionalProperties(Map.of(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "schema.registry.url", "http://schema-registry:8081",
        "auto.register.schemas", false,
        "use.latest.version", true,
        "latest.compatibility.strict", true
    ))
    .build();

// Consumer configuration with schema registry - use additional properties
ConsumerConfig consumerConfig = ConsumerConfig.defaultConfig();

KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
    .addDatacenter(datacenterEndpoint)
    .consumerConfig(consumerConfig)
    .schemaRegistryConfig(schemaRegistryConfig)
    .additionalProperties(Map.of(
        "group.id", "schema-aware-consumer",
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        "schema.registry.url", "http://schema-registry:8081",
        "specific.avro.reader", true
    ))
    .build();
```

## Avro Integration

### Schema Definition and Registration

```java
public class AvroSchemaManager {
    
    private final SchemaRegistryClient schemaRegistryClient;
    
    public AvroSchemaManager(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
    }
    
    public void registerUserEventSchema() {
        String schemaString = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "com.example.events",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}}
              ]
            }
            """;
        
        try {
            Schema schema = new Schema.Parser().parse(schemaString);
            String subject = "user-events-value";
            
            int schemaId = schemaRegistryClient.register(subject, schema);
            logger.info("Registered schema with ID: {}", schemaId);
            
        } catch (Exception e) {
            logger.error("Failed to register schema", e);
            throw new RuntimeException("Schema registration failed", e);
        }
    }
}
```

### Avro Producer Implementation

```java
public class AvroProducerExample {
    
    private final KafkaMultiDatacenterClient client;
    private final Schema userEventSchema;
    
    public AvroProducerExample(KafkaMultiDatacenterClient client) {
        this.client = client;
        this.userEventSchema = loadUserEventSchema();
    }
    
    public void sendUserEvent(String userId, String eventType, Map<String, String> properties) {
        // Create Avro record
        GenericRecord userEvent = new GenericData.Record(userEventSchema);
        userEvent.put("userId", userId);
        userEvent.put("eventType", eventType);
        userEvent.put("timestamp", Instant.now().toEpochMilli());
        userEvent.put("properties", properties);
        
        // Create producer record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
            "user-events",
            userId,
            userEvent
        );
        
        // Send asynchronously
        CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
        
        future
            .thenAccept(metadata -> {
                logger.info("Avro message sent: partition={}, offset={}", 
                           metadata.partition(), metadata.offset());
            })
            .exceptionally(throwable -> {
                logger.error("Failed to send Avro message", throwable);
                return null;
            });
    }
    
    public void sendSpecificRecord(UserEvent userEvent) {
        ProducerRecord<String, UserEvent> record = new ProducerRecord<>(
            "user-events-specific",
            userEvent.getUserId(),
            userEvent
        );
        
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("Specific Avro record sent: partition={}, offset={}", 
                       metadata.partition(), metadata.offset());
        } catch (Exception e) {
            logger.error("Failed to send specific Avro record", e);
        }
    }
    
    private Schema loadUserEventSchema() {
        try {
            String subject = "user-events-value";
            SchemaMetadata metadata = client.getSchemaRegistryClient().getLatestSchemaMetadata(subject);
            return new Schema.Parser().parse(metadata.getSchema());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load user event schema", e);
        }
    }
}
```

### Avro Consumer Implementation

```java
public class AvroConsumerExample {
    
    private final KafkaMultiDatacenterClient client;
    
    public AvroConsumerExample(KafkaMultiDatacenterClient client) {
        this.client = client;
    }
    
    public void consumeUserEvents() {
        client.consumerSync().subscribe(List.of("user-events"));
        
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord userEvent = record.value();
                    
                    String userId = userEvent.get("userId").toString();
                    String eventType = userEvent.get("eventType").toString();
                    Long timestamp = (Long) userEvent.get("timestamp");
                    
                    @SuppressWarnings("unchecked")
                    Map<String, String> properties = (Map<String, String>) userEvent.get("properties");
                    
                    logger.info("Consumed Avro record: userId={}, eventType={}, timestamp={}", 
                               userId, eventType, timestamp);
                    
                    processUserEvent(userId, eventType, timestamp, properties);
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Avro consumer error", e);
        }
    }
    
    public void consumeSpecificRecords() {
        client.consumerSync().subscribe(List.of("user-events-specific"));
        
        try {
            while (true) {
                ConsumerRecords<String, UserEvent> records = client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, UserEvent> record : records) {
                    UserEvent userEvent = record.value();
                    
                    logger.info("Consumed specific Avro record: {}", userEvent);
                    processSpecificUserEvent(userEvent);
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Specific Avro consumer error", e);
        }
    }
    
    private void processUserEvent(String userId, String eventType, Long timestamp, Map<String, String> properties) {
        // Business logic for processing user events
        logger.info("Processing user event: {} - {} at {}", userId, eventType, timestamp);
    }
    
    private void processSpecificUserEvent(UserEvent userEvent) {
        // Business logic for processing specific user events
        logger.info("Processing specific user event: {}", userEvent.toString());
    }
}
```

## JSON Schema Integration

### JSON Schema Definition and Registration

```java
public class JsonSchemaManager {
    
    private final SchemaRegistryClient schemaRegistryClient;
    
    public JsonSchemaManager(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
    }
    
    public void registerOrderSchema() {
        String jsonSchema = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "title": "Order",
              "description": "An order in the system",
              "properties": {
                "orderId": {
                  "type": "string",
                  "description": "Unique order identifier"
                },
                "customerId": {
                  "type": "string",
                  "description": "Customer identifier"
                },
                "items": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "productId": {"type": "string"},
                      "quantity": {"type": "integer", "minimum": 1},
                      "price": {"type": "number", "minimum": 0}
                    },
                    "required": ["productId", "quantity", "price"]
                  }
                },
                "totalAmount": {
                  "type": "number",
                  "minimum": 0
                },
                "orderDate": {
                  "type": "string",
                  "format": "date-time"
                }
              },
              "required": ["orderId", "customerId", "items", "totalAmount", "orderDate"]
            }
            """;
        
        try {
            String subject = "orders-value";
            ParsedSchema parsedSchema = new JsonSchema(jsonSchema);
            
            int schemaId = schemaRegistryClient.register(subject, parsedSchema);
            logger.info("Registered JSON schema with ID: {}", schemaId);
            
        } catch (Exception e) {
            logger.error("Failed to register JSON schema", e);
            throw new RuntimeException("JSON schema registration failed", e);
        }
    }
}
```

### JSON Schema Producer

```java
public class JsonSchemaProducerExample {
    
    private final KafkaMultiDatacenterClient client;
    
    public JsonSchemaProducerExample(KafkaMultiDatacenterClient client) {
        this.client = client;
    }
    
    public void sendOrder(Order order) {
        ProducerRecord<String, Order> record = new ProducerRecord<>(
            "orders",
            order.getOrderId(),
            order
        );
        
        CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
        
        future
            .thenAccept(metadata -> {
                logger.info("JSON Schema order sent: orderId={}, partition={}, offset={}", 
                           order.getOrderId(), metadata.partition(), metadata.offset());
            })
            .exceptionally(throwable -> {
                logger.error("Failed to send JSON Schema order", throwable);
                return null;
            });
    }
    
    public void sendOrderBatch(List<Order> orders) {
        List<CompletableFuture<RecordMetadata>> futures = orders.stream()
            .map(order -> {
                ProducerRecord<String, Order> record = new ProducerRecord<>(
                    "orders",
                    order.getOrderId(),
                    order
                );
                return client.producerAsync().sendAsync(record);
            })
            .collect(Collectors.toList());
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenRun(() -> {
                logger.info("JSON Schema order batch sent: {} orders", orders.size());
            })
            .exceptionally(throwable -> {
                logger.error("Failed to send JSON Schema order batch", throwable);
                return null;
            });
    }
}
```

### JSON Schema Consumer

```java
public class JsonSchemaConsumerExample {
    
    private final KafkaMultiDatacenterClient client;
    private final OrderProcessor orderProcessor;
    
    public JsonSchemaConsumerExample(KafkaMultiDatacenterClient client, OrderProcessor orderProcessor) {
        this.client = client;
        this.orderProcessor = orderProcessor;
    }
    
    public void consumeOrders() {
        client.consumerSync().subscribe(List.of("orders"));
        
        try {
            while (true) {
                ConsumerRecords<String, Order> records = client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, Order> record : records) {
                    Order order = record.value();
                    
                    logger.info("Consumed JSON Schema order: orderId={}, customerId={}, totalAmount={}", 
                               order.getOrderId(), order.getCustomerId(), order.getTotalAmount());
                    
                    try {
                        orderProcessor.processOrder(order);
                        logger.info("Successfully processed order: {}", order.getOrderId());
                    } catch (Exception e) {
                        logger.error("Failed to process order: {}", order.getOrderId(), e);
                        handleOrderProcessingError(order, e);
                    }
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("JSON Schema consumer error", e);
        }
    }
    
    private void handleOrderProcessingError(Order order, Exception error) {
        // Send to dead letter queue or retry logic
        logger.warn("Sending order {} to error handling", order.getOrderId());
        // Implementation depends on error handling strategy
    }
}
```

## Protobuf Integration

### Protobuf Schema Definition

```protobuf
// user_event.proto
syntax = "proto3";

package com.example.events;

option java_package = "com.example.events";
option java_outer_classname = "UserEventProtos";

message UserEvent {
  string user_id = 1;
  string event_type = 2;
  int64 timestamp = 3;
  map<string, string> properties = 4;
  optional string session_id = 5;
  repeated string tags = 6;
}

message UserEventBatch {
  repeated UserEvent events = 1;
  string batch_id = 2;
  int64 batch_timestamp = 3;
}
```

### Protobuf Schema Registration

```java
public class ProtobufSchemaManager {
    
    private final SchemaRegistryClient schemaRegistryClient;
    
    public ProtobufSchemaManager(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
    }
    
    public void registerUserEventProtobufSchema() {
        try {
            // Load protobuf schema
            Descriptors.Descriptor descriptor = UserEventProtos.UserEvent.getDescriptor();
            ProtobufSchema protobufSchema = ProtobufSchema.of(descriptor);
            
            String subject = "user-events-protobuf-value";
            int schemaId = schemaRegistryClient.register(subject, protobufSchema);
            
            logger.info("Registered Protobuf schema with ID: {}", schemaId);
            
        } catch (Exception e) {
            logger.error("Failed to register Protobuf schema", e);
            throw new RuntimeException("Protobuf schema registration failed", e);
        }
    }
}
```

### Protobuf Producer

```java
public class ProtobufProducerExample {
    
    private final KafkaMultiDatacenterClient client;
    
    public ProtobufProducerExample(KafkaMultiDatacenterClient client) {
        this.client = client;
    }
    
    public void sendUserEvent(String userId, String eventType, Map<String, String> properties) {
        UserEventProtos.UserEvent userEvent = UserEventProtos.UserEvent.newBuilder()
            .setUserId(userId)
            .setEventType(eventType)
            .setTimestamp(Instant.now().toEpochMilli())
            .putAllProperties(properties)
            .build();
        
        ProducerRecord<String, UserEventProtos.UserEvent> record = new ProducerRecord<>(
            "user-events-protobuf",
            userId,
            userEvent
        );
        
        CompletableFuture<RecordMetadata> future = client.producerAsync().sendAsync(record);
        
        future
            .thenAccept(metadata -> {
                logger.info("Protobuf message sent: userId={}, partition={}, offset={}", 
                           userId, metadata.partition(), metadata.offset());
            })
            .exceptionally(throwable -> {
                logger.error("Failed to send Protobuf message", throwable);
                return null;
            });
    }
    
    public void sendUserEventBatch(List<UserEventProtos.UserEvent> events) {
        UserEventProtos.UserEventBatch batch = UserEventProtos.UserEventBatch.newBuilder()
            .addAllEvents(events)
            .setBatchId(UUID.randomUUID().toString())
            .setBatchTimestamp(Instant.now().toEpochMilli())
            .build();
        
        ProducerRecord<String, UserEventProtos.UserEventBatch> record = new ProducerRecord<>(
            "user-events-protobuf-batch",
            batch.getBatchId(),
            batch
        );
        
        try {
            RecordMetadata metadata = client.producerSync().send(record);
            logger.info("Protobuf batch sent: batchId={}, events={}, partition={}, offset={}", 
                       batch.getBatchId(), events.size(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            logger.error("Failed to send Protobuf batch", e);
        }
    }
}
```

### Protobuf Consumer

```java
public class ProtobufConsumerExample {
    
    private final KafkaMultiDatacenterClient client;
    
    public ProtobufConsumerExample(KafkaMultiDatacenterClient client) {
        this.client = client;
    }
    
    public void consumeUserEvents() {
        client.consumerSync().subscribe(List.of("user-events-protobuf"));
        
        try {
            while (true) {
                ConsumerRecords<String, UserEventProtos.UserEvent> records = 
                    client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, UserEventProtos.UserEvent> record : records) {
                    UserEventProtos.UserEvent userEvent = record.value();
                    
                    logger.info("Consumed Protobuf record: userId={}, eventType={}, timestamp={}", 
                               userEvent.getUserId(), userEvent.getEventType(), userEvent.getTimestamp());
                    
                    processProtobufUserEvent(userEvent);
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Protobuf consumer error", e);
        }
    }
    
    public void consumeUserEventBatches() {
        client.consumerSync().subscribe(List.of("user-events-protobuf-batch"));
        
        try {
            while (true) {
                ConsumerRecords<String, UserEventProtos.UserEventBatch> records = 
                    client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, UserEventProtos.UserEventBatch> record : records) {
                    UserEventProtos.UserEventBatch batch = record.value();
                    
                    logger.info("Consumed Protobuf batch: batchId={}, events={}", 
                               batch.getBatchId(), batch.getEventsCount());
                    
                    for (UserEventProtos.UserEvent event : batch.getEventsList()) {
                        processProtobufUserEvent(event);
                    }
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Protobuf batch consumer error", e);
        }
    }
    
    private void processProtobufUserEvent(UserEventProtos.UserEvent userEvent) {
        logger.info("Processing Protobuf user event: {} - {}", 
                   userEvent.getUserId(), userEvent.getEventType());
        
        // Business logic for processing user events
        Map<String, String> properties = userEvent.getPropertiesMap();
        properties.forEach((key, value) -> 
            logger.debug("Event property: {}={}", key, value));
    }
}
```

## Schema Evolution

### Schema Compatibility Management

```java
public class SchemaEvolutionManager {
    
    private final SchemaRegistryClient schemaRegistryClient;
    
    public SchemaEvolutionManager(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
    }
    
    public void demonstrateSchemaEvolution() {
        String subject = "user-events-value";
        
        // Register initial schema (v1)
        registerInitialSchema(subject);
        
        // Evolve schema (v2) - add optional field
        evolveSchemaV2(subject);
        
        // Evolve schema (v3) - add required field with default
        evolveSchemaV3(subject);
        
        // Check compatibility
        checkSchemaCompatibility(subject);
    }
    
    private void registerInitialSchema(String subject) {
        String schemaV1 = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "com.example.events",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }
            """;
        
        try {
            Schema schema = new Schema.Parser().parse(schemaV1);
            int schemaId = schemaRegistryClient.register(subject, schema);
            logger.info("Registered schema v1 with ID: {}", schemaId);
        } catch (Exception e) {
            logger.error("Failed to register schema v1", e);
        }
    }
    
    private void evolveSchemaV2(String subject) {
        String schemaV2 = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "com.example.events",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "sessionId", "type": ["null", "string"], "default": null}
              ]
            }
            """;
        
        try {
            Schema schema = new Schema.Parser().parse(schemaV2);
            
            // Check compatibility before registering
            boolean isCompatible = schemaRegistryClient.testCompatibility(subject, schema);
            if (!isCompatible) {
                throw new RuntimeException("Schema v2 is not compatible with existing schemas");
            }
            
            int schemaId = schemaRegistryClient.register(subject, schema);
            logger.info("Registered schema v2 with ID: {}", schemaId);
        } catch (Exception e) {
            logger.error("Failed to register schema v2", e);
        }
    }
    
    private void evolveSchemaV3(String subject) {
        String schemaV3 = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "com.example.events",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "sessionId", "type": ["null", "string"], "default": null},
                {"name": "source", "type": "string", "default": "unknown"}
              ]
            }
            """;
        
        try {
            Schema schema = new Schema.Parser().parse(schemaV3);
            
            boolean isCompatible = schemaRegistryClient.testCompatibility(subject, schema);
            if (!isCompatible) {
                throw new RuntimeException("Schema v3 is not compatible with existing schemas");
            }
            
            int schemaId = schemaRegistryClient.register(subject, schema);
            logger.info("Registered schema v3 with ID: {}", schemaId);
        } catch (Exception e) {
            logger.error("Failed to register schema v3", e);
        }
    }
    
    private void checkSchemaCompatibility(String subject) {
        try {
            // Get all schema versions
            List<Integer> versions = schemaRegistryClient.getAllVersions(subject);
            logger.info("Available schema versions for {}: {}", subject, versions);
            
            // Check compatibility levels
            String compatibilityLevel = schemaRegistryClient.getCompatibility(subject);
            logger.info("Compatibility level for {}: {}", subject, compatibilityLevel);
            
            // Get latest schema
            SchemaMetadata latestSchema = schemaRegistryClient.getLatestSchemaMetadata(subject);
            logger.info("Latest schema version: {}, ID: {}", 
                       latestSchema.getVersion(), latestSchema.getId());
            
        } catch (Exception e) {
            logger.error("Failed to check schema compatibility", e);
        }
    }
}
```

### Handling Schema Evolution in Consumers

```java
public class SchemaEvolutionConsumer {
    
    private final KafkaMultiDatacenterClient client;
    private final SchemaRegistryClient schemaRegistryClient;
    
    public SchemaEvolutionConsumer(KafkaMultiDatacenterClient client) {
        this.client = client;
        this.schemaRegistryClient = client.getSchemaRegistryClient();
    }
    
    public void consumeEvolvingUserEvents() {
        client.consumerSync().subscribe(List.of("user-events"));
        
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = client.consumerSync().poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord userEvent = record.value();
                    
                    // Handle different schema versions
                    handleUserEventRecord(userEvent);
                }
                
                if (!records.isEmpty()) {
                    client.consumerSync().commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Schema evolution consumer error", e);
        }
    }
    
    private void handleUserEventRecord(GenericRecord record) {
        String userId = record.get("userId").toString();
        String eventType = record.get("eventType").toString();
        Long timestamp = (Long) record.get("timestamp");
        
        // Handle optional fields that may not exist in older versions
        String sessionId = getOptionalField(record, "sessionId");
        String source = getOptionalField(record, "source", "unknown");
        
        logger.info("Processing evolved user event: userId={}, eventType={}, sessionId={}, source={}", 
                   userId, eventType, sessionId, source);
        
        processUserEvent(userId, eventType, timestamp, sessionId, source);
    }
    
    private String getOptionalField(GenericRecord record, String fieldName) {
        return getOptionalField(record, fieldName, null);
    }
    
    private String getOptionalField(GenericRecord record, String fieldName, String defaultValue) {
        try {
            Object value = record.get(fieldName);
            return value != null ? value.toString() : defaultValue;
        } catch (Exception e) {
            // Field doesn't exist in this schema version
            return defaultValue;
        }
    }
    
    private void processUserEvent(String userId, String eventType, Long timestamp, 
                                 String sessionId, String source) {
        // Business logic that handles all schema versions
        logger.info("Processing user event from {} at {}", source, timestamp);
    }
}
```

## Multi-Datacenter Schema Registry

### Datacenter-Aware Schema Registry Configuration

```java
public class MultiDatacenterSchemaRegistry {
    
    public static SchemaRegistryConfig createMultiDatacenterConfig() {
        return SchemaRegistryConfig.builder()
            // Primary schema registry URLs
            .urls("http://schema-registry-primary:8081")
            
            // Datacenter-specific URLs
            .datacenterUrls(Map.of(
                "us-east-1", List.of(
                    "http://schema-east-1:8081", 
                    "http://schema-east-2:8081"
                ),
                "us-west-2", List.of(
                    "http://schema-west-1:8081", 
                    "http://schema-west-2:8081"
                ),
                "eu-west-1", List.of(
                    "http://schema-eu-1:8081"
                )
            ))
            
            // Failover configuration
            .failover(true)
            .healthCheckInterval(Duration.ofSeconds(30))
            
            // Caching to reduce cross-datacenter calls
            .cache(true, 5000, Duration.ofHours(1))
            
            // Security configuration
            .basicAuth("schema-client", loadPassword("schema-registry"))
            
            .build();
    }
    
    private static String loadPassword(String service) {
        return System.getenv("SCHEMA_REGISTRY_PASSWORD");
    }
}
```

### Schema Registry Health Monitoring

```java
@Component
public class SchemaRegistryHealthMonitor {
    
    private final SchemaRegistryClient schemaRegistryClient;
    private final MeterRegistry meterRegistry;
    
    public SchemaRegistryHealthMonitor(SchemaRegistryClient client, MeterRegistry meterRegistry) {
        this.schemaRegistryClient = client;
        this.meterRegistry = meterRegistry;
    }
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void checkSchemaRegistryHealth() {
        try {
            // Check connectivity
            long startTime = System.currentTimeMillis();
            List<String> subjects = schemaRegistryClient.getAllSubjects();
            long responseTime = System.currentTimeMillis() - startTime;
            
            // Record metrics
            meterRegistry.timer("schema.registry.response.time").record(responseTime, TimeUnit.MILLISECONDS);
            meterRegistry.gauge("schema.registry.subjects.count", subjects.size());
            
            logger.info("Schema Registry health check: {} subjects, {}ms response time", 
                       subjects.size(), responseTime);
            
            // Check each datacenter
            checkDatacenterConnectivity();
            
        } catch (Exception e) {
            logger.error("Schema Registry health check failed", e);
            meterRegistry.counter("schema.registry.health.failures").increment();
        }
    }
    
    private void checkDatacenterConnectivity() {
        Map<String, List<String>> datacenterUrls = schemaRegistryClient.getDatacenterUrls();
        
        datacenterUrls.forEach((datacenter, urls) -> {
            for (String url : urls) {
                try {
                    long startTime = System.currentTimeMillis();
                    // Perform health check against specific URL
                    checkUrlHealth(url);
                    long responseTime = System.currentTimeMillis() - startTime;
                    
                    meterRegistry.timer("schema.registry.datacenter.response.time")
                        .tags("datacenter", datacenter, "url", url)
                        .record(responseTime, TimeUnit.MILLISECONDS);
                        
                } catch (Exception e) {
                    logger.warn("Schema Registry health check failed for {} ({}): {}", 
                               datacenter, url, e.getMessage());
                    
                    meterRegistry.counter("schema.registry.datacenter.failures")
                        .tags("datacenter", datacenter, "url", url)
                        .increment();
                }
            }
        });
    }
    
    private void checkUrlHealth(String url) throws Exception {
        // Implementation to check specific URL health
        // This could be a simple HTTP GET to /subjects endpoint
    }
}
```

## Performance Optimization

### Schema Caching Strategies

```java
public class SchemaRegistryPerformanceOptimizer {
    
    private final SchemaRegistryClient schemaRegistryClient;
    private final LoadingCache<Integer, Schema> schemaCache;
    private final LoadingCache<String, Integer> latestVersionCache;
    
    public SchemaRegistryPerformanceOptimizer(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
        
        // Configure schema cache
        this.schemaCache = Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(Duration.ofMinutes(30))
            .recordStats()
            .build(this::loadSchema);
        
        // Configure latest version cache
        this.latestVersionCache = Caffeine.newBuilder()
            .maximumSize(500)
            .expireAfterWrite(Duration.ofMinutes(5))
            .recordStats()
            .build(this::loadLatestVersion);
    }
    
    public Schema getSchemaById(int schemaId) {
        try {
            return schemaCache.get(schemaId);
        } catch (Exception e) {
            logger.error("Failed to get schema by ID: {}", schemaId, e);
            throw new RuntimeException("Schema not found: " + schemaId, e);
        }
    }
    
    public int getLatestVersion(String subject) {
        try {
            return latestVersionCache.get(subject);
        } catch (Exception e) {
            logger.error("Failed to get latest version for subject: {}", subject, e);
            throw new RuntimeException("Latest version not found: " + subject, e);
        }
    }
    
    private Schema loadSchema(Integer schemaId) throws Exception {
        logger.debug("Loading schema from registry: {}", schemaId);
        return schemaRegistryClient.getById(schemaId);
    }
    
    private Integer loadLatestVersion(String subject) throws Exception {
        logger.debug("Loading latest version from registry: {}", subject);
        SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        return metadata.getVersion();
    }
    
    @Scheduled(fixedRate = 60000) // Every minute
    public void reportCacheStats() {
        CacheStats schemaStats = schemaCache.stats();
        CacheStats versionStats = latestVersionCache.stats();
        
        logger.info("Schema cache stats: hit_rate={:.2f}%, miss_count={}, eviction_count={}", 
                   schemaStats.hitRate() * 100, schemaStats.missCount(), schemaStats.evictionCount());
        
        logger.info("Version cache stats: hit_rate={:.2f}%, miss_count={}, eviction_count={}", 
                   versionStats.hitRate() * 100, versionStats.missCount(), versionStats.evictionCount());
    }
}
```

### Batch Schema Operations

```java
public class BatchSchemaOperations {
    
    private final SchemaRegistryClient schemaRegistryClient;
    
    public BatchSchemaOperations(SchemaRegistryClient client) {
        this.schemaRegistryClient = client;
    }
    
    public Map<String, Integer> registerSchemaBatch(Map<String, Schema> schemas) {
        Map<String, Integer> results = new ConcurrentHashMap<>();
        
        // Use parallel processing for batch registration
        schemas.entrySet().parallelStream().forEach(entry -> {
            String subject = entry.getKey();
            Schema schema = entry.getValue();
            
            try {
                int schemaId = schemaRegistryClient.register(subject, schema);
                results.put(subject, schemaId);
                logger.info("Registered schema for subject {}: ID {}", subject, schemaId);
            } catch (Exception e) {
                logger.error("Failed to register schema for subject: {}", subject, e);
                results.put(subject, -1); // Error marker
            }
        });
        
        return results;
    }
    
    public Map<String, SchemaMetadata> getLatestSchemasBatch(List<String> subjects) {
        Map<String, SchemaMetadata> results = new ConcurrentHashMap<>();
        
        subjects.parallelStream().forEach(subject -> {
            try {
                SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
                results.put(subject, metadata);
            } catch (Exception e) {
                logger.error("Failed to get latest schema for subject: {}", subject, e);
            }
        });
        
        return results;
    }
    
    public Map<String, Boolean> testCompatibilityBatch(Map<String, Schema> schemasToTest) {
        Map<String, Boolean> results = new ConcurrentHashMap<>();
        
        schemasToTest.entrySet().parallelStream().forEach(entry -> {
            String subject = entry.getKey();
            Schema schema = entry.getValue();
            
            try {
                boolean isCompatible = schemaRegistryClient.testCompatibility(subject, schema);
                results.put(subject, isCompatible);
                logger.info("Compatibility test for {}: {}", subject, isCompatible);
            } catch (Exception e) {
                logger.error("Failed to test compatibility for subject: {}", subject, e);
                results.put(subject, false);
            }
        });
        
        return results;
    }
}
```

## Best Practices

### 1. **Schema Design Best Practices**

```java
// Good: Use optional fields for backward compatibility
{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "eventType", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "sessionId", "type": ["null", "string"], "default": null}, // Optional
    {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}

// Good: Use defaults for new required fields
{
  "name": "version", 
  "type": "string", 
  "default": "1.0"
}

// Good: Use logical types for better semantics
{
  "name": "timestamp", 
  "type": "long", 
  "logicalType": "timestamp-millis"
}
```

### 2. **Schema Evolution Best Practices**

```java
public class SchemaEvolutionBestPractices {
    
    // Always test compatibility before registering
    public void evolveSchema(String subject, Schema newSchema) {
        try {
            boolean isCompatible = schemaRegistryClient.testCompatibility(subject, newSchema);
            if (!isCompatible) {
                throw new IllegalArgumentException("Schema is not compatible with existing versions");
            }
            
            int schemaId = schemaRegistryClient.register(subject, newSchema);
            logger.info("Successfully evolved schema for {}: ID {}", subject, schemaId);
            
        } catch (Exception e) {
            logger.error("Schema evolution failed for subject: {}", subject, e);
            throw e;
        }
    }
    
    // Use meaningful schema names and namespaces
    public Schema createWellNamedSchema() {
        return SchemaBuilder.record("UserRegistrationEvent")
            .namespace("com.company.user.events.v1")
            .fields()
            .requiredString("userId")
            .requiredString("email")
            .optionalLong("timestamp")
            .endRecord();
    }
}
```

### 3. **Performance Best Practices**

```java
// Enable caching for better performance
SchemaRegistryConfig.builder()
    .cache(true, 2000, Duration.ofMinutes(30))
    .build();

// Use connection pooling
.connectionTimeout(Duration.ofSeconds(10))
.readTimeout(Duration.ofSeconds(30))

// Batch operations when possible
public void registerMultipleSchemas(Map<String, Schema> schemas) {
    // Use parallel processing for independent operations
    schemas.entrySet().parallelStream().forEach(entry -> {
        registerSchema(entry.getKey(), entry.getValue());
    });
}
```

### 4. **Security Best Practices**

```java
SchemaRegistryConfig.builder()
    // Use SSL for production
    .security(SecurityConfig.builder()
        .securityProtocol("SSL")
        .sslTruststoreLocation("/etc/kafka/ssl/truststore.jks")
        .sslTruststorePassword(loadPassword("truststore"))
        .build())
    
    // Use authentication
    .basicAuth("schema-client", loadSecurePassword())
    
    // Validate SSL certificates
    .sslEndpointIdentificationAlgorithm("https")
    
    .build();
```

### 5. **Monitoring Best Practices**

```java
@Component
public class SchemaRegistryMonitoring {
    
    @EventListener
    public void handleSchemaRegistryEvent(SchemaRegistryEvent event) {
        switch (event.getType()) {
            case SCHEMA_REGISTERED:
                logger.info("Schema registered: subject={}, version={}", 
                           event.getSubject(), event.getVersion());
                break;
            case COMPATIBILITY_CHECK_FAILED:
                logger.error("Compatibility check failed: subject={}, error={}", 
                            event.getSubject(), event.getError());
                break;
            case CONNECTION_FAILED:
                logger.error("Schema Registry connection failed: {}", event.getError());
                triggerAlert("Schema Registry connection failure");
                break;
        }
    }
    
    private void triggerAlert(String message) {
        // Implementation for alerting system
    }
}
```

For more examples and detailed implementation, see the [examples directory](../lib/src/main/java/com/kafka/multidc/example/).
