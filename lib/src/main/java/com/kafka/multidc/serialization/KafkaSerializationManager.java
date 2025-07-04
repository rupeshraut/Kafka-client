package com.kafka.multidc.serialization;

import com.kafka.multidc.config.SchemaRegistryConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Advanced serialization support for Kafka Multi-Datacenter Client.
 * Provides Avro, JSON Schema, Protobuf serialization with schema registry integration.
 */
public interface KafkaSerializationManager {
    
    /**
     * Serialization format types.
     */
    enum SerializationFormat {
        AVRO,
        JSON_SCHEMA,
        PROTOBUF,
        JSON,
        STRING,
        BYTE_ARRAY,
        CUSTOM
    }
    
    /**
     * Schema evolution compatibility modes.
     */
    enum CompatibilityMode {
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }
    
    /**
     * Serializer configuration.
     */
    interface SerializerConfig {
        SerializationFormat getFormat();
        String getSchemaString();
        Class<?> getTargetClass();
        CompatibilityMode getCompatibilityMode();
        Map<String, Object> getProperties();
        boolean isKeySerializer();
        String getSubject();
    }
    
    /**
     * Deserializer configuration.
     */
    interface DeserializerConfig {
        SerializationFormat getFormat();
        String getSchemaString();
        Class<?> getTargetClass();
        Map<String, Object> getProperties();
        boolean isKeyDeserializer();
        String getSubject();
        boolean isSpecificRecordReader();
    }
    
    /**
     * Schema information.
     */
    interface SchemaInfo {
        int getId();
        String getSubject();
        int getVersion();
        String getSchema();
        SerializationFormat getFormat();
        CompatibilityMode getCompatibility();
        Map<String, String> getMetadata();
    }
    
    /**
     * Create a serializer for the specified configuration.
     */
    <T> Serializer<T> createSerializer(SerializerConfig config);
    
    /**
     * Create a deserializer for the specified configuration.
     */
    <T> Deserializer<T> createDeserializer(DeserializerConfig config);
    
    /**
     * Create an Avro serializer.
     */
    <T> Serializer<T> createAvroSerializer(Class<T> clazz, boolean isKey);
    
    /**
     * Create an Avro deserializer.
     */
    <T> Deserializer<T> createAvroDeserializer(Class<T> clazz, boolean isKey);
    
    /**
     * Create a JSON Schema serializer.
     */
    <T> Serializer<T> createJsonSchemaSerializer(Class<T> clazz, boolean isKey);
    
    /**
     * Create a JSON Schema deserializer.
     */
    <T> Deserializer<T> createJsonSchemaDeserializer(Class<T> clazz, boolean isKey);
    
    /**
     * Create a Protobuf serializer.
     */
    <T> Serializer<T> createProtobufSerializer(Class<T> clazz, boolean isKey);
    
    /**
     * Create a Protobuf deserializer.
     */
    <T> Deserializer<T> createProtobufDeserializer(Class<T> clazz, boolean isKey);
    
    /**
     * Register a schema for a subject.
     */
    CompletableFuture<SchemaInfo> registerSchema(String subject, String schema, SerializationFormat format);
    
    /**
     * Get the latest schema for a subject.
     */
    CompletableFuture<SchemaInfo> getLatestSchema(String subject);
    
    /**
     * Get a specific version of a schema.
     */
    CompletableFuture<SchemaInfo> getSchema(String subject, int version);
    
    /**
     * Check schema compatibility.
     */
    CompletableFuture<Boolean> isCompatible(String subject, String schema, SerializationFormat format);
    
    /**
     * Evolve a schema (register a new version).
     */
    CompletableFuture<SchemaInfo> evolveSchema(String subject, String newSchema, SerializationFormat format);
    
    /**
     * Set compatibility mode for a subject.
     */
    CompletableFuture<Void> setCompatibility(String subject, CompatibilityMode mode);
    
    /**
     * Get all subjects.
     */
    CompletableFuture<java.util.List<String>> getSubjects();
    
    /**
     * Delete a schema version.
     */
    CompletableFuture<Void> deleteSchema(String subject, int version);
    
    /**
     * Get schema registry configuration.
     */
    SchemaRegistryConfig getSchemaRegistryConfig();
    
    /**
     * Builder for serializer configuration.
     */
    interface SerializerConfigBuilder {
        SerializerConfigBuilder format(SerializationFormat format);
        SerializerConfigBuilder schema(String schema);
        SerializerConfigBuilder targetClass(Class<?> clazz);
        SerializerConfigBuilder compatibility(CompatibilityMode mode);
        SerializerConfigBuilder properties(Map<String, Object> properties);
        SerializerConfigBuilder property(String key, Object value);
        SerializerConfigBuilder isKey(boolean isKey);
        SerializerConfigBuilder subject(String subject);
        SerializerConfig build();
    }
    
    /**
     * Builder for deserializer configuration.
     */
    interface DeserializerConfigBuilder {
        DeserializerConfigBuilder format(SerializationFormat format);
        DeserializerConfigBuilder schema(String schema);
        DeserializerConfigBuilder targetClass(Class<?> clazz);
        DeserializerConfigBuilder properties(Map<String, Object> properties);
        DeserializerConfigBuilder property(String key, Object value);
        DeserializerConfigBuilder isKey(boolean isKey);
        DeserializerConfigBuilder subject(String subject);
        DeserializerConfigBuilder specificRecordReader(boolean specificReader);
        DeserializerConfig build();
    }
    
    /**
     * Create a serializer configuration builder.
     */
    SerializerConfigBuilder serializerConfig();
    
    /**
     * Create a deserializer configuration builder.
     */
    DeserializerConfigBuilder deserializerConfig();
    
    /**
     * Validate a schema.
     */
    CompletableFuture<Boolean> validateSchema(String schema, SerializationFormat format);
    
    /**
     * Generate a Java class from an Avro schema.
     */
    CompletableFuture<String> generateAvroClass(String schema, String className, String packageName);
    
    /**
     * Close the serialization manager and release resources.
     */
    void close();
}
