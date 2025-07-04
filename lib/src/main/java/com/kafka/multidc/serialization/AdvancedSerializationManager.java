package com.kafka.multidc.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Advanced serialization support for multi-datacenter Kafka operations.
 * Provides enterprise-grade serialization with schema evolution, compression,
 * and encryption capabilities.
 */
public interface AdvancedSerializationManager {
    
    /**
     * Serialization format types supported by the advanced serialization manager.
     * 
     * <p>Each format provides different trade-offs between performance, 
     * schema evolution support, and ecosystem compatibility.
     */
    enum SerializationFormat {
        /** JSON format for human-readable, cross-platform compatibility */
        JSON,
        /** Apache Avro format for schema evolution and compact binary representation */
        AVRO,
        /** Protocol Buffers format for high performance and strong typing */
        PROTOBUF,
        /** Custom user-defined serialization format */
        CUSTOM
    }
    
    /**
     * Compression algorithm types for reducing message size and network overhead.
     * 
     * <p>Different algorithms provide varying levels of compression ratio vs. speed trade-offs.
     */
    enum CompressionType {
        /** No compression applied */
        NONE,
        /** GZIP compression (higher compression ratio, slower) */
        GZIP,
        /** Snappy compression (balanced performance and compression) */
        SNAPPY,
        /** LZ4 compression (fastest compression, good for real-time) */
        LZ4,
        /** Zstandard compression (excellent compression ratio and speed) */
        ZSTD
    }
    
    /**
     * Encryption algorithm types for securing sensitive data in transit and at rest.
     */
    enum EncryptionType {
        /** No encryption applied */
        NONE,
        /** AES 128-bit encryption */
        AES_128,
        /** AES 256-bit encryption (stronger security) */
        AES_256
    }
    
    /**
     * Serialization configuration interface providing immutable configuration
     * for serialization behavior across datacenters.
     * 
     * <p>This configuration controls all aspects of message serialization including
     * format selection, compression, encryption, and schema registry integration.
     */
    interface SerializationConfig {
        /**
         * Returns the serialization format.
         * 
         * @return the configured serialization format
         */
        SerializationFormat getFormat();
        
        /**
         * Returns the compression algorithm.
         * 
         * @return the configured compression type
         */
        CompressionType getCompression();
        
        /**
         * Returns the encryption algorithm.
         * 
         * @return the configured encryption type
         */
        EncryptionType getEncryption();
        
        /**
         * Returns the schema registry URL.
         * 
         * @return the schema registry URL, or null if not configured
         */
        String getSchemaRegistryUrl();
        
        /**
         * Returns additional configuration properties.
         * 
         * @return an immutable map of additional properties
         */
        Map<String, Object> getProperties();
        
        /**
         * Returns whether schema validation is enabled.
         * 
         * @return true if schema validation is enabled, false otherwise
         */
        boolean isSchemaValidationEnabled();
        
        /**
         * Returns whether backward compatibility checking is enabled.
         * 
         * @return true if backward compatibility is enabled, false otherwise
         */
        boolean isBackwardCompatibilityEnabled();
    }
    
    /**
     * Advanced serializer with multi-datacenter support and enhanced capabilities.
     * 
     * <p>Extends the standard Kafka Serializer interface with additional methods
     * for datacenter-aware serialization, compression, encryption, and schema validation.
     * 
     * @param <T> the type of objects this serializer can serialize
     */
    interface AdvancedSerializer<T> extends Serializer<T> {
        
        /**
         * Serialize with datacenter-specific configuration.
         * 
         * @param topic the Kafka topic name
         * @param datacenterId the target datacenter identifier
         * @param data the object to serialize
         * @return the serialized byte array
         */
        byte[] serialize(String topic, String datacenterId, T data);
        
        /**
         * Serialize with compression applied.
         * 
         * @param topic the Kafka topic name
         * @param data the object to serialize
         * @param compression the compression algorithm to apply
         * @return the compressed serialized byte array
         */
        byte[] serializeCompressed(String topic, T data, CompressionType compression);
        
        /**
         * Serialize with encryption applied.
         * 
         * @param topic the Kafka topic name
         * @param data the object to serialize
         * @param encryption the encryption algorithm to apply
         * @return the encrypted serialized byte array
         */
        byte[] serializeEncrypted(String topic, T data, EncryptionType encryption);
        
        /**
         * Serialize with schema validation.
         * 
         * @param topic the Kafka topic name
         * @param data the object to serialize
         * @param schemaId the schema identifier for validation
         * @return the schema-validated serialized byte array
         */
        byte[] serializeWithSchema(String topic, T data, String schemaId);
    }
    
    /**
     * Advanced deserializer with multi-datacenter support and enhanced capabilities.
     * 
     * <p>Extends the standard Kafka Deserializer interface with additional methods
     * for datacenter-aware deserialization, decompression, decryption, and schema validation.
     * 
     * @param <T> the type of objects this deserializer can deserialize
     */
    interface AdvancedDeserializer<T> extends Deserializer<T> {
        
        /**
         * Deserialize with datacenter-specific configuration.
         * 
         * @param topic the Kafka topic name
         * @param datacenterId the source datacenter identifier
         * @param data the byte array to deserialize
         * @return the deserialized object
         */
        T deserialize(String topic, String datacenterId, byte[] data);
        
        /**
         * Deserialize compressed data.
         * 
         * @param topic the Kafka topic name
         * @param data the compressed byte array to deserialize
         * @param compression the compression algorithm that was applied
         * @return the deserialized object
         */
        T deserializeCompressed(String topic, byte[] data, CompressionType compression);
        
        /**
         * Deserialize encrypted data.
         * 
         * @param topic the Kafka topic name
         * @param data the encrypted byte array to deserialize
         * @param encryption the encryption algorithm that was applied
         * @return the deserialized object
         */
        T deserializeEncrypted(String topic, byte[] data, EncryptionType encryption);
        
        /**
         * Deserialize with schema validation.
         * 
         * @param topic the Kafka topic name
         * @param data the byte array to deserialize
         * @param expectedSchemaId the expected schema identifier for validation
         * @return the schema-validated deserialized object
         */
        T deserializeWithSchema(String topic, byte[] data, String expectedSchemaId);
    }
    
    /**
     * Register a serializer for a specific type.
     * 
     * @param <T> the type to register a serializer for
     * @param type the class of the type to serialize
     * @param serializer the advanced serializer implementation
     */
    <T> void registerSerializer(Class<T> type, AdvancedSerializer<T> serializer);
    
    /**
     * Register a deserializer for a specific type.
     * 
     * @param <T> the type to register a deserializer for
     * @param type the class of the type to deserialize
     * @param deserializer the advanced deserializer implementation
     */
    <T> void registerDeserializer(Class<T> type, AdvancedDeserializer<T> deserializer);
    
    /**
     * Get serializer for a type.
     * 
     * @param <T> the type to get a serializer for
     * @param type the class of the type to serialize
     * @return the registered serializer for the type, or null if not found
     */
    <T> AdvancedSerializer<T> getSerializer(Class<T> type);
    
    /**
     * Get deserializer for a type.
     * 
     * @param <T> the type to get a deserializer for
     * @param type the class of the type to deserialize
     * @return the registered deserializer for the type, or null if not found
     */
    <T> AdvancedDeserializer<T> getDeserializer(Class<T> type);
    
    /**
     * Configure serialization for a datacenter.
     * 
     * @param datacenterId the datacenter identifier
     * @param config the serialization configuration to apply
     */
    void configureDatacenterSerialization(String datacenterId, SerializationConfig config);
    
    /**
     * Get serialization configuration for a datacenter.
     * 
     * @param datacenterId the datacenter identifier
     * @return the serialization configuration for the datacenter
     */
    SerializationConfig getSerializationConfig(String datacenterId);
    
    /**
     * Validate schema compatibility between versions.
     * 
     * @param topic the Kafka topic name
     * @param schemaId the current schema identifier
     * @param newSchema the new schema definition to validate
     * @return true if the new schema is compatible, false otherwise
     */
    boolean validateSchemaCompatibility(String topic, String schemaId, String newSchema);
    
    /**
     * Get schema evolution history for a topic.
     * 
     * @param topic the Kafka topic name
     * @return a list of schema identifiers in chronological order
     */
    java.util.List<String> getSchemaEvolutionHistory(String topic);
    
    /**
     * Builder for creating serialization configuration instances.
     * 
     * <p>Provides a fluent API for configuring all aspects of serialization
     * including format, compression, encryption, and schema registry settings.
     */
    interface ConfigBuilder {
        /**
         * Sets the serialization format.
         * 
         * @param format the serialization format to use
         * @return this builder instance for method chaining
         */
        ConfigBuilder format(SerializationFormat format);
        
        /**
         * Sets the compression algorithm.
         * 
         * @param compression the compression algorithm to use
         * @return this builder instance for method chaining
         */
        ConfigBuilder compression(CompressionType compression);
        
        /**
         * Sets the encryption algorithm.
         * 
         * @param encryption the encryption algorithm to use
         * @return this builder instance for method chaining
         */
        ConfigBuilder encryption(EncryptionType encryption);
        
        /**
         * Sets the schema registry URL.
         * 
         * @param url the schema registry URL
         * @return this builder instance for method chaining
         */
        ConfigBuilder schemaRegistry(String url);
        
        /**
         * Enables schema validation for serialization operations.
         * 
         * @return this builder instance for method chaining
         */
        ConfigBuilder enableSchemaValidation();
        
        /**
         * Enables backward compatibility checking for schema evolution.
         * 
         * @return this builder instance for method chaining
         */
        ConfigBuilder enableBackwardCompatibility();
        
        /**
         * Sets additional configuration properties.
         * 
         * @param properties additional configuration properties
         * @return this builder instance for method chaining
         */
        ConfigBuilder properties(Map<String, Object> properties);
        
        /**
         * Builds the serialization configuration.
         * 
         * @return a new SerializationConfig instance with the specified settings
         */
        SerializationConfig build();
    }
    
    /**
     * Create a new configuration builder.
     * 
     * @return a new ConfigBuilder instance
     */
    ConfigBuilder newConfigBuilder();
}
