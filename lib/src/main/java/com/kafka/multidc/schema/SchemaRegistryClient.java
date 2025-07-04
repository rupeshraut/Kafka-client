package com.kafka.multidc.schema;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Schema Registry client interface for managing Avro/JSON schemas across datacenters.
 * Provides schema registration, retrieval, and evolution capabilities.
 */
public interface SchemaRegistryClient {
    
    /**
     * Register a schema for a subject.
     * 
     * @param subject the subject name (typically topic name + "-key" or "-value")
     * @param schema the schema definition (Avro or JSON Schema)
     * @return the schema ID assigned by the registry
     */
    CompletableFuture<Integer> registerSchema(String subject, String schema);
    
    /**
     * Register a schema for a subject in a specific datacenter.
     * 
     * @param datacenterId the datacenter ID
     * @param subject the subject name
     * @param schema the schema definition
     * @return the schema ID assigned by the registry
     */
    CompletableFuture<Integer> registerSchema(String datacenterId, String subject, String schema);
    
    /**
     * Get the latest schema for a subject.
     * 
     * @param subject the subject name
     * @return the latest schema definition
     */
    CompletableFuture<String> getLatestSchema(String subject);
    
    /**
     * Get a specific version of a schema for a subject.
     * 
     * @param subject the subject name
     * @param version the schema version
     * @return the schema definition
     */
    CompletableFuture<String> getSchema(String subject, int version);
    
    /**
     * Get schema by ID.
     * 
     * @param schemaId the schema ID
     * @return the schema definition
     */
    CompletableFuture<String> getSchemaById(int schemaId);
    
    /**
     * Get all versions of a schema for a subject.
     * 
     * @param subject the subject name
     * @return list of version numbers
     */
    CompletableFuture<java.util.List<Integer>> getVersions(String subject);
    
    /**
     * Check if a schema is compatible with the latest version.
     * 
     * @param subject the subject name
     * @param schema the schema to check
     * @return true if compatible, false otherwise
     */
    CompletableFuture<Boolean> isCompatible(String subject, String schema);
    
    /**
     * Delete a specific version of a schema.
     * 
     * @param subject the subject name
     * @param version the version to delete
     * @return the deleted version number
     */
    CompletableFuture<Integer> deleteSchemaVersion(String subject, int version);
    
    /**
     * Get all subjects registered in the schema registry.
     * 
     * @return list of subject names
     */
    CompletableFuture<java.util.List<String>> getAllSubjects();
    
    /**
     * Get configuration for a subject.
     * 
     * @param subject the subject name
     * @return configuration map
     */
    CompletableFuture<Map<String, String>> getSubjectConfig(String subject);
    
    /**
     * Update configuration for a subject.
     * 
     * @param subject the subject name
     * @param config the configuration to update
     * @return completion future
     */
    CompletableFuture<Void> updateSubjectConfig(String subject, Map<String, String> config);
    
    /**
     * Close the schema registry client and release resources.
     */
    void close();
}
