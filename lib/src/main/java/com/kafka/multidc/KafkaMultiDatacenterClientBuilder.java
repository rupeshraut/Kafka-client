package com.kafka.multidc;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.impl.DefaultKafkaMultiDatacenterClient;

/**
 * Builder for creating KafkaMultiDatacenterClient instances.
 * Provides a fluent API for configuring and creating clients.
 */
public class KafkaMultiDatacenterClientBuilder {
    
    private KafkaDatacenterConfiguration configuration;
    
    private KafkaMultiDatacenterClientBuilder() {
    }
    
    /**
     * Create a new builder instance.
     *
     * @return new builder
     */
    public static KafkaMultiDatacenterClientBuilder create() {
        return new KafkaMultiDatacenterClientBuilder();
    }
    
    /**
     * Create a client with the given configuration.
     *
     * @param configuration datacenter configuration
     * @return new client instance directly
     */
    public static KafkaMultiDatacenterClient create(KafkaDatacenterConfiguration configuration) {
        return new KafkaMultiDatacenterClientBuilder().configuration(configuration).build();
    }
    
    /**
     * Set the datacenter configuration.
     *
     * @param configuration datacenter configuration
     * @return this builder
     */
    public KafkaMultiDatacenterClientBuilder configuration(KafkaDatacenterConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
    
    /**
     * Build the KafkaMultiDatacenterClient.
     *
     * @return configured client instance
     * @throws IllegalStateException if configuration is not set
     */
    public KafkaMultiDatacenterClient build() {
        if (configuration == null) {
            throw new IllegalStateException("Configuration must be set before building client");
        }
        
        return new DefaultKafkaMultiDatacenterClient(configuration);
    }
}
