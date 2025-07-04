package com.kafka.multidc;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Isolated test for configuration validation issues.
 */
class ConfigurationValidationTest {
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testEmptyDatacentersValidation() {
        System.out.println("Starting empty datacenters validation test");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            System.out.println("About to build configuration with empty datacenters");
            KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
                .build();
            System.out.println("Configuration built unexpectedly: " + config);
        });
        
        System.out.println("Exception caught: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("At least one datacenter must be configured"));
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)  
    void testInvalidLocalDatacenterValidation() {
        System.out.println("Starting invalid local datacenter validation test");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            System.out.println("About to build configuration with invalid local datacenter");
            KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
                .addDatacenter(KafkaDatacenterEndpoint.builder()
                    .id("test-dc")
                    .bootstrapServers("localhost:9092")
                    .build())
                .localDatacenter("non-existent-dc")
                .build();
            System.out.println("Configuration built unexpectedly: " + config);
        });
        
        System.out.println("Exception caught: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("Local datacenter ID not found"));
    }
            @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testValidConfiguration() {
        System.out.println("Starting valid configuration test");
        
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(KafkaDatacenterEndpoint.builder()
                .id("test-dc")
                .bootstrapServers("localhost:9092")
                .build())
            .localDatacenter("test-dc")
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(10))
            .build();
            
        System.out.println("Valid configuration created successfully");
        assertNotNull(config);
        assertEquals(1, config.getDatacenters().size());
        assertEquals("test-dc", config.getLocalDatacenterId());
    }
}
