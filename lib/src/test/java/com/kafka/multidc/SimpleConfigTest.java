package com.kafka.multidc;

import com.kafka.multidc.config.KafkaDatacenterEndpoint;

/**
 * Simple test to debug configuration hanging issues.
 */
public class SimpleConfigTest {
    
    public static void main(String[] args) {
        System.out.println("Starting simple configuration test...");
        
        try {
            System.out.println("Creating endpoint...");
            KafkaDatacenterEndpoint endpoint = KafkaDatacenterEndpoint.builder()
                .id("test-dc")
                .bootstrapServers("localhost:9092")
                .build();
            System.out.println("Endpoint created: " + endpoint);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("Test completed");
    }
}
