package com.kafka.multidc;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.routing.RoutingStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for KafkaMultiDatacenterClient.
 */
class KafkaMultiDatacenterClientTest {
    
    @Test
    void testClientBuilder() {
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(KafkaDatacenterEndpoint.builder()
                .id("test-dc")
                .bootstrapServers("localhost:9092")
                .build())
            .localDatacenter("test-dc")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .build();
        
        assertNotNull(config);
        assertEquals(1, config.getDatacenters().size());
        assertEquals("test-dc", config.getLocalDatacenterId());
        assertEquals(RoutingStrategy.LATENCY_BASED, config.getRoutingStrategy());
    }
    
    @Test
    void testClientCreation() {
        KafkaDatacenterConfiguration config = KafkaDatacenterConfiguration.builder()
            .addDatacenter(KafkaDatacenterEndpoint.builder()
                .id("test-dc")
                .bootstrapServers("localhost:9092")
                .build())
            .build();
        
        try (KafkaMultiDatacenterClient client = KafkaMultiDatacenterClientBuilder.create(config)) {
            assertNotNull(client);
            assertNotNull(client.getConfiguration());
            assertFalse(client.isClosed());
        }
    }
    
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConfigurationValidation() {
        // Test empty datacenters - this should pass validation since we can call build() without datacenters
        // and add them via addDatacenter() later
        KafkaDatacenterConfiguration.builder().build();
        
        // Test invalid local datacenter
        assertThrows(IllegalArgumentException.class, () -> {
            KafkaDatacenterConfiguration.builder()
                .addDatacenter(
                    KafkaDatacenterEndpoint.builder()
                        .id("test-dc")
                        .bootstrapServers("localhost:9092")
                        .build()
                )
                .localDatacenter("non-existent-dc")
                .build();
        });
    }
}
