package com.kafka.multidc.partitioning;

import com.kafka.multidc.partitioning.impl.BuiltInPartitioningStrategies;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the ModulusPartitioningStrategy.
 */
class ModulusPartitioningStrategyTest {

    private BuiltInPartitioningStrategies.ModulusPartitioningStrategy<String, String> strategy;

    @BeforeEach
    void setUp() {
        strategy = new BuiltInPartitioningStrategies.ModulusPartitioningStrategy<>();
    }

    @Test
    void testPartitionWithNullKey() {
        // Given
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", null, "test-value");
        int numPartitions = 3;

        // When
        Integer partition = strategy.partition(record, numPartitions);

        // Then
        assertNull(partition, "Should return null for null keys to let Kafka handle it");
    }

    @Test
    void testPartitionWithStringKey() {
        // Given
        String key = "test-key";
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, "test-value");
        int numPartitions = 3;

        // When
        Integer partition = strategy.partition(record, numPartitions);

        // Then
        assertNotNull(partition);
        assertTrue(partition >= 0 && partition < numPartitions, 
                  "Partition should be between 0 and " + (numPartitions - 1));
        
        // Verify it's consistent
        int expectedPartition = Math.abs(key.hashCode()) % numPartitions;
        assertEquals(expectedPartition, partition, "Should use modulus of key hash");
    }

    @Test
    void testConsistentPartitioning() {
        // Given
        String key = "consistent-key";
        ProducerRecord<String, String> record1 = new ProducerRecord<>("test-topic", key, "value1");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic", key, "value2");
        int numPartitions = 5;

        // When
        Integer partition1 = strategy.partition(record1, numPartitions);
        Integer partition2 = strategy.partition(record2, numPartitions);

        // Then
        assertEquals(partition1, partition2, "Same key should always map to same partition");
    }

    @Test
    void testDifferentKeys() {
        // Given
        String key1 = "key1";
        String key2 = "key2";
        ProducerRecord<String, String> record1 = new ProducerRecord<>("test-topic", key1, "value1");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic", key2, "value2");
        int numPartitions = 3;

        // When
        Integer partition1 = strategy.partition(record1, numPartitions);
        Integer partition2 = strategy.partition(record2, numPartitions);

        // Then
        assertNotNull(partition1);
        assertNotNull(partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);
        assertTrue(partition2 >= 0 && partition2 < numPartitions);
        
        // Different keys might map to different partitions (not guaranteed, but likely)
        // This is just a sanity check
        int expectedPartition1 = Math.abs(key1.hashCode()) % numPartitions;
        int expectedPartition2 = Math.abs(key2.hashCode()) % numPartitions;
        assertEquals(expectedPartition1, partition1);
        assertEquals(expectedPartition2, partition2);
    }

    @Test
    void testIntegerMinValueEdgeCase() {
        // Given - create a key that produces Integer.MIN_VALUE hash
        // This is a tricky edge case because Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE (negative)
        String specialKey = createKeyWithMinValueHash();
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", specialKey, "value");
        int numPartitions = 3;

        // When
        Integer partition = strategy.partition(record, numPartitions);

        // Then
        assertNotNull(partition);
        assertTrue(partition >= 0 && partition < numPartitions, 
                  "Even with Integer.MIN_VALUE hash, partition should be valid");
    }

    @Test
    void testStrategyName() {
        // When
        String name = strategy.getStrategyName();

        // Then
        assertEquals("modulus", name);
    }

    @Test
    void testDistribution() {
        // Given
        int numPartitions = 3;
        int[] partitionCounts = new int[numPartitions];
        
        // When - send messages with different keys
        for (int i = 0; i < 30; i++) {
            String key = "key" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, "value" + i);
            Integer partition = strategy.partition(record, numPartitions);
            assertNotNull(partition);
            partitionCounts[partition]++;
        }

        // Then - verify all partitions are used
        for (int count : partitionCounts) {
            assertTrue(count > 0, "All partitions should receive some messages");
        }
    }

    /**
     * Creates a string key that should produce Integer.MIN_VALUE as its hash code.
     * This is used to test the edge case handling.
     */
    private String createKeyWithMinValueHash() {
        // We'll try a few candidates; if none produce MIN_VALUE, we'll use a known one
        String[] candidates = {"polygenelubricants", "GgG", "CGCG"};
        
        for (String candidate : candidates) {
            if (candidate.hashCode() == Integer.MIN_VALUE) {
                return candidate;
            }
        }
        
        // If we can't find one naturally, create a mock scenario by using a custom key class
        // For this test, we'll just use a string that we know will cause issues
        return "test-min-value-key"; // This won't actually produce MIN_VALUE, but tests the logic
    }
}
