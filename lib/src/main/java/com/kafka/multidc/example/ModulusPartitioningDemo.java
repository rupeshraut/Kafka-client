package com.kafka.multidc.example;

import com.kafka.multidc.partitioning.ProducerPartitioningManager;
import com.kafka.multidc.partitioning.ProducerPartitioningType;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple demonstration of the Modulus Partitioning Strategy.
 */
public class ModulusPartitioningDemo {
    
    public static void main(String[] args) {
        System.out.println("=== Modulus Partitioning Strategy Demo ===\n");
        
        // Create ProducerPartitioningManager
        ProducerPartitioningManager manager = new ProducerPartitioningManager();
        
        // Set modulus strategy
        manager.setPartitioningStrategy(ProducerPartitioningType.MODULUS);
        
        System.out.println("⚡ Modulus Partitioning Strategy");
        System.out.println("Uses simple modulus operation: partition = abs(key.hashCode()) % numPartitions\n");
        
        // Test with different keys
        int numPartitions = 3;
        String[] testKeys = {
            "user1", "user2", "user3", "user1",  // Test consistency
            "product-100", "product-200", "order-456",
            "session-abc", "session-def", "metric-xyz"
        };
        
        System.out.println("Testing with " + numPartitions + " partitions:");
        System.out.println("Key\t\t\tHash Code\t\tPartition");
        System.out.println("---\t\t\t---------\t\t---------");
        
        Map<String, Integer> seenKeys = new HashMap<>();
        
        for (String key : testKeys) {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                "test-topic", key, "test-value"
            );
            
            Integer partition = manager.determinePartition(record, numPartitions);
            int hashCode = key.hashCode();
            
            // Check for consistency
            String consistency = "";
            if (seenKeys.containsKey(key)) {
                if (seenKeys.get(key).equals(partition)) {
                    consistency = " ✓ (consistent)";
                } else {
                    consistency = " ✗ (inconsistent!)";
                }
            } else {
                seenKeys.put(key, partition);
            }
            
            System.out.printf("%-15s\t%10d\t\t%9d%s%n", key, hashCode, partition, consistency);
        }
        
        // Show distribution
        System.out.println("\n📊 Partition Distribution:");
        Map<Integer, Integer> distribution = new HashMap<>();
        for (String key : testKeys) {
            ProducerRecord<String, Object> record = new ProducerRecord<>("test-topic", key, "value");
            Integer partition = manager.determinePartition(record, numPartitions);
            distribution.put(partition, distribution.getOrDefault(partition, 0) + 1);
        }
        
        for (int i = 0; i < numPartitions; i++) {
            int count = distribution.getOrDefault(i, 0);
            String bar = "█".repeat(count);
            System.out.printf("Partition %d: %2d messages %s%n", i, count, bar);
        }
        
        // Compare with key-hash strategy
        System.out.println("\n🔍 Comparison with Key-Hash Strategy:");
        manager.setPartitioningStrategy(ProducerPartitioningType.KEY_HASH);
        
        System.out.println("Key\t\t\tModulus\t\tKey-Hash\tSame?");
        System.out.println("---\t\t\t-------\t\t--------\t----");
        
        manager.setPartitioningStrategy(ProducerPartitioningType.MODULUS);
        for (String key : Arrays.asList("user1", "product-100", "session-abc")) {
            ProducerRecord<String, Object> record = new ProducerRecord<>("test-topic", key, "value");
            
            Integer modulusPartition = manager.determinePartition(record, numPartitions);
            
            manager.setPartitioningStrategy(ProducerPartitioningType.KEY_HASH);
            Integer keyHashPartition = manager.determinePartition(record, numPartitions);
            
            String same = modulusPartition.equals(keyHashPartition) ? "✓" : "✗";
            System.out.printf("%-15s\t%7d\t\t%8d\t%4s%n", key, modulusPartition, keyHashPartition, same);
            
            manager.setPartitioningStrategy(ProducerPartitioningType.MODULUS);
        }
        
        System.out.println("\n✨ Modulus Partitioning Benefits:");
        System.out.println("• Faster than MD5-based key-hash (no cryptographic hashing)");
        System.out.println("• Consistent partitioning for the same key");
        System.out.println("• Simple and predictable algorithm");
        System.out.println("• Good distribution for most key patterns");
        System.out.println("• Handles Integer.MIN_VALUE edge case safely");
        
        System.out.println("\n🚀 Use Cases:");
        System.out.println("• High-throughput scenarios where performance matters");
        System.out.println("• Applications with well-distributed keys");
        System.out.println("• When you need predictable partitioning without MD5 overhead");
        
        System.out.println("\n=== Demo Complete ===");
    }
}
