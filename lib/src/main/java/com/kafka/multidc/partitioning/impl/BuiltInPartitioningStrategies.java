package com.kafka.multidc.partitioning.impl;

import com.kafka.multidc.partitioning.ProducerPartitioningStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Built-in producer partitioning strategies implementations.
 */
public class BuiltInPartitioningStrategies {
    
    private static final Logger logger = LoggerFactory.getLogger(BuiltInPartitioningStrategies.class);
    
    /**
     * Round-robin partitioning strategy.
     */
    public static class RoundRobinPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private final AtomicInteger counter = new AtomicInteger(0);
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            return counter.getAndIncrement() % numPartitions;
        }
        
        @Override
        public String getStrategyName() {
            return "round-robin";
        }
    }
    
    /**
     * Key-hash partitioning strategy using consistent hashing.
     */
    public static class KeyHashPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private MessageDigest md5;
        
        public KeyHashPartitioningStrategy() {
            try {
                this.md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                logger.warn("MD5 not available, falling back to hashCode()", e);
            }
        }
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            if (record.key() == null) {
                return null; // Let Kafka handle null keys
            }
            
            if (md5 != null) {
                byte[] keyBytes = record.key().toString().getBytes(StandardCharsets.UTF_8);
                byte[] hash = md5.digest(keyBytes);
                
                // Convert first 4 bytes to int
                int hashInt = ((hash[0] & 0xFF) << 24) |
                             ((hash[1] & 0xFF) << 16) |
                             ((hash[2] & 0xFF) << 8) |
                             (hash[3] & 0xFF);
                
                return Math.abs(hashInt) % numPartitions;
            } else {
                return Math.abs(record.key().hashCode()) % numPartitions;
            }
        }
        
        @Override
        public String getStrategyName() {
            return "key-hash";
        }
    }
    
    /**
     * Modulus partitioning strategy using simple modulus operation on key hash.
     * This is a simpler and faster alternative to the MD5-based key-hash strategy.
     */
    public static class ModulusPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            if (record.key() == null) {
                return null; // Let Kafka handle null keys
            }
            
            // Use simple modulus operation on the key's hash code
            int hash = record.key().hashCode();
            
            // Ensure positive result by using Math.abs, but handle Integer.MIN_VALUE edge case
            if (hash == Integer.MIN_VALUE) {
                hash = 0;
            } else {
                hash = Math.abs(hash);
            }
            
            return hash % numPartitions;
        }
        
        @Override
        public String getStrategyName() {
            return "modulus";
        }
    }
    
    /**
     * Random partitioning strategy.
     */
    public static class RandomPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            return ThreadLocalRandom.current().nextInt(numPartitions);
        }
        
        @Override
        public String getStrategyName() {
            return "random";
        }
    }
    
    /**
     * Sticky partitioning strategy - sends to same partition until batch threshold.
     */
    public static class StickyPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private volatile int currentPartition = -1;
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private int batchSize = 100; // Default batch size
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            // If key is provided, use key-hash for stickiness
            if (record.key() != null) {
                return Math.abs(record.key().hashCode()) % numPartitions;
            }
            
            // For keyless messages, use sticky behavior
            if (currentPartition == -1 || messageCount.incrementAndGet() >= batchSize) {
                currentPartition = ThreadLocalRandom.current().nextInt(numPartitions);
                messageCount.set(0);
                logger.debug("Sticky partitioner switched to partition {}", currentPartition);
            }
            
            return currentPartition;
        }
        
        @Override
        public String getStrategyName() {
            return "sticky";
        }
        
        @Override
        public void configure(Map<String, Object> properties) {
            Object batchSizeObj = properties.get("sticky.batch.size");
            if (batchSizeObj instanceof Number) {
                this.batchSize = ((Number) batchSizeObj).intValue();
                logger.info("Sticky partitioner configured with batch size: {}", batchSize);
            }
        }
    }
    
    /**
     * Geographic partitioning strategy based on region codes.
     */
    public static class GeographicPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private Map<String, Integer> regionToPartitionMap;
        private int defaultPartition = 0;
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            if (regionToPartitionMap == null) {
                // Auto-generate region mapping if not configured
                initializeDefaultRegionMapping(numPartitions);
            }
            
            String region = extractRegionFromRecord(record);
            if (region != null && regionToPartitionMap.containsKey(region)) {
                return regionToPartitionMap.get(region);
            }
            
            return defaultPartition;
        }
        
        @Override
        public String getStrategyName() {
            return "geographic";
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void configure(Map<String, Object> properties) {
            Object regionMapObj = properties.get("geographic.region.mapping");
            if (regionMapObj instanceof Map) {
                this.regionToPartitionMap = (Map<String, Integer>) regionMapObj;
            }
            
            Object defaultPartObj = properties.get("geographic.default.partition");
            if (defaultPartObj instanceof Number) {
                this.defaultPartition = ((Number) defaultPartObj).intValue();
            }
        }
        
        private void initializeDefaultRegionMapping(int numPartitions) {
            regionToPartitionMap = Map.of(
                "us-east", 0 % numPartitions,
                "us-west", 1 % numPartitions,
                "eu", 2 % numPartitions,
                "asia", 3 % numPartitions
            );
            logger.info("Initialized default geographic region mapping for {} partitions", numPartitions);
        }
        
        private String extractRegionFromRecord(ProducerRecord<K, V> record) {
            // Try to extract region from headers
            if (record.headers() != null) {
                var regionHeader = record.headers().lastHeader("region");
                if (regionHeader != null) {
                    return new String(regionHeader.value(), StandardCharsets.UTF_8);
                }
            }
            
            // Try to extract from key if it's a string
            if (record.key() instanceof String) {
                String keyStr = (String) record.key();
                if (keyStr.contains("-")) {
                    return keyStr.split("-")[0]; // e.g., "us-east-user123" -> "us-east"
                }
            }
            
            return null;
        }
    }
    
    /**
     * Time-based partitioning strategy.
     */
    public static class TimeBasedPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private long timeWindowMillis = 3600000; // 1 hour default
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            long timestamp = record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
            long timeWindow = timestamp / timeWindowMillis;
            return (int) (timeWindow % numPartitions);
        }
        
        @Override
        public String getStrategyName() {
            return "time-based";
        }
        
        @Override
        public void configure(Map<String, Object> properties) {
            Object windowObj = properties.get("time.window.millis");
            if (windowObj instanceof Number) {
                this.timeWindowMillis = ((Number) windowObj).longValue();
                logger.info("Time-based partitioner configured with window: {} ms", timeWindowMillis);
            }
        }
    }
    
    /**
     * Load-balanced partitioning strategy (simulation).
     */
    public static class LoadBalancedPartitioningStrategy<K, V> implements ProducerPartitioningStrategy<K, V> {
        private final AtomicInteger[] partitionCounters;
        private final int numPartitions;
        
        public LoadBalancedPartitioningStrategy(int numPartitions) {
            this.numPartitions = numPartitions;
            this.partitionCounters = new AtomicInteger[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                partitionCounters[i] = new AtomicInteger(0);
            }
        }
        
        @Override
        public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
            // Find partition with lowest load
            int minLoad = Integer.MAX_VALUE;
            int selectedPartition = 0;
            
            for (int i = 0; i < this.numPartitions; i++) {
                int load = partitionCounters[i].get();
                if (load < minLoad) {
                    minLoad = load;
                    selectedPartition = i;
                }
            }
            
            // Increment counter for selected partition
            partitionCounters[selectedPartition].incrementAndGet();
            
            return selectedPartition;
        }
        
        @Override
        public String getStrategyName() {
            return "load-balanced";
        }
    }
}
