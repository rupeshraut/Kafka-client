package com.kafka.multidc.partitioning;

import com.kafka.multidc.partitioning.impl.BuiltInPartitioningStrategies;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for producer partitioning strategies.
 * Handles strategy selection, configuration, and execution.
 */
public class ProducerPartitioningManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerPartitioningManager.class);
    
    private final Map<String, ProducerPartitioningStrategy<?, ?>> strategies = new ConcurrentHashMap<>();
    private ProducerPartitioningType defaultStrategy = ProducerPartitioningType.DEFAULT;
    private ProducerPartitioningStrategy<?, ?> activeStrategy;
    
    public ProducerPartitioningManager() {
        initializeBuiltInStrategies();
    }
    
    /**
     * Initialize built-in partitioning strategies.
     */
    private void initializeBuiltInStrategies() {
        strategies.put("round-robin", new BuiltInPartitioningStrategies.RoundRobinPartitioningStrategy<>());
        strategies.put("key-hash", new BuiltInPartitioningStrategies.KeyHashPartitioningStrategy<>());
        strategies.put("random", new BuiltInPartitioningStrategies.RandomPartitioningStrategy<>());
        strategies.put("sticky", new BuiltInPartitioningStrategies.StickyPartitioningStrategy<>());
        strategies.put("geographic", new BuiltInPartitioningStrategies.GeographicPartitioningStrategy<>());
        strategies.put("time-based", new BuiltInPartitioningStrategies.TimeBasedPartitioningStrategy<>());
        
        logger.info("Initialized {} built-in partitioning strategies", strategies.size());
    }
    
    /**
     * Set the partitioning strategy by type.
     *
     * @param strategyType The strategy type
     */
    public void setPartitioningStrategy(ProducerPartitioningType strategyType) {
        setPartitioningStrategy(strategyType, null);
    }
    
    /**
     * Set the partitioning strategy with configuration.
     *
     * @param strategyType The strategy type
     * @param config Configuration properties
     */
    public void setPartitioningStrategy(ProducerPartitioningType strategyType, Map<String, Object> config) {
        this.defaultStrategy = strategyType;
        
        switch (strategyType) {
            case ROUND_ROBIN:
                activeStrategy = strategies.get("round-robin");
                break;
            case KEY_HASH:
                activeStrategy = strategies.get("key-hash");
                break;
            case RANDOM:
                activeStrategy = strategies.get("random");
                break;
            case STICKY:
                activeStrategy = strategies.get("sticky");
                break;
            case GEOGRAPHIC:
                activeStrategy = strategies.get("geographic");
                break;
            case TIME_BASED:
                activeStrategy = strategies.get("time-based");
                break;
            case LOAD_BALANCED:
                // Load-balanced strategy needs numPartitions, will be created per topic
                activeStrategy = null;
                break;
            case DEFAULT:
            case CUSTOM:
            default:
                activeStrategy = null;
                break;
        }
        
        if (activeStrategy != null && config != null) {
            activeStrategy.configure(config);
        }
        
        logger.info("Set partitioning strategy to: {}", strategyType);
    }
    
    /**
     * Register a custom partitioning strategy.
     *
     * @param name Strategy name
     * @param strategy Strategy implementation
     */
    public void registerCustomStrategy(String name, ProducerPartitioningStrategy<?, ?> strategy) {
        strategies.put(name, strategy);
        logger.info("Registered custom partitioning strategy: {}", name);
    }
    
    /**
     * Set custom partitioning strategy by name.
     *
     * @param strategyName Strategy name
     */
    public void setCustomPartitioningStrategy(String strategyName) {
        ProducerPartitioningStrategy<?, ?> strategy = strategies.get(strategyName);
        if (strategy != null) {
            this.activeStrategy = strategy;
            this.defaultStrategy = ProducerPartitioningType.CUSTOM;
            logger.info("Set custom partitioning strategy: {}", strategyName);
        } else {
            throw new IllegalArgumentException("Unknown partitioning strategy: " + strategyName);
        }
    }
    
    /**
     * Determine partition for a record.
     *
     * @param record The producer record
     * @param numPartitions Number of partitions for the topic
     * @param <K> Key type
     * @param <V> Value type
     * @return Partition number or null for default Kafka partitioning
     */
    @SuppressWarnings("unchecked")
    public <K, V> Integer determinePartition(ProducerRecord<K, V> record, int numPartitions) {
        if (activeStrategy == null) {
            if (defaultStrategy == ProducerPartitioningType.LOAD_BALANCED) {
                // Create load-balanced strategy on demand
                var loadBalancedStrategy = new BuiltInPartitioningStrategies.LoadBalancedPartitioningStrategy<K, V>(numPartitions);
                return loadBalancedStrategy.partition(record, numPartitions);
            }
            return null; // Use default Kafka partitioning
        }
        
        try {
            ProducerPartitioningStrategy<K, V> typedStrategy = (ProducerPartitioningStrategy<K, V>) activeStrategy;
            Integer partition = typedStrategy.partition(record, numPartitions);
            
            if (partition != null && (partition < 0 || partition >= numPartitions)) {
                logger.warn("Invalid partition {} returned by strategy {}, using default", 
                           partition, typedStrategy.getStrategyName());
                return null;
            }
            
            return partition;
        } catch (Exception e) {
            logger.error("Error in partitioning strategy {}, falling back to default", 
                        activeStrategy.getStrategyName(), e);
            return null;
        }
    }
    
    /**
     * Get current partitioning strategy type.
     *
     * @return Current strategy type
     */
    public ProducerPartitioningType getCurrentStrategy() {
        return defaultStrategy;
    }
    
    /**
     * Get current active strategy name.
     *
     * @return Strategy name or null if using default
     */
    public String getCurrentStrategyName() {
        return activeStrategy != null ? activeStrategy.getStrategyName() : "default";
    }
    
    /**
     * Get partitioning statistics.
     *
     * @return Statistics map
     */
    public Map<String, Object> getPartitioningStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("currentStrategy", getCurrentStrategyName());
        stats.put("availableStrategies", strategies.keySet());
        stats.put("strategyType", defaultStrategy.toString());
        return stats;
    }
}
