# Kafka Multi-Datacenter Client - Partitioning Strategies

This document describes the comprehensive partitioning strategies implemented in the Kafka Multi-Datacenter Client library, providing both producer and consumer partitioning capabilities for enterprise-grade applications.

## Overview

The partitioning system provides:

- **Producer Partitioning Strategies** - Control how messages are distributed across partitions
- **Consumer Partitioning Strategies** - Manage partition assignment for consumer groups
- **Multi-Datacenter Awareness** - Geographic and locality-based partitioning
- **Performance Optimization** - Load balancing and throughput-focused strategies
- **Enterprise Features** - Custom strategies and advanced configuration

## Producer Partitioning Strategies

### Built-in Strategies

#### 1. Round-Robin Partitioning
- **Type**: `ProducerPartitioningType.ROUND_ROBIN`
- **Description**: Distributes messages evenly across all partitions in sequential order
- **Use Case**: Even load distribution for keyless messages
- **Example**:
```java
// Messages distributed: P0, P1, P2, P0, P1, P2, ...
```

#### 2. Key-Hash Partitioning
- **Type**: `ProducerPartitioningType.KEY_HASH`
- **Description**: Uses consistent hashing of message keys with MD5 fallback
- **Use Case**: Ensures messages with the same key always go to the same partition
- **Features**:
  - MD5 hashing for consistent distribution
  - Automatic fallback to Java hashCode()
  - Null key handling

#### 3. Random Partitioning
- **Type**: `ProducerPartitioningType.RANDOM`
- **Description**: Randomly selects partition for each message
- **Use Case**: Load testing and scenarios requiring unpredictable distribution

#### 4. Sticky Partitioning
- **Type**: `ProducerPartitioningType.STICKY`
- **Description**: Sends messages to the same partition until batch is full
- **Use Case**: Optimizing producer batching and throughput
- **Configuration**:
  - `sticky.batch.size` - Messages per batch (default: 100)

#### 5. Geographic Partitioning
- **Type**: `ProducerPartitioningType.GEOGRAPHIC`
- **Description**: Routes messages based on geographic regions
- **Use Case**: Data locality and regulatory compliance
- **Features**:
  - Region header detection
  - Key-based region extraction
  - Configurable region-to-partition mapping
- **Configuration**:
  - `geographic.region.mapping` - Map of region to partition
  - `geographic.default.partition` - Default partition for unknown regions

#### 6. Time-Based Partitioning
- **Type**: `ProducerPartitioningType.TIME_BASED`
- **Description**: Partitions based on message timestamp windows
- **Use Case**: Time-series data and chronological processing
- **Configuration**:
  - `time.window.millis` - Time window size (default: 1 hour)

#### 7. Load-Balanced Partitioning
- **Type**: `ProducerPartitioningType.LOAD_BALANCED`
- **Description**: Monitors partition load and routes to least loaded partition
- **Use Case**: Preventing partition hotspots in high-throughput scenarios

### Usage Examples

```java
// Round-Robin Strategy
ProducerPartitioningManager manager = new ProducerPartitioningManager();
manager.setPartitioningStrategy(ProducerPartitioningType.ROUND_ROBIN);

// Geographic Strategy with Configuration
Map<String, Object> geoConfig = Map.of(
    "geographic.region.mapping", Map.of(
        "us-east", 0,
        "us-west", 1,
        "eu", 2
    ),
    "geographic.default.partition", 0
);
manager.setPartitioningStrategy(ProducerPartitioningType.GEOGRAPHIC, geoConfig);

// Custom Strategy
manager.registerCustomStrategy("business-logic", new MyCustomStrategy());
manager.setCustomPartitioningStrategy("business-logic");
```

### Message Headers for Geographic Partitioning

```java
ProducerRecord<String, Object> record = new ProducerRecord<>("topic", "key", value);
record.headers().add(new RecordHeader("region", "us-east".getBytes()));
```

## Consumer Partitioning Strategies

### Built-in Consumer Strategies

#### 1. Datacenter-Aware Range Strategy
- **Description**: Range assignment with datacenter locality preference
- **Features**:
  - Groups consumers by datacenter
  - Assigns consecutive partition ranges
  - Prefers local datacenter consumers

#### 2. Load-Balanced Assignment Strategy
- **Description**: Assigns partitions based on load metrics
- **Features**:
  - Monitors partition load
  - Round-robin assignment with load consideration
  - Configurable load mapping

#### 3. Datacenter-Aware Sticky Strategy
- **Description**: Minimizes partition movement with datacenter awareness
- **Features**:
  - Maintains previous assignments when possible
  - Datacenter preference for new assignments
  - Supports incremental rebalancing

#### 4. Priority-Based Assignment Strategy
- **Description**: Assigns partitions based on consumer priorities
- **Features**:
  - Weighted partition distribution
  - Higher priority consumers get more partitions
  - Configurable priority mapping

### Consumer Strategy Configuration

```java
// Load-Balanced Strategy
Map<String, Object> loadConfig = Map.of(
    "partition.load.map", Map.of(
        "topic-0", 100,
        "topic-1", 50,
        "topic-2", 200
    )
);

// Priority-Based Strategy
Map<String, Object> priorityConfig = Map.of(
    "consumer.priorities", Map.of(
        "high-priority-consumer", 3,
        "normal-consumer", 1,
        "backup-consumer", 1
    )
);
```

## Advanced Features

### Multi-Datacenter Partition Affinity

```java
// Producer with datacenter preference
ProducerRecord<String, Object> record = new ProducerRecord<>("topic", "key", value);
record.headers().add(new RecordHeader("preferred-datacenter", "us-east".getBytes()));

// Consumer with datacenter information
Map<String, String> datacenterInfo = Map.of(
    "consumer-1", "us-east",
    "consumer-2", "us-west",
    "consumer-3", "eu"
);
```

### Dynamic Partitioning

```java
// Adaptive partitioning based on load
scheduler.scheduleAtFixedRate(() -> {
    int loadLevel = getCurrentLoadLevel();
    ProducerPartitioningType strategy = loadLevel > 80 ? LOAD_BALANCED : 
                                       loadLevel > 50 ? ROUND_ROBIN : STICKY;
    manager.setPartitioningStrategy(strategy);
}, 0, 30, TimeUnit.SECONDS);
```

### Custom Partitioning Strategy

```java
public class BusinessLogicPartitioningStrategy<K, V> 
    implements ProducerPartitioningStrategy<K, V> {
    
    @Override
    public Integer partition(ProducerRecord<K, V> record, int numPartitions) {
        // Custom business logic
        if (record.value() instanceof CustomerOrder) {
            CustomerOrder order = (CustomerOrder) record.value();
            return switch (order.getCustomerTier()) {
                case PREMIUM -> 0;
                case STANDARD -> 1;
                case BASIC -> 2;
                default -> null;
            };
        }
        return null;
    }
    
    @Override
    public String getStrategyName() {
        return "business-logic";
    }
}
```

## Performance Considerations

### High-Throughput Scenarios

1. **Sticky Partitioning**: Optimizes producer batching
2. **Key-Hash with Limited Key Space**: Enables effective batching
3. **Load-Balanced**: Prevents partition hotspots

### Low-Latency Requirements

1. **Random Partitioning**: Minimal computation overhead
2. **Round-Robin**: Simple and fast
3. **Custom with Caching**: Pre-computed partition assignments

### Multi-Datacenter Optimization

1. **Geographic Partitioning**: Reduces cross-datacenter traffic
2. **Datacenter-Aware Consumer Assignment**: Locality preference
3. **Partition Affinity Headers**: Explicit datacenter routing

## Monitoring and Metrics

### Producer Partitioning Metrics

```java
Map<String, Object> stats = manager.getPartitioningStats();
// Returns:
// - currentStrategy: Strategy name
// - availableStrategies: List of registered strategies
// - strategyType: Current strategy type
```

### Consumer Assignment Metrics

- Partition assignment distribution
- Rebalance frequency and duration
- Datacenter locality compliance
- Load balance effectiveness

## Example Implementation

See the complete example in `PartitioningStrategiesExample.java`:

- Demonstrates all producer partitioning strategies
- Shows consumer assignment patterns
- Includes performance testing scenarios
- Covers multi-datacenter configurations

## Best Practices

### Producer Partitioning

1. **Use Key-Hash** for maintaining order within keys
2. **Use Geographic** for data locality requirements
3. **Use Sticky** for high-throughput scenarios
4. **Use Load-Balanced** to prevent hotspots

### Consumer Partitioning

1. **Datacenter-Aware Sticky** for stable assignments
2. **Load-Balanced** for uneven partition loads
3. **Priority-Based** for differentiated service levels
4. **Range** for simple, predictable assignments

### Configuration

1. Monitor partition distribution regularly
2. Adjust strategies based on load patterns
3. Test rebalancing behavior under failures
4. Configure appropriate timeouts and retries

## Integration with Enterprise Features

The partitioning system integrates seamlessly with:

- **Security**: Partition-based access control
- **Schema Registry**: Schema-aware partitioning
- **Dead Letter Queues**: Partition-preserving error handling
- **Observability**: Detailed partitioning metrics
- **Resilience**: Partition-aware circuit breakers

This comprehensive partitioning system enables fine-grained control over message distribution and consumption patterns, supporting enterprise requirements for performance, locality, and reliability.
