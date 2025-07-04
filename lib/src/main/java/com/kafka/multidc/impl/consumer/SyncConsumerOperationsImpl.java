package com.kafka.multidc.impl.consumer;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.operations.consumer.KafkaConsumerOperations;
import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import com.kafka.multidc.routing.DatacenterRouter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Synchronous implementation of Kafka consumer operations.
 * Provides blocking consumer operations with automatic datacenter routing.
 */
public class SyncConsumerOperationsImpl implements KafkaConsumerOperations.Sync {
    
    private static final Logger logger = LoggerFactory.getLogger(SyncConsumerOperationsImpl.class);
    
    private final KafkaConnectionPoolManager poolManager;
    private final DatacenterRouter router;
    private final KafkaDatacenterConfiguration configuration;
    private final MeterRegistry meterRegistry;
    private final Map<String, Timer> consumeTimers = new ConcurrentHashMap<>();
    
    public SyncConsumerOperationsImpl(
            KafkaConnectionPoolManager poolManager,
            DatacenterRouter router,
            KafkaDatacenterConfiguration configuration,
            MeterRegistry meterRegistry) {
        this.poolManager = poolManager;
        this.router = router;
        this.configuration = configuration;
        this.meterRegistry = meterRegistry;
    }
    
    @Override
    public void subscribe(Collection<String> topics) {
        String datacenterId = router.selectDatacenter();
        subscribe(datacenterId, topics);
    }
    
    @Override
    public void subscribe(String datacenterId, Collection<String> topics) {
        logger.info("Subscribing to topics: {} in datacenter: {}", topics, datacenterId);
        
        try {
            KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
            consumer.subscribe(topics);
            logger.debug("Successfully subscribed to topics: {} in datacenter: {}", topics, datacenterId);
        } catch (Exception e) {
            logger.error("Failed to subscribe to topics: {} in datacenter: {}", topics, datacenterId, e);
            throw new RuntimeException("Failed to subscribe to topics", e);
        }
    }
    
    @Override
    public <K, V> ConsumerRecords<K, V> poll(Duration timeout) {
        String datacenterId = router.selectDatacenter();
        return poll(datacenterId, timeout);
    }
    
    @Override
    public <K, V> ConsumerRecords<K, V> poll(String datacenterId, Duration timeout) {
        Timer timer = getConsumeTimer("poll", datacenterId);
        
        try {
            return timer.recordCallable(() -> {
                try {
                    KafkaConsumer<K, V> consumer = poolManager.getConsumer(datacenterId, "default-group");
                    logger.debug("Polling messages from datacenter: {} with timeout: {}", datacenterId, timeout);
                    
                    ConsumerRecords<K, V> records = consumer.poll(timeout);
                    logger.debug("Polled {} records from datacenter: {}", records.count(), datacenterId);
                    
                    return records;
                } catch (Exception e) {
                    logger.error("Failed to poll from datacenter: {}", datacenterId, e);
                    throw new RuntimeException("Failed to poll from datacenter: " + datacenterId, e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to record poll timing", e);
        }
    }
    
    @Override
    public void commitSync() {
        String datacenterId = router.selectDatacenter();
        Timer timer = getConsumeTimer("commit", datacenterId);
        
        timer.record(() -> {
            try {
                KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
                consumer.commitSync();
                logger.debug("Successfully committed offsets synchronously in datacenter: {}", datacenterId);
            } catch (Exception e) {
                logger.error("Failed to commit offsets synchronously in datacenter: {}", datacenterId, e);
                throw new RuntimeException("Failed to commit offsets", e);
            }
        });
    }
    
    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        String datacenterId = router.selectDatacenter();
        Timer timer = getConsumeTimer("commit", datacenterId);
        
        timer.record(() -> {
            try {
                KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
                consumer.commitSync(offsets);
                logger.debug("Successfully committed specific offsets synchronously in datacenter: {}", datacenterId);
            } catch (Exception e) {
                logger.error("Failed to commit specific offsets synchronously in datacenter: {}", datacenterId, e);
                throw new RuntimeException("Failed to commit offsets", e);
            }
        });
    }
    
    @Override
    public void seek(TopicPartition partition, long offset) {
        String datacenterId = router.selectDatacenter();
        
        try {
            KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
            consumer.seek(partition, offset);
            logger.debug("Successfully sought to offset {} for partition: {} in datacenter: {}", 
                        offset, partition, datacenterId);
        } catch (Exception e) {
            logger.error("Failed to seek to offset {} for partition: {} in datacenter: {}", 
                        offset, partition, datacenterId, e);
            throw new RuntimeException("Failed to seek to offset", e);
        }
    }
    
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        String datacenterId = router.selectDatacenter();
        
        try {
            KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
            consumer.seekToBeginning(partitions);
            logger.debug("Successfully sought to beginning for partitions: {} in datacenter: {}", partitions, datacenterId);
        } catch (Exception e) {
            logger.error("Failed to seek to beginning for partitions: {} in datacenter: {}", partitions, datacenterId, e);
            throw new RuntimeException("Failed to seek to beginning", e);
        }
    }
    
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        String datacenterId = router.selectDatacenter();
        
        try {
            KafkaConsumer<?, ?> consumer = poolManager.getConsumer(datacenterId, "default-group");
            consumer.seekToEnd(partitions);
            logger.debug("Successfully sought to end for partitions: {} in datacenter: {}", partitions, datacenterId);
        } catch (Exception e) {
            logger.error("Failed to seek to end for partitions: {} in datacenter: {}", partitions, datacenterId, e);
            throw new RuntimeException("Failed to seek to end", e);
        }
    }
    
    @Override
    public void close() {
        close(Duration.ofSeconds(30));
    }
    
    @Override
    public void close(Duration timeout) {
        logger.info("Closing sync consumer with timeout: {}", timeout);
        // Implementation for closing consumers would go here
        // For now, we'll just log the close operation
        logger.debug("Sync consumer closed");
    }
    
    private Timer getConsumeTimer(String operation, String datacenterId) {
        String key = operation + "-" + datacenterId;
        return consumeTimers.computeIfAbsent(key, k -> 
            Timer.builder("kafka.consumer.operation.duration")
                .tag("operation", operation)
                .tag("datacenter", datacenterId)
                .register(meterRegistry));
    }
}
