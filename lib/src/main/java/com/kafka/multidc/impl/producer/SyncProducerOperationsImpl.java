package com.kafka.multidc.impl.producer;

import com.kafka.multidc.operations.producer.KafkaProducerOperations;
import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import com.kafka.multidc.routing.DatacenterRouter;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Synchronous producer operations implementation.
 */
public class SyncProducerOperationsImpl implements KafkaProducerOperations.Sync {
    
    private static final Logger logger = LoggerFactory.getLogger(SyncProducerOperationsImpl.class);
    
    private final KafkaConnectionPoolManager poolManager;
    private final DatacenterRouter router;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    public SyncProducerOperationsImpl(KafkaConnectionPoolManager poolManager, 
                                     DatacenterRouter router,
                                     CircuitBreaker circuitBreaker,
                                     Retry retry) {
        this.poolManager = poolManager;
        this.router = router;
        this.circuitBreaker = circuitBreaker;
        this.retry = retry;
    }
    
    @Override
    public <K, V> RecordMetadata send(String datacenterId, ProducerRecord<K, V> record) {
        return circuitBreaker.executeSupplier(() -> 
            retry.executeSupplier(() -> {
                logger.debug("Sending record to datacenter: {}, topic: {}", datacenterId, record.topic());
                
                KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                try {
                    return producer.send(record).get();
                } catch (Exception e) {
                    logger.error("Failed to send record to datacenter: {}", datacenterId, e);
                    throw new RuntimeException("Failed to send record", e);
                }
            })
        );
    }
    
    @Override
    public <K, V> RecordMetadata send(ProducerRecord<K, V> record) {
        String datacenterId = router.selectDatacenter();
        return send(datacenterId, record);
    }
    
    @Override
    public <K, V> RecordMetadata send(ProducerRecord<K, V> record, Duration timeout) {
        String datacenterId = router.selectDatacenter();
        
        return circuitBreaker.executeSupplier(() -> 
            retry.executeSupplier(() -> {
                logger.debug("Sending record with timeout to datacenter: {}, topic: {}", datacenterId, record.topic());
                
                KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                try {
                    return producer.send(record).get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.error("Failed to send record with timeout to datacenter: {}", datacenterId, e);
                    throw new RuntimeException("Failed to send record with timeout", e);
                }
            })
        );
    }
    
    @Override
    public <K, V> List<RecordMetadata> sendBatch(List<ProducerRecord<K, V>> records) {
        String datacenterId = router.selectDatacenter();
        
        return circuitBreaker.executeSupplier(() -> 
            retry.executeSupplier(() -> {
                logger.debug("Sending batch of {} records to datacenter: {}", records.size(), datacenterId);
                
                KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                List<RecordMetadata> results = new ArrayList<>();
                
                try {
                    for (ProducerRecord<K, V> record : records) {
                        RecordMetadata metadata = producer.send(record).get();
                        results.add(metadata);
                    }
                    return results;
                } catch (Exception e) {
                    logger.error("Failed to send batch to datacenter: {}", datacenterId, e);
                    throw new RuntimeException("Failed to send batch", e);
                }
            })
        );
    }
    
    @Override
    public <K, V> List<RecordMetadata> sendTransactional(List<ProducerRecord<K, V>> records) {
        String datacenterId = router.selectDatacenter();
        
        return circuitBreaker.executeSupplier(() -> 
            retry.executeSupplier(() -> {
                logger.debug("Sending transactional batch of {} records to datacenter: {}", records.size(), datacenterId);
                
                KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                List<RecordMetadata> results = new ArrayList<>();
                
                try {
                    producer.beginTransaction();
                    
                    for (ProducerRecord<K, V> record : records) {
                        RecordMetadata metadata = producer.send(record).get();
                        results.add(metadata);
                    }
                    
                    producer.commitTransaction();
                    return results;
                } catch (Exception e) {
                    logger.error("Failed to send transactional batch to datacenter: {}", datacenterId, e);
                    try {
                        producer.abortTransaction();
                    } catch (Exception abortException) {
                        logger.error("Failed to abort transaction", abortException);
                    }
                    throw new RuntimeException("Failed to send transactional batch", e);
                }
            })
        );
    }
    
    @Override
    public void flush() {
        logger.debug("Flushing all producers");
        
        // Flush all producers in all datacenters
        for (String datacenterId : router.getAllDatacenters().stream().map(dc -> dc.getId()).toList()) {
            try {
                KafkaProducer<?, ?> producer = poolManager.getProducer(datacenterId);
                producer.flush();
            } catch (Exception e) {
                logger.warn("Failed to flush producer for datacenter: {}", datacenterId, e);
            }
        }
    }
}
