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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous producer operations implementation.
 */
public class AsyncProducerOperationsImpl implements KafkaProducerOperations.Async {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducerOperationsImpl.class);
    
    private final KafkaConnectionPoolManager poolManager;
    private final DatacenterRouter router;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    public AsyncProducerOperationsImpl(KafkaConnectionPoolManager poolManager, 
                                      DatacenterRouter router,
                                      CircuitBreaker circuitBreaker,
                                      Retry retry) {
        this.poolManager = poolManager;
        this.router = router;
        this.circuitBreaker = circuitBreaker;
        this.retry = retry;
    }
    
    @Override
    public <K, V> CompletableFuture<RecordMetadata> sendAsync(String datacenterId, ProducerRecord<K, V> record) {
        return CompletableFuture.supplyAsync(() -> 
            circuitBreaker.executeSupplier(() -> 
                retry.executeSupplier(() -> {
                    logger.debug("Sending record async to datacenter: {}, topic: {}", datacenterId, record.topic());
                    
                    KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                    try {
                        return producer.send(record).get();
                    } catch (Exception e) {
                        logger.error("Failed to send record async to datacenter: {}", datacenterId, e);
                        throw new RuntimeException("Failed to send record async", e);
                    }
                })
            )
        );
    }
    
    @Override
    public <K, V> CompletableFuture<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {
        String datacenterId = router.selectDatacenter();
        return sendAsync(datacenterId, record);
    }
    
    @Override
    public <K, V> CompletableFuture<List<RecordMetadata>> sendBatchAsync(List<ProducerRecord<K, V>> records) {
        String datacenterId = router.selectDatacenter();
        
        return CompletableFuture.supplyAsync(() -> 
            circuitBreaker.executeSupplier(() -> 
                retry.executeSupplier(() -> {
                    logger.debug("Sending batch async of {} records to datacenter: {}", records.size(), datacenterId);
                    
                    KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                    
                    try {
                        List<CompletableFuture<RecordMetadata>> futures = records.stream()
                            .map(record -> CompletableFuture.supplyAsync(() -> {
                                try {
                                    return producer.send(record).get();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }))
                            .toList();
                        
                        return futures.stream()
                            .map(CompletableFuture::join)
                            .toList();
                    } catch (Exception e) {
                        logger.error("Failed to send batch async to datacenter: {}", datacenterId, e);
                        throw new RuntimeException("Failed to send batch async", e);
                    }
                })
            )
        );
    }
    
    @Override
    public <K, V> CompletableFuture<List<RecordMetadata>> sendTransactionalAsync(List<ProducerRecord<K, V>> records) {
        String datacenterId = router.selectDatacenter();
        
        return CompletableFuture.supplyAsync(() -> 
            circuitBreaker.executeSupplier(() -> 
                retry.executeSupplier(() -> {
                    logger.debug("Sending transactional batch async of {} records to datacenter: {}", records.size(), datacenterId);
                    
                    KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                    
                    try {
                        producer.beginTransaction();
                        
                        List<RecordMetadata> results = records.stream()
                            .map(record -> {
                                try {
                                    return producer.send(record).get();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .toList();
                        
                        producer.commitTransaction();
                        return results;
                    } catch (Exception e) {
                        logger.error("Failed to send transactional batch async to datacenter: {}", datacenterId, e);
                        try {
                            producer.abortTransaction();
                        } catch (Exception abortException) {
                            logger.error("Failed to abort transaction", abortException);
                        }
                        throw new RuntimeException("Failed to send transactional batch async", e);
                    }
                })
            )
        );
    }
    
    @Override
    public CompletableFuture<Void> flushAsync() {
        return CompletableFuture.runAsync(() -> {
            logger.debug("Flushing all producers async");
            
            // Flush all producers in all datacenters
            List<CompletableFuture<Void>> flushFutures = router.getAllDatacenters().stream()
                .map(dc -> dc.getId())
                .map(datacenterId -> CompletableFuture.runAsync(() -> {
                    try {
                        KafkaProducer<?, ?> producer = poolManager.getProducer(datacenterId);
                        producer.flush();
                    } catch (Exception e) {
                        logger.warn("Failed to flush producer for datacenter: {}", datacenterId, e);
                    }
                }))
                .toList();
            
            CompletableFuture.allOf(flushFutures.toArray(new CompletableFuture[0])).join();
        });
    }
}
