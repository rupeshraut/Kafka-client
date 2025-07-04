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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Reactive producer operations implementation.
 */
public class ReactiveProducerOperationsImpl implements KafkaProducerOperations.Reactive {
    
    private static final Logger logger = LoggerFactory.getLogger(ReactiveProducerOperationsImpl.class);
    
    private final KafkaConnectionPoolManager poolManager;
    private final DatacenterRouter router;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    public ReactiveProducerOperationsImpl(KafkaConnectionPoolManager poolManager, 
                                         DatacenterRouter router,
                                         CircuitBreaker circuitBreaker,
                                         Retry retry) {
        this.poolManager = poolManager;
        this.router = router;
        this.circuitBreaker = circuitBreaker;
        this.retry = retry;
    }
    
    @Override
    public <K, V> Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        String datacenterId = router.selectDatacenter();
        return send(datacenterId, record);
    }
    
    @Override
    public <K, V> Mono<RecordMetadata> send(String datacenterId, ProducerRecord<K, V> record) {
        return Mono.fromCallable(() -> 
            circuitBreaker.executeSupplier(() -> 
                retry.executeSupplier(() -> {
                    logger.debug("Sending record reactively to datacenter: {}, topic: {}", datacenterId, record.topic());
                    
                    KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                    try {
                        return producer.send(record).get();
                    } catch (Exception e) {
                        logger.error("Failed to send record reactively to datacenter: {}", datacenterId, e);
                        throw new RuntimeException("Failed to send record reactively", e);
                    }
                })
            )
        ).onErrorResume(throwable -> {
            logger.error("Error in reactive send", throwable);
            return Mono.error(throwable);
        });
    }
    
    @Override
    public <K, V> Flux<RecordMetadata> sendMany(Flux<ProducerRecord<K, V>> records) {
        return records
            .flatMap(record -> send(record))
            .onErrorContinue((throwable, record) -> {
                logger.error("Failed to send record in sendMany: {}", record, throwable);
            });
    }
    
    @Override
    public <K, V> Flux<List<RecordMetadata>> sendBatched(Flux<ProducerRecord<K, V>> records, int batchSize) {
        return records
            .buffer(batchSize)
            .flatMap(batch -> {
                String datacenterId = router.selectDatacenter();
                
                return Mono.fromCallable(() -> 
                    circuitBreaker.executeSupplier(() -> 
                        retry.executeSupplier(() -> {
                            logger.debug("Sending batched records reactively to datacenter: {}, batch size: {}", datacenterId, batch.size());
                            
                            KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                            
                            try {
                                return batch.stream()
                                    .map(record -> {
                                        try {
                                            return producer.send(record).get();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                                    .toList();
                            } catch (Exception e) {
                                logger.error("Failed to send batched records reactively to datacenter: {}", datacenterId, e);
                                throw new RuntimeException("Failed to send batched records reactively", e);
                            }
                        })
                    )
                );
            })
            .onErrorContinue((throwable, batch) -> {
                logger.error("Failed to send batch in sendBatched: {}", batch, throwable);
            });
    }
    
    @Override
    public <K, V> Mono<List<RecordMetadata>> sendTransactional(Flux<ProducerRecord<K, V>> records) {
        String datacenterId = router.selectDatacenter();
        
        return records
            .collectList()
            .flatMap(recordList -> 
                Mono.fromCallable(() -> 
                    circuitBreaker.executeSupplier(() -> 
                        retry.executeSupplier(() -> {
                            logger.debug("Sending transactional records reactively to datacenter: {}, count: {}", datacenterId, recordList.size());
                            
                            KafkaProducer<K, V> producer = poolManager.getProducer(datacenterId);
                            
                            try {
                                producer.beginTransaction();
                                
                                List<RecordMetadata> results = recordList.stream()
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
                                logger.error("Failed to send transactional records reactively to datacenter: {}", datacenterId, e);
                                try {
                                    producer.abortTransaction();
                                } catch (Exception abortException) {
                                    logger.error("Failed to abort transaction", abortException);
                                }
                                throw new RuntimeException("Failed to send transactional records reactively", e);
                            }
                        })
                    )
                )
            )
            .onErrorResume(throwable -> {
                logger.error("Error in reactive transactional send", throwable);
                return Mono.error(throwable);
            });
    }
}
