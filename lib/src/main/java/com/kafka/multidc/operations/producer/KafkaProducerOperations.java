package com.kafka.multidc.operations.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface KafkaProducerOperations {
    
    interface Sync {
        /**
         * Send a record synchronously to the specified datacenter.
         */
        <K, V> RecordMetadata send(String datacenterId, ProducerRecord<K, V> record);
        
        /**
         * Send a record synchronously with automatic datacenter selection.
         */
        <K, V> RecordMetadata send(ProducerRecord<K, V> record);
        
        /**
         * Send a record synchronously with timeout.
         */
        <K, V> RecordMetadata send(ProducerRecord<K, V> record, Duration timeout);
        
        /**
         * Send multiple records in a batch synchronously.
         */
        <K, V> List<RecordMetadata> sendBatch(List<ProducerRecord<K, V>> records);
        
        /**
         * Send a transactional batch of records.
         */
        <K, V> List<RecordMetadata> sendTransactional(List<ProducerRecord<K, V>> records);
        
        /**
         * Flush all pending records synchronously.
         */
        void flush();
    }
    
    interface Async {
        /**
         * Send a record asynchronously to the specified datacenter.
         */
        <K, V> CompletableFuture<RecordMetadata> sendAsync(String datacenterId, ProducerRecord<K, V> record);
        
        /**
         * Send a record asynchronously with automatic datacenter selection.
         */
        <K, V> CompletableFuture<RecordMetadata> sendAsync(ProducerRecord<K, V> record);
        
        /**
         * Send multiple records asynchronously.
         */
        <K, V> CompletableFuture<List<RecordMetadata>> sendBatchAsync(List<ProducerRecord<K, V>> records);
        
        /**
         * Send a transactional batch asynchronously.
         */
        <K, V> CompletableFuture<List<RecordMetadata>> sendTransactionalAsync(List<ProducerRecord<K, V>> records);
        
        /**
         * Flush all pending records asynchronously.
         */
        CompletableFuture<Void> flushAsync();
    }
    
    interface Reactive {
        /**
         * Send a record reactively with automatic datacenter selection.
         */
        <K, V> Mono<RecordMetadata> send(ProducerRecord<K, V> record);
        
        /**
         * Send a record reactively to a specific datacenter.
         */
        <K, V> Mono<RecordMetadata> send(String datacenterId, ProducerRecord<K, V> record);
        
        /**
         * Send a stream of records reactively with backpressure.
         */
        <K, V> Flux<RecordMetadata> sendMany(Flux<ProducerRecord<K, V>> records);
        
        /**
         * Send records in batches reactively.
         */
        <K, V> Flux<List<RecordMetadata>> sendBatched(Flux<ProducerRecord<K, V>> records, int batchSize);
        
        /**
         * Send transactional records reactively.
         */
        <K, V> Mono<List<RecordMetadata>> sendTransactional(Flux<ProducerRecord<K, V>> records);
    }
}
