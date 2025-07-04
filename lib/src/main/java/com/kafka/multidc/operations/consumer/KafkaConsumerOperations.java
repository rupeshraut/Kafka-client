package com.kafka.multidc.operations.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface KafkaConsumerOperations {
    
    interface Sync {
        /**
         * Subscribe to topics with automatic datacenter selection.
         */
        void subscribe(Collection<String> topics);
        
        /**
         * Subscribe to topics in a specific datacenter.
         */
        void subscribe(String datacenterId, Collection<String> topics);
        
        /**
         * Poll for records synchronously.
         */
        <K, V> ConsumerRecords<K, V> poll(Duration timeout);
        
        /**
         * Poll for records from a specific datacenter.
         */
        <K, V> ConsumerRecords<K, V> poll(String datacenterId, Duration timeout);
        
        /**
         * Commit offsets synchronously.
         */
        void commitSync();
        
        /**
         * Commit specific offsets synchronously.
         */
        void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
        
        /**
         * Seek to a specific offset.
         */
        void seek(TopicPartition partition, long offset);
        
        /**
         * Seek to the beginning of partitions.
         */
        void seekToBeginning(Collection<TopicPartition> partitions);
        
        /**
         * Seek to the end of partitions.
         */
        void seekToEnd(Collection<TopicPartition> partitions);
        
        /**
         * Close the consumer.
         */
        void close();
        
        /**
         * Close the consumer with timeout.
         */
        void close(Duration timeout);
    }
    
    interface Async {
        /**
         * Subscribe to topics asynchronously.
         */
        CompletableFuture<Void> subscribeAsync(Collection<String> topics);
        
        /**
         * Subscribe to topics in a specific datacenter asynchronously.
         */
        CompletableFuture<Void> subscribeAsync(String datacenterId, Collection<String> topics);
        
        /**
         * Poll for records asynchronously.
         */
        <K, V> CompletableFuture<ConsumerRecords<K, V>> pollAsync(Duration timeout);
        
        /**
         * Process records with a callback asynchronously.
         */
        <K, V> CompletableFuture<Void> processRecords(Consumer<ConsumerRecord<K, V>> processor);
        
        /**
         * Commit offsets asynchronously.
         */
        CompletableFuture<Void> commitAsync();
        
        /**
         * Commit specific offsets asynchronously.
         */
        CompletableFuture<Void> commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets);
        
        /**
         * Close the consumer asynchronously.
         */
        CompletableFuture<Void> closeAsync();
    }
    
    interface Reactive {
        /**
         * Subscribe to topics and return a reactive stream of records.
         */
        <K, V> Flux<ConsumerRecord<K, V>> subscribe(String... topics);
        
        /**
         * Subscribe to topics in a specific datacenter reactively.
         */
        <K, V> Flux<ConsumerRecord<K, V>> subscribeToDatacenter(String datacenterId, String... topics);
        
        /**
         * Subscribe with custom consumer group.
         */
        <K, V> Flux<ConsumerRecord<K, V>> subscribeWithGroup(String groupId, String... topics);
        
        /**
         * Process records with automatic offset commits.
         */
        <K, V> Flux<ConsumerRecord<K, V>> subscribeWithAutoCommit(String... topics);
        
        /**
         * Process records in batches.
         */
        <K, V> Flux<List<ConsumerRecord<K, V>>> subscribeBatched(int batchSize, String... topics);
        
        /**
         * Subscribe with manual acknowledgment.
         */
        <K, V> Flux<ConsumerRecord<K, V>> subscribeManualAck(String... topics);
        
        /**
         * Close the reactive consumer.
         */
        Mono<Void> close();
    }
}
