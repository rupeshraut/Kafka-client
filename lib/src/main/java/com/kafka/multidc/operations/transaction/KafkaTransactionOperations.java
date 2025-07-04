package com.kafka.multidc.operations.transaction;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Transaction operations for multi-datacenter Kafka producers.
 * Provides exactly-once semantics with ACID transaction support.
 */
public interface KafkaTransactionOperations {
    
    /**
     * Synchronous transaction operations.
     */
    interface Sync {
        
        /**
         * Begin a new transaction.
         */
        void beginTransaction();
        
        /**
         * Send a record as part of the current transaction.
         */
        <K, V> void sendTransactional(ProducerRecord<K, V> record);
        
        /**
         * Send records to offsets as part of the current transaction.
         */
        <K, V> void sendOffsetsToTransaction(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, String consumerGroupId);
        
        /**
         * Commit the current transaction.
         */
        void commitTransaction();
        
        /**
         * Abort the current transaction.
         */
        void abortTransaction();
        
        /**
         * Execute a function within a transaction with automatic commit/abort.
         */
        <T> T executeInTransaction(TransactionCallback<T> callback);
    }
    
    /**
     * Asynchronous transaction operations.
     */
    interface Async {
        
        /**
         * Begin a new transaction asynchronously.
         */
        CompletableFuture<Void> beginTransactionAsync();
        
        /**
         * Send a record as part of the current transaction asynchronously.
         */
        <K, V> CompletableFuture<Void> sendTransactionalAsync(ProducerRecord<K, V> record);
        
        /**
         * Send multiple records transactionally.
         */
        <K, V> CompletableFuture<Void> sendAllTransactionalAsync(List<ProducerRecord<K, V>> records);
        
        /**
         * Send records to offsets as part of the current transaction asynchronously.
         */
        CompletableFuture<Void> sendOffsetsToTransactionAsync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, String consumerGroupId);
        
        /**
         * Commit the current transaction asynchronously.
         */
        CompletableFuture<Void> commitTransactionAsync();
        
        /**
         * Abort the current transaction asynchronously.
         */
        CompletableFuture<Void> abortTransactionAsync();
        
        /**
         * Execute a function within a transaction with automatic commit/abort.
         */
        <T> CompletableFuture<T> executeInTransactionAsync(AsyncTransactionCallback<T> callback);
    }
    
    /**
     * Reactive transaction operations.
     */
    interface Reactive {
        
        /**
         * Execute operations within a transactional context reactively.
         */
        <T> Mono<T> executeInTransaction(ReactiveTransactionCallback<T> callback);
        
        /**
         * Begin a transaction reactively.
         */
        Mono<Void> beginTransaction();
        
        /**
         * Send a record transactionally.
         */
        <K, V> Mono<Void> sendTransactional(ProducerRecord<K, V> record);
        
        /**
         * Commit transaction reactively.
         */
        Mono<Void> commitTransaction();
        
        /**
         * Abort transaction reactively.
         */
        Mono<Void> abortTransaction();
    }
    
    /**
     * Transaction status information.
     */
    interface TransactionStatus {
        boolean isTransactionActive();
        String getTransactionId();
        Duration getTransactionTimeout();
        long getTransactionStartTime();
    }
    
    /**
     * Get current transaction status.
     */
    TransactionStatus getTransactionStatus();
    
    /**
     * Callback interface for synchronous transactional operations.
     * 
     * <p>Implementations of this interface define the business logic to be executed
     * within a Kafka transaction. The transaction will be automatically committed
     * if the callback completes successfully, or aborted if an exception is thrown.
     * 
     * @param <T> the return type of the transactional operation
     */
    @FunctionalInterface
    interface TransactionCallback<T> {
        /**
         * Executes the transactional operation.
         * 
         * @return the result of the operation
         * @throws Exception if the operation fails (will cause transaction abort)
         */
        T execute() throws Exception;
    }
    
    /**
     * Callback interface for asynchronous transactional operations.
     * 
     * <p>Implementations of this interface define asynchronous business logic 
     * to be executed within a Kafka transaction. The transaction will be 
     * automatically committed when the returned CompletableFuture completes 
     * successfully, or aborted if it completes exceptionally.
     * 
     * @param <T> the return type of the transactional operation
     */
    @FunctionalInterface
    interface AsyncTransactionCallback<T> {
        /**
         * Executes the asynchronous transactional operation.
         * 
         * @return a CompletableFuture representing the result of the operation
         */
        CompletableFuture<T> execute();
    }
    
    /**
     * Callback interface for reactive transactional operations.
     * 
     * <p>Implementations of this interface define reactive business logic 
     * to be executed within a Kafka transaction using Project Reactor. 
     * The transaction will be automatically committed when the returned 
     * Mono completes successfully, or aborted if it signals an error.
     * 
     * @param <T> the return type of the transactional operation
     */
    @FunctionalInterface
    interface ReactiveTransactionCallback<T> {
        /**
         * Executes the reactive transactional operation.
         * 
         * @return a Mono representing the result of the operation
         */
        Mono<T> execute();
    }
}
