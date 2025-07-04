package com.kafka.multidc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Advanced consumer group management for multi-datacenter Kafka deployments.
 * Provides sophisticated consumer group coordination, rebalancing strategies,
 * and partition assignment controls.
 */
public interface ConsumerGroupManager {
    
    /**
     * Consumer group information and statistics.
     */
    interface GroupInfo {
        String getGroupId();
        String getGroupState();
        Set<String> getMembers();
        Map<String, Set<TopicPartition>> getMemberAssignments();
        String getCoordinator();
        String getProtocolType();
        String getProtocol();
        long getGenerationId();
    }
    
    /**
     * Consumer group member information.
     */
    interface MemberInfo {
        String getMemberId();
        String getClientId();
        String getHost();
        Set<TopicPartition> getAssignment();
        boolean isLeader();
        Map<String, Object> getMetadata();
    }
    
    /**
     * Partition assignment strategy configuration.
     */
    interface AssignmentStrategy {
        enum Type {
            RANGE,
            ROUND_ROBIN,
            STICKY,
            COOPERATIVE_STICKY,
            CUSTOM
        }
        
        Type getType();
        String getClassName();
        Map<String, String> getParameters();
    }
    
    /**
     * Rebalance event information.
     */
    interface RebalanceEvent {
        enum Type {
            PARTITION_REVOKED,
            PARTITION_ASSIGNED,
            PARTITION_LOST
        }
        
        Type getType();
        Collection<TopicPartition> getPartitions();
        long getTimestamp();
        String getConsumerGroupId();
        String getMemberId();
    }
    
    /**
     * Synchronous consumer group operations.
     */
    interface Sync {
        
        /**
         * Get information about the consumer group.
         */
        GroupInfo getGroupInfo(String groupId);
        
        /**
         * List all consumer groups.
         */
        List<GroupInfo> listGroups();
        
        /**
         * Get member information for a specific consumer group.
         */
        List<MemberInfo> getGroupMembers(String groupId);
        
        /**
         * Reset offsets for a consumer group.
         */
        void resetOffsets(String groupId, Map<TopicPartition, Long> offsets);
        
        /**
         * Reset offsets to earliest for specified topics.
         */
        void resetOffsetsToEarliest(String groupId, Set<String> topics);
        
        /**
         * Reset offsets to latest for specified topics.
         */
        void resetOffsetsToLatest(String groupId, Set<String> topics);
        
        /**
         * Delete a consumer group (must be empty).
         */
        void deleteGroup(String groupId);
        
        /**
         * Get lag information for a consumer group.
         */
        Map<TopicPartition, Long> getConsumerLag(String groupId);
        
        /**
         * Subscribe with custom rebalance listener.
         */
        void subscribeWithRebalanceListener(Collection<String> topics, ConsumerRebalanceListener listener);
        
        /**
         * Manually assign partitions (bypasses group coordination).
         */
        void assignPartitions(Collection<TopicPartition> partitions);
    }
    
    /**
     * Asynchronous consumer group operations.
     */
    interface Async {
        
        /**
         * Get information about the consumer group asynchronously.
         */
        CompletableFuture<GroupInfo> getGroupInfoAsync(String groupId);
        
        /**
         * List all consumer groups asynchronously.
         */
        CompletableFuture<List<GroupInfo>> listGroupsAsync();
        
        /**
         * Get member information for a specific consumer group asynchronously.
         */
        CompletableFuture<List<MemberInfo>> getGroupMembersAsync(String groupId);
        
        /**
         * Reset offsets for a consumer group asynchronously.
         */
        CompletableFuture<Void> resetOffsetsAsync(String groupId, Map<TopicPartition, Long> offsets);
        
        /**
         * Delete a consumer group asynchronously.
         */
        CompletableFuture<Void> deleteGroupAsync(String groupId);
        
        /**
         * Get lag information for a consumer group asynchronously.
         */
        CompletableFuture<Map<TopicPartition, Long>> getConsumerLagAsync(String groupId);
        
        /**
         * Monitor rebalance events asynchronously.
         */
        CompletableFuture<Void> monitorRebalanceEvents(RebalanceEventHandler handler);
    }
    
    /**
     * Reactive consumer group operations.
     */
    interface Reactive {
        
        /**
         * Get information about the consumer group reactively.
         */
        Mono<GroupInfo> getGroupInfo(String groupId);
        
        /**
         * List all consumer groups reactively.
         */
        Flux<GroupInfo> listGroups();
        
        /**
         * Get member information for a specific consumer group reactively.
         */
        Flux<MemberInfo> getGroupMembers(String groupId);
        
        /**
         * Reset offsets for a consumer group reactively.
         */
        Mono<Void> resetOffsets(String groupId, Map<TopicPartition, Long> offsets);
        
        /**
         * Delete a consumer group reactively.
         */
        Mono<Void> deleteGroup(String groupId);
        
        /**
         * Get lag information for a consumer group reactively.
         */
        Mono<Map<TopicPartition, Long>> getConsumerLag(String groupId);
        
        /**
         * Monitor rebalance events as a reactive stream.
         */
        Flux<RebalanceEvent> rebalanceEvents();
        
        /**
         * Stream consumer records with automatic lag monitoring.
         */
        <K, V> Flux<ConsumerRecord<K, V>> consumeWithLagMonitoring(String groupId, Duration lagThreshold);
    }
    
    /**
     * Consumer group configuration.
     */
    interface GroupConfig {
        String getGroupId();
        AssignmentStrategy getAssignmentStrategy();
        Duration getSessionTimeout();
        Duration getHeartbeatInterval();
        Duration getMaxPollInterval();
        int getMaxPollRecords();
        boolean isAutoCommitEnabled();
        Duration getAutoCommitInterval();
        Map<String, String> getAdditionalProperties();
        
        /**
         * Builder for GroupConfig.
         */
        interface Builder {
            Builder groupId(String groupId);
            Builder assignmentStrategy(AssignmentStrategy strategy);
            Builder sessionTimeout(Duration timeout);
            Builder heartbeatInterval(Duration interval);
            Builder maxPollInterval(Duration interval);
            Builder maxPollRecords(int records);
            Builder enableAutoCommit(Duration interval);
            Builder disableAutoCommit();
            Builder additionalProperties(Map<String, String> properties);
            GroupConfig build();
        }
    }
    
    /**
     * Create a new consumer group configuration.
     */
    GroupConfig.Builder newGroupConfig();
    
    /**
     * Handler for rebalance events.
     */
    @FunctionalInterface
    interface RebalanceEventHandler {
        void handle(RebalanceEvent event);
    }
}
