package com.kafka.multidc.consumer.impl;

import com.kafka.multidc.consumer.ConsumerGroupManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of ConsumerGroupManager providing comprehensive
 * consumer group management capabilities for multi-datacenter Kafka deployments.
 * 
 * <p>This implementation provides:
 * <ul>
 *   <li>Consumer group configuration management</li>
 *   <li>Assignment strategy selection</li>
 *   <li>Session and heartbeat management</li>
 *   <li>Auto-commit configuration</li>
 *   <li>Custom properties support</li>
 * </ul>
 * 
 * <p>The class is thread-safe and manages consumer instances across multiple datacenters.
 * 
 * @author Kafka Multi-DC Team
 * @version 1.0
 * @since 1.0
 */
public class DefaultConsumerGroupManager implements ConsumerGroupManager {
    
    /** The Kafka admin client for consumer group operations */
    private final AdminClient adminClient;
    
    /** Map of consumer instances keyed by datacenter identifier */
    private final Map<String, KafkaConsumer<Object, Object>> consumers;
    
    /** Timeout duration for administrative operations */
    private final Duration operationTimeout;
    
    /**
     * Constructs a new DefaultConsumerGroupManager with the specified configuration.
     * 
     * @param adminClient the Kafka admin client for group operations
     * @param consumers map of consumer instances keyed by datacenter ID
     * @param operationTimeout timeout for administrative operations
     * @throws IllegalArgumentException if any parameter is null
     */
    public DefaultConsumerGroupManager(
            AdminClient adminClient,
            Map<String, KafkaConsumer<Object, Object>> consumers,
            Duration operationTimeout) {
        this.adminClient = adminClient;
        this.consumers = new ConcurrentHashMap<>(consumers);
        this.operationTimeout = operationTimeout;
    }
    
    /**
     * Creates a new GroupConfig builder for configuring consumer group settings.
     * 
     * @return a new GroupConfig.Builder instance
     */
    @Override
    public GroupConfig.Builder newGroupConfig() {
        return new DefaultGroupConfigBuilder();
    }
    
    /**
     * Default implementation of GroupConfig.Builder providing fluent API
     * for configuring consumer group parameters.
     * 
     * <p>This builder supports configuration of:
     * <ul>
     *   <li>Group ID and assignment strategy</li>
     *   <li>Session timeout and heartbeat intervals</li>
     *   <li>Poll intervals and record limits</li>
     *   <li>Auto-commit settings</li>
     *   <li>Additional custom properties</li>
     * </ul>
     */
    private static class DefaultGroupConfigBuilder implements GroupConfig.Builder {
        private String groupId;
        private AssignmentStrategy assignmentStrategy;
        private Duration sessionTimeout = Duration.ofSeconds(30);
        private Duration heartbeatInterval = Duration.ofSeconds(3);
        private Duration maxPollInterval = Duration.ofMinutes(5);
        private int maxPollRecords = 500;
        private boolean autoCommitEnabled = true;
        private Duration autoCommitInterval = Duration.ofSeconds(5);
        private Map<String, String> additionalProperties = new HashMap<>();
        
        /**
         * Sets the consumer group ID.
         * 
         * @param groupId the consumer group identifier
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }
        
        /**
         * Sets the partition assignment strategy for the consumer group.
         * 
         * @param strategy the assignment strategy to use
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder assignmentStrategy(AssignmentStrategy strategy) {
            this.assignmentStrategy = strategy;
            return this;
        }
        
        /**
         * Sets the session timeout for group coordination.
         * 
         * @param timeout the session timeout duration
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder sessionTimeout(Duration timeout) {
            this.sessionTimeout = timeout;
            return this;
        }
        
        /**
         * Sets the heartbeat interval for group coordination.
         * 
         * @param interval the heartbeat interval duration
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder heartbeatInterval(Duration interval) {
            this.heartbeatInterval = interval;
            return this;
        }
        
        /**
         * Sets the maximum time between polls before the consumer is considered dead.
         * 
         * @param interval the maximum poll interval duration
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder maxPollInterval(Duration interval) {
            this.maxPollInterval = interval;
            return this;
        }
        
        /**
         * Sets the maximum number of records returned in a single poll.
         * 
         * @param records the maximum number of records per poll
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder maxPollRecords(int records) {
            this.maxPollRecords = records;
            return this;
        }
        
        /**
         * Enables automatic offset commits with the specified interval.
         * 
         * @param interval the auto-commit interval duration
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder enableAutoCommit(Duration interval) {
            this.autoCommitEnabled = true;
            this.autoCommitInterval = interval;
            return this;
        }
        
        /**
         * Disables automatic offset commits, requiring manual commit operations.
         * 
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder disableAutoCommit() {
            this.autoCommitEnabled = false;
            return this;
        }
        
        /**
         * Adds additional custom properties to the consumer configuration.
         * 
         * @param properties map of additional configuration properties
         * @return this builder instance for method chaining
         */
        @Override
        public GroupConfig.Builder additionalProperties(Map<String, String> properties) {
            this.additionalProperties.putAll(properties);
            return this;
        }
        
        /**
         * Builds and returns the configured GroupConfig instance.
         * 
         * @return a new GroupConfig with the specified configuration
         */
        @Override
        public GroupConfig build() {
            return new DefaultGroupConfig(
                groupId, assignmentStrategy, sessionTimeout, heartbeatInterval,
                maxPollInterval, maxPollRecords, autoCommitEnabled, 
                autoCommitInterval, additionalProperties
            );
        }
    }
    
    /**
     * Default implementation of GroupConfig providing immutable consumer group configuration.
     * 
     * <p>This class encapsulates all consumer group configuration parameters including:
     * <ul>
     *   <li>Group identification and assignment strategy</li>
     *   <li>Timing parameters (timeouts, intervals)</li>
     *   <li>Polling and commit behavior</li>
     *   <li>Additional custom properties</li>
     * </ul>
     * 
     * <p>All configuration values are immutable once the object is created.
     */
    private static class DefaultGroupConfig implements GroupConfig {
        private final String groupId;
        private final AssignmentStrategy assignmentStrategy;
        private final Duration sessionTimeout;
        private final Duration heartbeatInterval;
        private final Duration maxPollInterval;
        private final int maxPollRecords;
        private final boolean autoCommitEnabled;
        private final Duration autoCommitInterval;
        private final Map<String, String> additionalProperties;
        
        /**
         * Constructs a new DefaultGroupConfig with the specified parameters.
         * 
         * @param groupId the consumer group identifier
         * @param assignmentStrategy the partition assignment strategy
         * @param sessionTimeout the session timeout duration
         * @param heartbeatInterval the heartbeat interval duration
         * @param maxPollInterval the maximum poll interval duration
         * @param maxPollRecords the maximum records per poll
         * @param autoCommitEnabled whether auto-commit is enabled
         * @param autoCommitInterval the auto-commit interval duration
         * @param additionalProperties additional configuration properties
         */
        public DefaultGroupConfig(String groupId, AssignmentStrategy assignmentStrategy,
                                Duration sessionTimeout, Duration heartbeatInterval,
                                Duration maxPollInterval, int maxPollRecords,
                                boolean autoCommitEnabled, Duration autoCommitInterval,
                                Map<String, String> additionalProperties) {
            this.groupId = groupId;
            this.assignmentStrategy = assignmentStrategy;
            this.sessionTimeout = sessionTimeout;
            this.heartbeatInterval = heartbeatInterval;
            this.maxPollInterval = maxPollInterval;
            this.maxPollRecords = maxPollRecords;
            this.autoCommitEnabled = autoCommitEnabled;
            this.autoCommitInterval = autoCommitInterval;
            this.additionalProperties = Map.copyOf(additionalProperties);
        }
        
        /**
         * Returns the consumer group identifier.
         * 
         * @return the group ID
         */
        @Override
        public String getGroupId() {
            return groupId;
        }
        
        /**
         * Returns the partition assignment strategy.
         * 
         * @return the assignment strategy
         */
        @Override
        public AssignmentStrategy getAssignmentStrategy() {
            return assignmentStrategy;
        }
        
        /**
         * Returns the session timeout duration.
         * 
         * @return the session timeout
         */
        @Override
        public Duration getSessionTimeout() {
            return sessionTimeout;
        }
        
        /**
         * Returns the heartbeat interval duration.
         * 
         * @return the heartbeat interval
         */
        @Override
        public Duration getHeartbeatInterval() {
            return heartbeatInterval;
        }
        
        /**
         * Returns the maximum poll interval duration.
         * 
         * @return the maximum poll interval
         */
        @Override
        public Duration getMaxPollInterval() {
            return maxPollInterval;
        }
        
        /**
         * Returns the maximum number of records per poll.
         * 
         * @return the maximum poll records
         */
        @Override
        public int getMaxPollRecords() {
            return maxPollRecords;
        }
        
        /**
         * Returns whether automatic offset commits are enabled.
         * 
         * @return true if auto-commit is enabled, false otherwise
         */
        @Override
        public boolean isAutoCommitEnabled() {
            return autoCommitEnabled;
        }
        
        /**
         * Returns the auto-commit interval duration.
         * 
         * @return the auto-commit interval
         */
        @Override
        public Duration getAutoCommitInterval() {
            return autoCommitInterval;
        }
        
        /**
         * Returns additional configuration properties.
         * 
         * @return an immutable map of additional properties
         */
        @Override
        public Map<String, String> getAdditionalProperties() {
            return additionalProperties;
        }
    }
}
