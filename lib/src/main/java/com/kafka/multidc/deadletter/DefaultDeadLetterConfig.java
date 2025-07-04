package com.kafka.multidc.deadletter;

/**
 * Default implementation of DeadLetterConfig.
 */
public class DefaultDeadLetterConfig implements DeadLetterQueueHandler.DeadLetterConfig {
    
    private final String deadLetterTopicSuffix;
    private final int maxRetryAttempts;
    private final boolean enabled;
    private final DeadLetterQueueHandler.DeadLetterStrategy strategy;
    
    private DefaultDeadLetterConfig(Builder builder) {
        this.deadLetterTopicSuffix = builder.deadLetterTopicSuffix;
        this.maxRetryAttempts = builder.maxRetryAttempts;
        this.enabled = builder.enabled;
        this.strategy = builder.strategy;
    }
    
    @Override
    public String getDeadLetterTopicSuffix() {
        return deadLetterTopicSuffix;
    }
    
    @Override
    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public DeadLetterQueueHandler.DeadLetterStrategy getStrategy() {
        return strategy;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String deadLetterTopicSuffix = ".dlq";
        private int maxRetryAttempts = 3;
        private boolean enabled = true;
        private DeadLetterQueueHandler.DeadLetterStrategy strategy = DeadLetterQueueHandler.DeadLetterStrategy.TOPIC_BASED;
        
        public Builder deadLetterTopicSuffix(String deadLetterTopicSuffix) {
            this.deadLetterTopicSuffix = deadLetterTopicSuffix;
            return this;
        }
        
        public Builder maxRetryAttempts(int maxRetryAttempts) {
            this.maxRetryAttempts = maxRetryAttempts;
            return this;
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder strategy(DeadLetterQueueHandler.DeadLetterStrategy strategy) {
            this.strategy = strategy;
            return this;
        }
        
        public DefaultDeadLetterConfig build() {
            return new DefaultDeadLetterConfig(this);
        }
    }
}
