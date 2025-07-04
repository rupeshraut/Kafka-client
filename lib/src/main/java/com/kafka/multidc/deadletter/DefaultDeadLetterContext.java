package com.kafka.multidc.deadletter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Default implementation of DeadLetterContext.
 */
public class DefaultDeadLetterContext implements DeadLetterQueueHandler.DeadLetterContext {
    
    private final ConsumerRecord<?, ?> originalRecord;
    private final Exception failureReason;
    private final int retryAttempt;
    private final String datacenterId;
    private final long firstFailureTimestamp;
    private final long lastFailureTimestamp;
    
    private DefaultDeadLetterContext(Builder builder) {
        this.originalRecord = builder.originalRecord;
        this.failureReason = builder.failureReason;
        this.retryAttempt = builder.retryAttempt;
        this.datacenterId = builder.datacenterId;
        this.firstFailureTimestamp = builder.firstFailureTimestamp;
        this.lastFailureTimestamp = builder.lastFailureTimestamp;
    }
    
    @Override
    public ConsumerRecord<?, ?> getOriginalRecord() {
        return originalRecord;
    }
    
    @Override
    public Exception getFailureReason() {
        return failureReason;
    }
    
    @Override
    public int getRetryAttempt() {
        return retryAttempt;
    }
    
    @Override
    public String getDatacenterId() {
        return datacenterId;
    }
    
    @Override
    public long getFirstFailureTimestamp() {
        return firstFailureTimestamp;
    }
    
    @Override
    public long getLastFailureTimestamp() {
        return lastFailureTimestamp;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private ConsumerRecord<?, ?> originalRecord;
        private Exception failureReason;
        private int retryAttempt;
        private String datacenterId;
        private long firstFailureTimestamp;
        private long lastFailureTimestamp;
        
        public Builder originalRecord(ConsumerRecord<?, ?> originalRecord) {
            this.originalRecord = originalRecord;
            return this;
        }
        
        public Builder failureReason(Exception failureReason) {
            this.failureReason = failureReason;
            return this;
        }
        
        public Builder retryAttempt(int retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }
        
        public Builder datacenterId(String datacenterId) {
            this.datacenterId = datacenterId;
            return this;
        }
        
        public Builder firstFailureTimestamp(long firstFailureTimestamp) {
            this.firstFailureTimestamp = firstFailureTimestamp;
            return this;
        }
        
        public Builder lastFailureTimestamp(long lastFailureTimestamp) {
            this.lastFailureTimestamp = lastFailureTimestamp;
            return this;
        }
        
        public DefaultDeadLetterContext build() {
            if (originalRecord == null) {
                throw new IllegalArgumentException("originalRecord cannot be null");
            }
            if (failureReason == null) {
                throw new IllegalArgumentException("failureReason cannot be null");
            }
            if (datacenterId == null) {
                throw new IllegalArgumentException("datacenterId cannot be null");
            }
            
            if (firstFailureTimestamp == 0) {
                this.firstFailureTimestamp = System.currentTimeMillis();
            }
            if (lastFailureTimestamp == 0) {
                this.lastFailureTimestamp = System.currentTimeMillis();
            }
            
            return new DefaultDeadLetterContext(this);
        }
    }
}
