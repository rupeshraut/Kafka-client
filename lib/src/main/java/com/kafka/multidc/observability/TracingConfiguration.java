package com.kafka.multidc.observability;

import java.util.Map;

/**
 * Configuration for distributed tracing in the Kafka client.
 */
public class TracingConfiguration {
    private final boolean enabled;
    private final String serviceName;
    private final double samplingRate;
    private final Map<String, String> tracingHeaders;
    private final String tracingBackend; // "jaeger", "zipkin", "noop"
    private final String exporterEndpoint;
    private final boolean enableDetailedTracing;
    private final long maxSpanDuration;
    
    private TracingConfiguration(Builder builder) {
        this.enabled = builder.enabled;
        this.serviceName = builder.serviceName;
        this.samplingRate = builder.samplingRate;
        this.tracingHeaders = builder.tracingHeaders;
        this.tracingBackend = builder.tracingBackend;
        this.exporterEndpoint = builder.exporterEndpoint;
        this.enableDetailedTracing = builder.enableDetailedTracing;
        this.maxSpanDuration = builder.maxSpanDuration;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public String getServiceName() {
        return serviceName;
    }
    
    public double getSamplingRate() {
        return samplingRate;
    }
    
    public Map<String, String> getTracingHeaders() {
        return tracingHeaders;
    }
    
    public String getTracingBackend() {
        return tracingBackend;
    }
    
    public String getExporterEndpoint() {
        return exporterEndpoint;
    }
    
    public boolean isDetailedTracingEnabled() {
        return enableDetailedTracing;
    }
    
    public long getMaxSpanDuration() {
        return maxSpanDuration;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private boolean enabled = false;
        private String serviceName = "kafka-multidc-client";
        private double samplingRate = 0.1; // 10% sampling by default
        private Map<String, String> tracingHeaders = Map.of();
        private String tracingBackend = "noop";
        private String exporterEndpoint;
        private boolean enableDetailedTracing = false;
        private long maxSpanDuration = 30000; // 30 seconds default
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }
        
        public Builder samplingRate(double samplingRate) {
            if (samplingRate < 0.0 || samplingRate > 1.0) {
                throw new IllegalArgumentException("Sampling rate must be between 0.0 and 1.0");
            }
            this.samplingRate = samplingRate;
            return this;
        }
        
        public Builder tracingHeaders(Map<String, String> tracingHeaders) {
            this.tracingHeaders = tracingHeaders != null ? Map.copyOf(tracingHeaders) : Map.of();
            return this;
        }
        
        public Builder tracingBackend(String tracingBackend) {
            this.tracingBackend = tracingBackend;
            return this;
        }
        
        public Builder exporterEndpoint(String exporterEndpoint) {
            this.exporterEndpoint = exporterEndpoint;
            return this;
        }
        
        public Builder enableDetailedTracing(boolean enableDetailedTracing) {
            this.enableDetailedTracing = enableDetailedTracing;
            return this;
        }
        
        public Builder maxSpanDuration(long maxSpanDuration) {
            this.maxSpanDuration = maxSpanDuration;
            return this;
        }
        
        public TracingConfiguration build() {
            if (enabled && (serviceName == null || serviceName.trim().isEmpty())) {
                throw new IllegalArgumentException("Service name is required when tracing is enabled");
            }
            return new TracingConfiguration(this);
        }
    }
}
