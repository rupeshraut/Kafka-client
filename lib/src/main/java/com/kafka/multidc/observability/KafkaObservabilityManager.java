package com.kafka.multidc.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Advanced observability and performance monitoring for Kafka Multi-Datacenter Client.
 * Provides comprehensive metrics, tracing, and performance analytics.
 */
public interface KafkaObservabilityManager {
    
    /**
     * Performance metrics for producer operations.
     */
    interface ProducerMetrics {
        long getTotalRecordsSent();
        long getTotalBytesSent();
        double getAverageLatency();
        double getP95Latency();
        double getP99Latency();
        double getThroughputRecordsPerSecond();
        double getThroughputBytesPerSecond();
        long getErrorCount();
        double getErrorRate();
        Map<String, Object> getDatacenterMetrics();
    }
    
    /**
     * Performance metrics for consumer operations.
     */
    interface ConsumerMetrics {
        long getTotalRecordsConsumed();
        long getTotalBytesConsumed();
        double getAverageProcessingTime();
        double getP95ProcessingTime();
        double getP99ProcessingTime();
        double getConsumptionRateRecordsPerSecond();
        double getConsumptionRateBytesPerSecond();
        Map<TopicPartition, Long> getConsumerLag();
        Map<String, Object> getDatacenterMetrics();
    }
    
    /**
     * Connection pool metrics.
     */
    interface ConnectionPoolMetrics {
        int getActiveConnections();
        int getIdleConnections();
        int getTotalConnections();
        int getMaxPoolSize();
        double getConnectionUtilization();
        long getConnectionCreateCount();
        long getConnectionDestroyCount();
        double getAverageConnectionAge();
        Map<String, Object> getDatacenterConnectionMetrics();
    }
    
    /**
     * Schema registry metrics.
     */
    interface SchemaRegistryMetrics {
        long getTotalSchemaRequests();
        double getAverageSchemaLookupTime();
        double getSchemaCacheHitRate();
        double getSchemaCacheMissRate();
        long getSchemaRegistrationCount();
        long getSchemaEvolutionCount();
        Map<String, Object> getDatacenterSchemaMetrics();
    }
    
    /**
     * Distributed tracing information.
     */
    interface TraceInfo {
        String getTraceId();
        String getSpanId();
        String getParentSpanId();
        Map<String, String> getBaggage();
        Instant getStartTime();
        Duration getDuration();
        String getOperationName();
        Tags getTags();
    }
    
    /**
     * Performance alert configuration.
     */
    interface AlertConfig {
        Duration getLatencyThreshold();
        double getErrorRateThreshold();
        double getThroughputThreshold();
        Duration getLagThreshold();
        boolean isEnabled();
    }
    
    /**
     * Alert event information.
     */
    interface AlertEvent {
        enum Type {
            HIGH_LATENCY,
            HIGH_ERROR_RATE,
            LOW_THROUGHPUT,
            HIGH_CONSUMER_LAG,
            CONNECTION_POOL_EXHAUSTED,
            SCHEMA_REGISTRY_ERROR
        }
        
        Type getType();
        String getMessage();
        Instant getTimestamp();
        Map<String, Object> getDetails();
        String getDatacenterId();
        String getSeverity();
    }
    
    /**
     * Get current producer metrics.
     */
    ProducerMetrics getProducerMetrics();
    
    /**
     * Get producer metrics for a specific datacenter.
     */
    ProducerMetrics getProducerMetrics(String datacenterId);
    
    /**
     * Get current consumer metrics.
     */
    ConsumerMetrics getConsumerMetrics();
    
    /**
     * Get consumer metrics for a specific datacenter.
     */
    ConsumerMetrics getConsumerMetrics(String datacenterId);
    
    /**
     * Get connection pool metrics.
     */
    ConnectionPoolMetrics getConnectionPoolMetrics();
    
    /**
     * Get schema registry metrics.
     */
    SchemaRegistryMetrics getSchemaRegistryMetrics();
    
    /**
     * Start a new distributed trace.
     */
    TraceInfo startTrace(String operationName, Tags tags);
    
    /**
     * Continue an existing trace.
     */
    TraceInfo continueTrace(String traceId, String spanId, String operationName, Tags tags);
    
    /**
     * Finish a trace.
     */
    void finishTrace(TraceInfo trace);
    
    /**
     * Configure performance alerts.
     */
    void configureAlerts(AlertConfig config);
    
    /**
     * Register an alert handler.
     */
    void registerAlertHandler(AlertHandler handler);
    
    /**
     * Export metrics to external monitoring system.
     */
    CompletableFuture<Void> exportMetrics(MetricsExporter exporter);
    
    /**
     * Generate performance report.
     */
    CompletableFuture<PerformanceReport> generateReport(Duration period);
    
    /**
     * Reset all metrics.
     */
    void resetMetrics();
    
    /**
     * Enable detailed tracing.
     */
    void enableDetailedTracing();
    
    /**
     * Disable detailed tracing.
     */
    void disableDetailedTracing();
    
    /**
     * Check if detailed tracing is enabled.
     */
    boolean isDetailedTracingEnabled();
    
    /**
     * Handler for performance alerts.
     */
    @FunctionalInterface
    interface AlertHandler {
        void handle(AlertEvent event);
    }
    
    /**
     * Metrics exporter interface.
     */
    interface MetricsExporter {
        void export(Map<String, Object> metrics);
        String getExporterName();
        boolean isEnabled();
    }
    
    /**
     * Performance report.
     */
    interface PerformanceReport {
        Duration getReportPeriod();
        Instant getStartTime();
        Instant getEndTime();
        ProducerMetrics getProducerSummary();
        ConsumerMetrics getConsumerSummary();
        ConnectionPoolMetrics getConnectionSummary();
        SchemaRegistryMetrics getSchemaSummary();
        Map<String, Object> getDatacenterBreakdown();
        List<AlertEvent> getAlerts();
        String generateTextReport();
        String generateJsonReport();
    }
    
    /**
     * Builder for observability manager configuration.
     */
    interface Builder {
        Builder meterRegistry(MeterRegistry registry);
        Builder enableProducerMetrics();
        Builder enableConsumerMetrics();
        Builder enableConnectionPoolMetrics();
        Builder enableSchemaRegistryMetrics();
        Builder enableDistributedTracing();
        Builder alertConfig(AlertConfig config);
        Builder metricsExporter(MetricsExporter exporter);
        Builder detailedMetricsEnabled(boolean enabled);
        KafkaObservabilityManager build();
    }
    
    /**
     * Create a new observability manager builder.
     */
    static Builder builder() {
        throw new UnsupportedOperationException("Builder implementation not yet available");
    }
}
