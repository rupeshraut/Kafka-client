package com.kafka.multidc.performance;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Performance optimization and monitoring utilities for Kafka Multi-Datacenter Client.
 * Provides advanced performance tuning, monitoring, and optimization capabilities.
 */
public interface PerformanceOptimizationManager {
    
    /**
     * Performance optimization strategies.
     */
    enum OptimizationStrategy {
        THROUGHPUT_OPTIMIZED,
        LATENCY_OPTIMIZED,
        BALANCED,
        CUSTOM
    }
    
    /**
     * Batching strategies for producers.
     */
    enum BatchingStrategy {
        TIME_BASED,
        SIZE_BASED,
        ADAPTIVE,
        DISABLED
    }
    
    /**
     * Connection pooling strategies.
     */
    enum PoolingStrategy {
        ROUND_ROBIN,
        LEAST_CONNECTIONS,
        WEIGHTED_ROUND_ROBIN,
        ADAPTIVE
    }
    
    /**
     * Performance metrics snapshot.
     */
    interface PerformanceSnapshot {
        Instant getTimestamp();
        Duration getMeasurementPeriod();
        double getProducerThroughput();
        double getConsumerThroughput();
        Duration getAverageLatency();
        Duration getP95Latency();
        Duration getP99Latency();
        double getErrorRate();
        Map<String, Object> getDatacenterMetrics();
        Map<TopicPartition, Long> getConsumerLag();
    }
    
    /**
     * Performance tuning configuration.
     */
    interface TuningConfig {
        OptimizationStrategy getStrategy();
        BatchingStrategy getBatchingStrategy();
        PoolingStrategy getPoolingStrategy();
        int getBatchSize();
        Duration getBatchTimeout();
        int getConnectionPoolSize();
        Duration getConnectionTimeout();
        int getRetryAttempts();
        Duration getRetryBackoff();
        Map<String, Object> getCustomParameters();
    }
    
    /**
     * Performance alert configuration.
     */
    interface AlertConfig {
        Duration getLatencyThreshold();
        double getThroughputThreshold();
        double getErrorRateThreshold();
        Duration getLagThreshold();
        boolean isEnabled();
    }
    
    /**
     * Performance optimization recommendations.
     */
    interface OptimizationRecommendation {
        enum Type {
            INCREASE_BATCH_SIZE,
            DECREASE_BATCH_TIMEOUT,
            INCREASE_CONNECTION_POOL,
            ENABLE_COMPRESSION,
            ADJUST_RETRY_POLICY,
            TUNE_CONSUMER_CONFIG,
            OPTIMIZE_SERIALIZATION,
            DATACENTER_REBALANCING
        }
        
        Type getType();
        String getDescription();
        Map<String, Object> getSuggestedParameters();
        double getExpectedImprovement();
        String getJustification();
    }
    
    /**
     * Apply performance tuning configuration.
     */
    void applyTuningConfig(TuningConfig config);
    
    /**
     * Apply tuning configuration for a specific datacenter.
     */
    void applyDatacenterTuning(String datacenterId, TuningConfig config);
    
    /**
     * Get current performance snapshot.
     */
    PerformanceSnapshot getCurrentSnapshot();
    
    /**
     * Get performance snapshot for a specific datacenter.
     */
    PerformanceSnapshot getDatacenterSnapshot(String datacenterId);
    
    /**
     * Get performance history over a time period.
     */
    List<PerformanceSnapshot> getPerformanceHistory(Duration period);
    
    /**
     * Analyze performance and get optimization recommendations.
     */
    CompletableFuture<List<OptimizationRecommendation>> analyzePerformance();
    
    /**
     * Analyze performance for a specific datacenter.
     */
    CompletableFuture<List<OptimizationRecommendation>> analyzeDatacenterPerformance(String datacenterId);
    
    /**
     * Enable automatic performance tuning.
     */
    void enableAutoTuning();
    
    /**
     * Disable automatic performance tuning.
     */
    void disableAutoTuning();
    
    /**
     * Check if automatic tuning is enabled.
     */
    boolean isAutoTuningEnabled();
    
    /**
     * Configure performance alerts.
     */
    void configureAlerts(AlertConfig alertConfig);
    
    /**
     * Register a performance alert handler.
     */
    void registerAlertHandler(AlertHandler handler);
    
    /**
     * Start performance monitoring.
     */
    void startMonitoring();
    
    /**
     * Stop performance monitoring.
     */
    void stopMonitoring();
    
    /**
     * Export performance metrics to external system.
     */
    CompletableFuture<Void> exportMetrics(MetricsExporter exporter);
    
    /**
     * Generate performance report.
     */
    CompletableFuture<PerformanceReport> generateReport(Duration period);
    
    /**
     * Optimize producer configuration automatically.
     */
    Map<String, Object> optimizeProducerConfig(Map<String, Object> currentConfig);
    
    /**
     * Optimize consumer configuration automatically.
     */
    Map<String, Object> optimizeConsumerConfig(Map<String, Object> currentConfig);
    
    /**
     * Benchmark current configuration.
     */
    CompletableFuture<BenchmarkResult> runBenchmark(BenchmarkConfig config);
    
    /**
     * Performance alert handler.
     */
    @FunctionalInterface
    interface AlertHandler {
        void handleAlert(PerformanceAlert alert);
    }
    
    /**
     * Performance alert information.
     */
    interface PerformanceAlert {
        enum Severity {
            INFO,
            WARNING,
            CRITICAL
        }
        
        Severity getSeverity();
        String getMessage();
        Instant getTimestamp();
        String getDatacenterId();
        Map<String, Object> getMetrics();
        List<OptimizationRecommendation> getRecommendations();
    }
    
    /**
     * Metrics exporter interface.
     */
    interface MetricsExporter {
        void export(PerformanceSnapshot snapshot);
        String getExporterName();
        boolean isEnabled();
    }
    
    /**
     * Performance report.
     */
    interface PerformanceReport {
        Duration getReportPeriod();
        PerformanceSnapshot getSummary();
        List<PerformanceSnapshot> getHistory();
        List<OptimizationRecommendation> getRecommendations();
        Map<String, PerformanceSnapshot> getDatacenterBreakdown();
        String generateTextReport();
        String generateJsonReport();
    }
    
    /**
     * Benchmark configuration.
     */
    interface BenchmarkConfig {
        Duration getDuration();
        int getThreadCount();
        int getMessageSize();
        int getMessageCount();
        List<String> getTopics();
        Map<String, Object> getProducerConfig();
        Map<String, Object> getConsumerConfig();
    }
    
    /**
     * Benchmark result.
     */
    interface BenchmarkResult {
        Duration getDuration();
        long getMessagesSent();
        long getMessagesReceived();
        double getThroughputMbps();
        Duration getAverageLatency();
        Duration getP95Latency();
        Duration getP99Latency();
        double getErrorRate();
        Map<String, Object> getDetailedMetrics();
    }
    
    /**
     * Builder for tuning configuration.
     */
    interface TuningConfigBuilder {
        TuningConfigBuilder strategy(OptimizationStrategy strategy);
        TuningConfigBuilder batching(BatchingStrategy strategy, int batchSize, Duration timeout);
        TuningConfigBuilder pooling(PoolingStrategy strategy, int poolSize);
        TuningConfigBuilder retries(int attempts, Duration backoff);
        TuningConfigBuilder connectionTimeout(Duration timeout);
        TuningConfigBuilder customParameter(String key, Object value);
        TuningConfig build();
    }
    
    /**
     * Create a new tuning configuration builder.
     */
    TuningConfigBuilder newTuningConfigBuilder();
}
