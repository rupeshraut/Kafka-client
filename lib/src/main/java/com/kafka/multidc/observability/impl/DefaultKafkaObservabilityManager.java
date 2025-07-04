package com.kafka.multidc.observability.impl;

import com.kafka.multidc.observability.KafkaObservabilityManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of KafkaObservabilityManager providing comprehensive
 * observability and monitoring capabilities for the Kafka Multi-Datacenter Client.
 */
public class DefaultKafkaObservabilityManager implements KafkaObservabilityManager {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultKafkaObservabilityManager.class);
    
    private final MeterRegistry meterRegistry;
    private final Map<String, ProducerMetricsImpl> producerMetrics = new ConcurrentHashMap<>();
    private final Map<String, ConsumerMetricsImpl> consumerMetrics = new ConcurrentHashMap<>();
    private final ConnectionPoolMetricsImpl connectionPoolMetrics;
    private final SchemaRegistryMetricsImpl schemaRegistryMetrics;
    private final List<AlertHandler> alertHandlers = new ArrayList<>();
    private final AtomicReference<AlertConfig> alertConfig = new AtomicReference<>();
    private volatile boolean detailedTracingEnabled = false;
    
    public DefaultKafkaObservabilityManager(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.connectionPoolMetrics = new ConnectionPoolMetricsImpl(meterRegistry);
        this.schemaRegistryMetrics = new SchemaRegistryMetricsImpl(meterRegistry);
        logger.info("DefaultKafkaObservabilityManager initialized with MeterRegistry: {}", meterRegistry.getClass().getSimpleName());
    }
    
    @Override
    public ProducerMetrics getProducerMetrics(String datacenterId) {
        return producerMetrics.computeIfAbsent(datacenterId, dc -> new ProducerMetricsImpl(dc, meterRegistry));
    }
    
    @Override
    public ConsumerMetrics getConsumerMetrics(String datacenterId) {
        return consumerMetrics.computeIfAbsent(datacenterId, dc -> new ConsumerMetricsImpl(dc, meterRegistry));
    }
    
    @Override
    public ConsumerMetrics getConsumerMetrics() {
        // Return metrics for the default datacenter or a combined view
        return consumerMetrics.values().stream().findFirst()
            .orElse(new ConsumerMetricsImpl("default", meterRegistry));
    }
    
    @Override
    public ProducerMetrics getProducerMetrics() {
        // Return metrics for the default datacenter or a combined view
        return producerMetrics.values().stream().findFirst()
            .orElse(new ProducerMetricsImpl("default", meterRegistry));
    }
    
    @Override
    public ConnectionPoolMetrics getConnectionPoolMetrics() {
        return connectionPoolMetrics;
    }
    
    @Override
    public SchemaRegistryMetrics getSchemaRegistryMetrics() {
        return schemaRegistryMetrics;
    }
    
    @Override
    public TraceInfo startTrace(String operationName, Tags tags) {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        return new TraceInfoImpl(traceId, spanId, null, operationName, tags, Instant.now());
    }
    
    @Override
    public TraceInfo continueTrace(String traceId, String spanId, String operationName, Tags tags) {
        String newSpanId = UUID.randomUUID().toString();
        return new TraceInfoImpl(traceId, newSpanId, spanId, operationName, tags, Instant.now());
    }
    
    @Override
    public void finishTrace(TraceInfo trace) {
        Duration duration = Duration.between(trace.getStartTime(), Instant.now());
        Timer.Sample sample = Timer.start(meterRegistry);
        sample.stop(Timer.builder("kafka.multidc.trace")
            .description("Kafka Multi-DC operation trace")
            .tags(trace.getTags())
            .register(meterRegistry));
        
        logger.trace("Finished trace: {} operation {} duration {}", 
            trace.getTraceId(), trace.getOperationName(), duration);
    }
    
    @Override
    public void configureAlerts(AlertConfig config) {
        this.alertConfig.set(config);
        logger.info("Alert configuration updated");
    }
    
    @Override
    public void registerAlertHandler(AlertHandler handler) {
        alertHandlers.add(handler);
        logger.info("Registered alert handler: {}", handler.getClass().getSimpleName());
    }
    
    @Override
    public CompletableFuture<Void> exportMetrics(MetricsExporter exporter) {
        return CompletableFuture.runAsync(() -> {
            try {
                Map<String, Object> allMetrics = new HashMap<>();
                
                // Export producer metrics
                producerMetrics.forEach((dc, metrics) -> {
                    allMetrics.put("producer." + dc + ".totalRecordsSent", metrics.getTotalRecordsSent());
                    allMetrics.put("producer." + dc + ".totalBytesSent", metrics.getTotalBytesSent());
                    allMetrics.put("producer." + dc + ".averageLatency", metrics.getAverageLatency());
                    allMetrics.put("producer." + dc + ".errorRate", metrics.getErrorRate());
                });
                
                // Export consumer metrics
                consumerMetrics.forEach((dc, metrics) -> {
                    allMetrics.put("consumer." + dc + ".totalRecordsConsumed", metrics.getTotalRecordsConsumed());
                    allMetrics.put("consumer." + dc + ".totalBytesConsumed", metrics.getTotalBytesConsumed());
                    allMetrics.put("consumer." + dc + ".averageProcessingTime", metrics.getAverageProcessingTime());
                });
                
                // Export connection pool metrics
                allMetrics.put("connectionPool.activeConnections", connectionPoolMetrics.getActiveConnections());
                allMetrics.put("connectionPool.totalConnections", connectionPoolMetrics.getTotalConnections());
                
                // Export schema registry metrics
                allMetrics.put("schemaRegistry.cacheHitRate", schemaRegistryMetrics.getSchemaCacheHitRate());
                allMetrics.put("schemaRegistry.totalRequests", schemaRegistryMetrics.getTotalSchemaRequests());
                
                exporter.export(allMetrics);
                logger.debug("Exported {} metrics", allMetrics.size());
            } catch (Exception e) {
                logger.error("Failed to export metrics", e);
                throw new RuntimeException("Metrics export failed", e);
            }
        });
    }
    
    @Override
    public CompletableFuture<PerformanceReport> generateReport(Duration period) {
        return CompletableFuture.supplyAsync(() -> 
            new PerformanceReportImpl(period, producerMetrics, consumerMetrics, connectionPoolMetrics, schemaRegistryMetrics)
        );
    }
    
    @Override
    public void resetMetrics() {
        producerMetrics.clear();
        consumerMetrics.clear();
        connectionPoolMetrics.reset();
        schemaRegistryMetrics.reset();
        logger.info("All metrics have been reset");
    }
    
    @Override
    public void enableDetailedTracing() {
        detailedTracingEnabled = true;
        logger.info("Detailed tracing enabled");
    }
    
    @Override
    public void disableDetailedTracing() {
        detailedTracingEnabled = false;
        logger.info("Detailed tracing disabled");
    }
    
    @Override
    public boolean isDetailedTracingEnabled() {
        return detailedTracingEnabled;
    }
    
    // Inner implementation classes
    
    private static class ProducerMetricsImpl implements ProducerMetrics {
        private final String datacenter;
        private final AtomicLong totalRecordsSent = new AtomicLong(0);
        private final AtomicLong totalBytesSent = new AtomicLong(0);
        private final AtomicLong errorCount = new AtomicLong(0);
        private final List<Double> latencyHistory = Collections.synchronizedList(new ArrayList<>());
        private final Counter recordCounter;
        private final Counter byteCounter;
        private final Timer latencyTimer;
        
        public ProducerMetricsImpl(String datacenter, MeterRegistry meterRegistry) {
            this.datacenter = datacenter;
            Tags tags = Tags.of("datacenter", datacenter);
            
            this.recordCounter = Counter.builder("kafka.multidc.producer.records")
                .description("Total records sent")
                .tags(tags)
                .register(meterRegistry);
                
            this.byteCounter = Counter.builder("kafka.multidc.producer.bytes")
                .description("Total bytes sent")
                .tags(tags)
                .register(meterRegistry);
                
            this.latencyTimer = Timer.builder("kafka.multidc.producer.latency")
                .description("Producer latency")
                .tags(tags)
                .register(meterRegistry);
        }
        
        @Override
        public long getTotalRecordsSent() {
            return totalRecordsSent.get();
        }
        
        @Override
        public long getTotalBytesSent() {
            return totalBytesSent.get();
        }
        
        @Override
        public double getAverageLatency() {
            return latencyHistory.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
        
        @Override
        public double getP95Latency() {
            return calculatePercentile(95);
        }
        
        @Override
        public double getP99Latency() {
            return calculatePercentile(99);
        }
        
        @Override
        public double getThroughputRecordsPerSecond() {
            return recordCounter.count() / 60.0;
        }
        
        @Override
        public double getThroughputBytesPerSecond() {
            return byteCounter.count() / 60.0;
        }
        
        @Override
        public long getErrorCount() {
            return errorCount.get();
        }
        
        @Override
        public double getErrorRate() {
            long total = totalRecordsSent.get();
            return total > 0 ? (double) errorCount.get() / total : 0.0;
        }
        
        @Override
        public Map<String, Object> getDatacenterMetrics() {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("datacenter", datacenter);
            metrics.put("totalRecordsSent", getTotalRecordsSent());
            metrics.put("totalBytesSent", getTotalBytesSent());
            metrics.put("averageLatency", getAverageLatency());
            metrics.put("errorRate", getErrorRate());
            return metrics;
        }
        
        private double calculatePercentile(int percentile) {
            if (latencyHistory.isEmpty()) return 0.0;
            
            List<Double> sorted = new ArrayList<>(latencyHistory);
            Collections.sort(sorted);
            
            int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
            return sorted.get(Math.max(0, index));
        }
    }
    
    private static class ConsumerMetricsImpl implements ConsumerMetrics {
        private final String datacenter;
        private final AtomicLong totalRecordsConsumed = new AtomicLong(0);
        private final AtomicLong totalBytesConsumed = new AtomicLong(0);
        private final Map<TopicPartition, Long> consumerLag = new ConcurrentHashMap<>();
        private final List<Double> processingTimeHistory = Collections.synchronizedList(new ArrayList<>());
        
        public ConsumerMetricsImpl(String datacenter, MeterRegistry meterRegistry) {
            this.datacenter = datacenter;
            Tags tags = Tags.of("datacenter", datacenter);
            
            Counter.builder("kafka.multidc.consumer.records")
                .description("Total records consumed")
                .tags(tags)
                .register(meterRegistry);
                
            Counter.builder("kafka.multidc.consumer.bytes")
                .description("Total bytes consumed")
                .tags(tags)
                .register(meterRegistry);
        }
        
        @Override
        public long getTotalRecordsConsumed() {
            return totalRecordsConsumed.get();
        }
        
        @Override
        public long getTotalBytesConsumed() {
            return totalBytesConsumed.get();
        }
        
        @Override
        public double getAverageProcessingTime() {
            return processingTimeHistory.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
        
        @Override
        public double getP95ProcessingTime() {
            return calculatePercentile(95);
        }
        
        @Override
        public double getP99ProcessingTime() {
            return calculatePercentile(99);
        }
        
        @Override
        public double getConsumptionRateRecordsPerSecond() {
            return totalRecordsConsumed.get() / 60.0;
        }
        
        @Override
        public double getConsumptionRateBytesPerSecond() {
            return totalBytesConsumed.get() / 60.0;
        }
        
        @Override
        public Map<TopicPartition, Long> getConsumerLag() {
            return new HashMap<>(consumerLag);
        }
        
        @Override
        public Map<String, Object> getDatacenterMetrics() {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("datacenter", datacenter);
            metrics.put("totalRecordsConsumed", getTotalRecordsConsumed());
            metrics.put("totalBytesConsumed", getTotalBytesConsumed());
            metrics.put("averageProcessingTime", getAverageProcessingTime());
            metrics.put("consumerLag", getConsumerLag());
            return metrics;
        }
        
        private double calculatePercentile(int percentile) {
            if (processingTimeHistory.isEmpty()) return 0.0;
            
            List<Double> sorted = new ArrayList<>(processingTimeHistory);
            Collections.sort(sorted);
            
            int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
            return sorted.get(Math.max(0, index));
        }
    }
    
    private static class ConnectionPoolMetricsImpl implements ConnectionPoolMetrics {
        private final AtomicLong activeConnections = new AtomicLong(0);
        private final AtomicLong idleConnections = new AtomicLong(0);
        private final AtomicLong totalConnections = new AtomicLong(0);
        private final AtomicLong connectionCreateCount = new AtomicLong(0);
        private final AtomicLong connectionDestroyCount = new AtomicLong(0);
        private final int maxPoolSize = 100; // Default max pool size
        
        public ConnectionPoolMetricsImpl(MeterRegistry meterRegistry) {
            Gauge.builder("kafka.multidc.pool.active", this, obj -> obj.activeConnections.get())
                .description("Active connections")
                .register(meterRegistry);
                
            Gauge.builder("kafka.multidc.pool.idle", this, obj -> obj.idleConnections.get())
                .description("Idle connections")
                .register(meterRegistry);
        }
        
        @Override
        public int getActiveConnections() {
            return (int) activeConnections.get();
        }
        
        @Override
        public int getIdleConnections() {
            return (int) idleConnections.get();
        }
        
        @Override
        public int getTotalConnections() {
            return (int) totalConnections.get();
        }
        
        @Override
        public int getMaxPoolSize() {
            return maxPoolSize;
        }
        
        @Override
        public double getConnectionUtilization() {
            int total = getTotalConnections();
            return total > 0 ? (double) getActiveConnections() / total : 0.0;
        }
        
        @Override
        public long getConnectionCreateCount() {
            return connectionCreateCount.get();
        }
        
        @Override
        public long getConnectionDestroyCount() {
            return connectionDestroyCount.get();
        }
        
        @Override
        public double getAverageConnectionAge() {
            // Simplified implementation - would need connection creation timestamps in production
            return 300.0; // 5 minutes default
        }
        
        @Override
        public Map<String, Object> getDatacenterConnectionMetrics() {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("activeConnections", getActiveConnections());
            metrics.put("idleConnections", getIdleConnections());
            metrics.put("totalConnections", getTotalConnections());
            metrics.put("maxPoolSize", getMaxPoolSize());
            metrics.put("connectionUtilization", getConnectionUtilization());
            return metrics;
        }
        
        public void reset() {
            activeConnections.set(0);
            idleConnections.set(0);
            totalConnections.set(0);
            connectionCreateCount.set(0);
            connectionDestroyCount.set(0);
        }
    }
    
    private static class SchemaRegistryMetricsImpl implements SchemaRegistryMetrics {
        private final AtomicLong totalSchemaRequests = new AtomicLong(0);
        private final AtomicLong cacheHits = new AtomicLong(0);
        private final AtomicLong cacheMisses = new AtomicLong(0);
        private final AtomicLong schemaRegistrationCount = new AtomicLong(0);
        private final AtomicLong schemaEvolutionCount = new AtomicLong(0);
        private final List<Double> lookupTimeHistory = Collections.synchronizedList(new ArrayList<>());
        
        public SchemaRegistryMetricsImpl(MeterRegistry meterRegistry) {
            Counter.builder("kafka.multidc.schema.requests")
                .description("Total schema requests")
                .register(meterRegistry);
                
            Counter.builder("kafka.multidc.schema.cache.hits")
                .description("Schema cache hits")
                .register(meterRegistry);
                
            Counter.builder("kafka.multidc.schema.cache.misses")
                .description("Schema cache misses")
                .register(meterRegistry);
        }
        
        @Override
        public long getTotalSchemaRequests() {
            return totalSchemaRequests.get();
        }
        
        @Override
        public double getAverageSchemaLookupTime() {
            return lookupTimeHistory.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
        
        @Override
        public double getSchemaCacheHitRate() {
            long total = cacheHits.get() + cacheMisses.get();
            return total > 0 ? (double) cacheHits.get() / total : 0.0;
        }
        
        @Override
        public double getSchemaCacheMissRate() {
            long total = cacheHits.get() + cacheMisses.get();
            return total > 0 ? (double) cacheMisses.get() / total : 0.0;
        }
        
        @Override
        public long getSchemaRegistrationCount() {
            return schemaRegistrationCount.get();
        }
        
        @Override
        public long getSchemaEvolutionCount() {
            return schemaEvolutionCount.get();
        }
        
        @Override
        public Map<String, Object> getDatacenterSchemaMetrics() {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("totalSchemaRequests", getTotalSchemaRequests());
            metrics.put("averageSchemaLookupTime", getAverageSchemaLookupTime());
            metrics.put("schemaCacheHitRate", getSchemaCacheHitRate());
            metrics.put("schemaRegistrationCount", getSchemaRegistrationCount());
            metrics.put("schemaEvolutionCount", getSchemaEvolutionCount());
            return metrics;
        }
        
        public void reset() {
            totalSchemaRequests.set(0);
            cacheHits.set(0);
            cacheMisses.set(0);
            schemaRegistrationCount.set(0);
            schemaEvolutionCount.set(0);
            lookupTimeHistory.clear();
        }
    }
    
    private static class PerformanceReportImpl implements PerformanceReport {
        private final Duration reportPeriod;
        private final Instant startTime;
        private final Instant endTime;
        private final Map<String, ProducerMetricsImpl> producerMetrics;
        private final Map<String, ConsumerMetricsImpl> consumerMetrics;
        private final ConnectionPoolMetricsImpl connectionPoolMetrics;
        private final SchemaRegistryMetricsImpl schemaRegistryMetrics;
        
        public PerformanceReportImpl(Duration reportPeriod, 
                                   Map<String, ProducerMetricsImpl> producerMetrics,
                                   Map<String, ConsumerMetricsImpl> consumerMetrics,
                                   ConnectionPoolMetricsImpl connectionPoolMetrics,
                                   SchemaRegistryMetricsImpl schemaRegistryMetrics) {
            this.reportPeriod = reportPeriod;
            this.endTime = Instant.now();
            this.startTime = endTime.minus(reportPeriod);
            this.producerMetrics = new HashMap<>(producerMetrics);
            this.consumerMetrics = new HashMap<>(consumerMetrics);
            this.connectionPoolMetrics = connectionPoolMetrics;
            this.schemaRegistryMetrics = schemaRegistryMetrics;
        }
        
        @Override
        public Duration getReportPeriod() {
            return reportPeriod;
        }
        
        @Override
        public Instant getStartTime() {
            return startTime;
        }
        
        @Override
        public Instant getEndTime() {
            return endTime;
        }
        
        @Override
        public ProducerMetrics getProducerSummary() {
            // Return first producer metrics or a combined view
            return producerMetrics.values().stream().findFirst().orElse(null);
        }
        
        @Override
        public ConsumerMetrics getConsumerSummary() {
            // Return first consumer metrics or a combined view
            return consumerMetrics.values().stream().findFirst().orElse(null);
        }
        
        @Override
        public ConnectionPoolMetrics getConnectionSummary() {
            return connectionPoolMetrics;
        }
        
        @Override
        public SchemaRegistryMetrics getSchemaSummary() {
            return schemaRegistryMetrics;
        }
        
        @Override
        public Map<String, Object> getDatacenterBreakdown() {
            Map<String, Object> breakdown = new HashMap<>();
            
            Map<String, Object> producers = new HashMap<>();
            producerMetrics.forEach((dc, metrics) -> producers.put(dc, metrics.getDatacenterMetrics()));
            breakdown.put("producers", producers);
            
            Map<String, Object> consumers = new HashMap<>();
            consumerMetrics.forEach((dc, metrics) -> consumers.put(dc, metrics.getDatacenterMetrics()));
            breakdown.put("consumers", consumers);
            
            breakdown.put("connectionPool", connectionPoolMetrics.getDatacenterConnectionMetrics());
            breakdown.put("schemaRegistry", schemaRegistryMetrics.getDatacenterSchemaMetrics());
            
            return breakdown;
        }
        
        @Override
        public List<AlertEvent> getAlerts() {
            return new ArrayList<>(); // Simplified implementation
        }
        
        @Override
        public String generateTextReport() {
            StringBuilder report = new StringBuilder();
            report.append("Kafka Multi-Datacenter Performance Report\n");
            report.append("========================================\n");
            report.append("Period: ").append(reportPeriod).append("\n");
            report.append("Start: ").append(startTime).append("\n");
            report.append("End: ").append(endTime).append("\n\n");
            
            report.append("Producer Summary:\n");
            producerMetrics.forEach((dc, metrics) -> {
                report.append("  ").append(dc).append(":\n");
                report.append("    Records Sent: ").append(metrics.getTotalRecordsSent()).append("\n");
                report.append("    Bytes Sent: ").append(metrics.getTotalBytesSent()).append("\n");
                report.append("    Avg Latency: ").append(String.format("%.2f ms", metrics.getAverageLatency())).append("\n");
                report.append("    Error Rate: ").append(String.format("%.2f%%", metrics.getErrorRate() * 100)).append("\n");
            });
            
            report.append("\nConsumer Summary:\n");
            consumerMetrics.forEach((dc, metrics) -> {
                report.append("  ").append(dc).append(":\n");
                report.append("    Records Consumed: ").append(metrics.getTotalRecordsConsumed()).append("\n");
                report.append("    Bytes Consumed: ").append(metrics.getTotalBytesConsumed()).append("\n");
                report.append("    Avg Processing Time: ").append(String.format("%.2f ms", metrics.getAverageProcessingTime())).append("\n");
            });
            
            return report.toString();
        }
        
        @Override
        public String generateJsonReport() {
            // Simplified JSON generation - would use Jackson in production
            return "{ \"period\": \"" + reportPeriod + "\", \"timestamp\": \"" + endTime + "\" }";
        }
    }
    
    // TraceInfo implementation
    private static class TraceInfoImpl implements TraceInfo {
        private final String traceId;
        private final String spanId;
        private final String parentSpanId;
        private final String operationName;
        private final Tags tags;
        private final Instant startTime;
        private final Map<String, String> baggage = new HashMap<>();
        
        public TraceInfoImpl(String traceId, String spanId, String parentSpanId, 
                           String operationName, Tags tags, Instant startTime) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.parentSpanId = parentSpanId;
            this.operationName = operationName;
            this.tags = tags;
            this.startTime = startTime;
        }
        
        @Override
        public String getTraceId() {
            return traceId;
        }
        
        @Override
        public String getSpanId() {
            return spanId;
        }
        
        @Override
        public String getParentSpanId() {
            return parentSpanId;
        }
        
        @Override
        public Map<String, String> getBaggage() {
            return new HashMap<>(baggage);
        }
        
        @Override
        public Instant getStartTime() {
            return startTime;
        }
        
        @Override
        public Duration getDuration() {
            return Duration.between(startTime, Instant.now());
        }
        
        @Override
        public String getOperationName() {
            return operationName;
        }
        
        @Override
        public Tags getTags() {
            return tags;
        }
    }
}
