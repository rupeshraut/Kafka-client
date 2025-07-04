# Monitoring Guide

This guide provides comprehensive information on monitoring the Kafka Multi-Datacenter Client Library, including metrics collection, health checks, alerting, and observability best practices.

## Table of Contents

- [Overview](#overview)
- [Metrics Integration](#metrics-integration)
- [Health Indicators](#health-indicators)
- [Performance Monitoring](#performance-monitoring)
- [Alerting and Notifications](#alerting-and-notifications)
- [Distributed Tracing](#distributed-tracing)
- [Dashboard Configuration](#dashboard-configuration)
- [Custom Metrics](#custom-metrics)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

The Kafka Multi-Datacenter Client Library provides comprehensive monitoring capabilities:

- **Micrometer Integration**: Rich metrics collection and export
- **Spring Boot Actuator**: Health indicators and management endpoints
- **Distributed Tracing**: Integration with Jaeger and Zipkin
- **Custom Metrics**: Application-specific monitoring
- **Performance Analytics**: Real-time performance insights
- **Alerting**: Configurable alerts for critical conditions

## Metrics Integration

### Micrometer Configuration

```java
@Configuration
@EnableMetrics
public class MetricsConfiguration {
    
    @Bean
    public MeterRegistry meterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }
    
    @Bean
    public KafkaMultiDatacenterClientMetrics kafkaMetrics(MeterRegistry meterRegistry) {
        return new KafkaMultiDatacenterClientMetrics(meterRegistry);
    }
}
```

### Basic Metrics Collection

```java
@Component
public class KafkaMetricsCollector {
    
    private final MeterRegistry meterRegistry;
    private final KafkaMultiDatacenterClient kafkaClient;
    
    // Core metrics
    private final Timer sendLatencyTimer;
    private final Timer consumeLatencyTimer;
    private final Counter recordsSentCounter;
    private final Counter recordsConsumedCounter;
    private final Counter errorsCounter;
    private final Gauge connectionPoolGauge;
    
    public KafkaMetricsCollector(MeterRegistry meterRegistry, KafkaMultiDatacenterClient kafkaClient) {
        this.meterRegistry = meterRegistry;
        this.kafkaClient = kafkaClient;
        
        // Initialize metrics
        this.sendLatencyTimer = Timer.builder("kafka.producer.send.latency")
            .description("Producer send latency")
            .register(meterRegistry);
            
        this.consumeLatencyTimer = Timer.builder("kafka.consumer.consume.latency")
            .description("Consumer consume latency")
            .register(meterRegistry);
            
        this.recordsSentCounter = Counter.builder("kafka.producer.records.sent")
            .description("Number of records sent")
            .register(meterRegistry);
            
        this.recordsConsumedCounter = Counter.builder("kafka.consumer.records.consumed")
            .description("Number of records consumed")
            .register(meterRegistry);
            
        this.errorsCounter = Counter.builder("kafka.errors")
            .description("Number of errors")
            .register(meterRegistry);
            
        this.connectionPoolGauge = Gauge.builder("kafka.connection.pool.size")
            .description("Connection pool size")
            .register(meterRegistry, this, KafkaMetricsCollector::getConnectionPoolSize);
    }
    
    public void recordSendLatency(Duration latency, String datacenter, String topic) {
        sendLatencyTimer.record(latency.toMillis(), TimeUnit.MILLISECONDS,
            Tags.of("datacenter", datacenter, "topic", topic));
    }
    
    public void recordConsumeLatency(Duration latency, String datacenter, String topic) {
        consumeLatencyTimer.record(latency.toMillis(), TimeUnit.MILLISECONDS,
            Tags.of("datacenter", datacenter, "topic", topic));
    }
    
    public void recordSentRecord(String datacenter, String topic, boolean success) {
        recordsSentCounter.increment(Tags.of(
            "datacenter", datacenter, 
            "topic", topic, 
            "status", success ? "success" : "failure"
        ));
    }
    
    public void recordConsumedRecord(String datacenter, String topic) {
        recordsConsumedCounter.increment(Tags.of("datacenter", datacenter, "topic", topic));
    }
    
    public void recordError(String errorType, String datacenter, String operation) {
        errorsCounter.increment(Tags.of(
            "error.type", errorType,
            "datacenter", datacenter,
            "operation", operation
        ));
    }
    
    private double getConnectionPoolSize() {
        // Use the aggregated connection metrics from the actual API
        KafkaConnectionPoolManager.AggregatedMetrics metrics = kafkaClient.getAggregatedConnectionMetrics();
        return metrics.getTotalActiveConnections();
    }
}
```

### Advanced Metrics with Custom Tags

```java
@Component
public class AdvancedKafkaMetrics {
    
    private final MeterRegistry meterRegistry;
    
    // Partition-level metrics
    private final Map<String, Timer> partitionLatencyTimers = new ConcurrentHashMap<>();
    private final Map<String, Counter> partitionThroughputCounters = new ConcurrentHashMap<>();
    
    // Datacenter-level metrics
    private final Map<String, Gauge> datacenterHealthGauges = new ConcurrentHashMap<>();
    private final Map<String, Timer> datacenterResponseTimers = new ConcurrentHashMap<>();
    
    public AdvancedKafkaMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    public void recordPartitionMetrics(String topic, int partition, Duration latency, String datacenter) {
        String partitionKey = topic + "-" + partition;
        
        // Get or create partition latency timer
        Timer partitionTimer = partitionLatencyTimers.computeIfAbsent(partitionKey, key ->
            Timer.builder("kafka.partition.latency")
                .description("Per-partition latency")
                .tags("topic", topic, "partition", String.valueOf(partition), "datacenter", datacenter)
                .register(meterRegistry)
        );
        
        partitionTimer.record(latency);
        
        // Get or create partition throughput counter
        Counter partitionCounter = partitionThroughputCounters.computeIfAbsent(partitionKey, key ->
            Counter.builder("kafka.partition.throughput")
                .description("Per-partition throughput")
                .tags("topic", topic, "partition", String.valueOf(partition), "datacenter", datacenter)
                .register(meterRegistry)
        );
        
        partitionCounter.increment();
    }
    
    public void recordDatacenterHealth(String datacenter, boolean healthy) {
        Gauge healthGauge = datacenterHealthGauges.computeIfAbsent(datacenter, dc ->
            Gauge.builder("kafka.datacenter.health")
                .description("Datacenter health status")
                .tags("datacenter", datacenter)
                .register(meterRegistry, this, metrics -> healthy ? 1.0 : 0.0)
        );
    }
    
    public void recordDatacenterResponseTime(String datacenter, Duration responseTime) {
        Timer responseTimer = datacenterResponseTimers.computeIfAbsent(datacenter, dc ->
            Timer.builder("kafka.datacenter.response.time")
                .description("Datacenter response time")
                .tags("datacenter", datacenter)
                .register(meterRegistry)
        );
        
        responseTimer.record(responseTime);
    }
    
    @EventListener
    public void handlePartitionRebalance(PartitionRebalanceEvent event) {
        meterRegistry.counter("kafka.consumer.rebalance.events")
            .tags("type", event.getType().name(), "consumer.group", event.getConsumerGroup())
            .increment();
        
        logger.info("Partition rebalance event: type={}, group={}, partitions={}", 
                   event.getType(), event.getConsumerGroup(), event.getPartitions().size());
    }
}
```

## Health Indicators

### Spring Boot Actuator Integration

```java
@Component
@ConditionalOnClass(HealthIndicator.class)
public class KafkaMultiDatacenterHealthIndicator implements HealthIndicator {
    
    private final KafkaMultiDatacenterClient kafkaClient;
    private final Duration healthCheckTimeout;
    
    public KafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient kafkaClient) {
        this.kafkaClient = kafkaClient;
        this.healthCheckTimeout = Duration.ofSeconds(10);
    }
    
    @Override
    public Health health() {
        try {
            return performHealthCheck();
        } catch (Exception e) {
            return Health.down()
                .withDetail("error", e.getMessage())
                .withDetail("timestamp", Instant.now())
                .build();
        }
    }
    
    private Health performHealthCheck() {
        Health.Builder healthBuilder = Health.up();
        
        // Check datacenter connectivity
        Map<String, DatacenterHealthInfo> datacenterHealth = checkDatacenterHealth();
        healthBuilder.withDetail("datacenters", datacenterHealth);
        
        // Check connection pools
        ConnectionPoolHealthInfo poolHealth = checkConnectionPoolHealth();
        healthBuilder.withDetail("connectionPools", poolHealth);
        
        // Check schema registry
        SchemaRegistryHealthInfo schemaHealth = checkSchemaRegistryHealth();
        healthBuilder.withDetail("schemaRegistry", schemaHealth);
        
        // Check circuit breakers
        Map<String, CircuitBreakerHealthInfo> circuitBreakerHealth = checkCircuitBreakers();
        healthBuilder.withDetail("circuitBreakers", circuitBreakerHealth);
        
        // Overall health determination
        boolean allHealthy = datacenterHealth.values().stream().allMatch(DatacenterHealthInfo::isHealthy) &&
                           poolHealth.isHealthy() &&
                           schemaHealth.isHealthy() &&
                           circuitBreakerHealth.values().stream().allMatch(CircuitBreakerHealthInfo::isHealthy);
        
        return allHealthy ? healthBuilder.build() : healthBuilder.down().build();
    }
    
    private Map<String, DatacenterHealthInfo> checkDatacenterHealth() {
        return kafkaClient.getDatacenterEndpoints().stream()
            .collect(Collectors.toMap(
                KafkaDatacenterEndpoint::getId,
                this::checkSingleDatacenterHealth
            ));
    }
    
    private DatacenterHealthInfo checkSingleDatacenterHealth(KafkaDatacenterEndpoint endpoint) {
        try {
            long startTime = System.currentTimeMillis();
            boolean isConnected = kafkaClient.isDatacenterConnected(endpoint.getId());
            long responseTime = System.currentTimeMillis() - startTime;
            
            return DatacenterHealthInfo.builder()
                .healthy(isConnected)
                .responseTimeMs(responseTime)
                .lastChecked(Instant.now())
                .endpoint(endpoint.getBootstrapServers())
                .build();
                
        } catch (Exception e) {
            return DatacenterHealthInfo.builder()
                .healthy(false)
                .error(e.getMessage())
                .lastChecked(Instant.now())
                .endpoint(endpoint.getBootstrapServers())
                .build();
        }
    }
    
    private ConnectionPoolHealthInfo checkConnectionPoolHealth() {
        KafkaConnectionPoolManager poolManager = kafkaClient.getConnectionPoolManager();
        
        return ConnectionPoolHealthInfo.builder()
            .totalConnections(poolManager.getTotalConnections())
            .activeConnections(poolManager.getActiveConnections())
            .idleConnections(poolManager.getIdleConnections())
            .healthy(poolManager.isHealthy())
            .poolUtilization(poolManager.getPoolUtilization())
            .build();
    }
    
    private SchemaRegistryHealthInfo checkSchemaRegistryHealth() {
        try {
            SchemaRegistryClient schemaClient = kafkaClient.getSchemaRegistryClient();
            
            long startTime = System.currentTimeMillis();
            List<String> subjects = schemaClient.getAllSubjects();
            long responseTime = System.currentTimeMillis() - startTime;
            
            return SchemaRegistryHealthInfo.builder()
                .healthy(true)
                .responseTimeMs(responseTime)
                .subjectCount(subjects.size())
                .lastChecked(Instant.now())
                .build();
                
        } catch (Exception e) {
            return SchemaRegistryHealthInfo.builder()
                .healthy(false)
                .error(e.getMessage())
                .lastChecked(Instant.now())
                .build();
        }
    }
    
    private Map<String, CircuitBreakerHealthInfo> checkCircuitBreakers() {
        ResilienceManager resilienceManager = kafkaClient.getResilienceManager();
        
        return resilienceManager.getCircuitBreakers().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    CircuitBreaker cb = entry.getValue();
                    return CircuitBreakerHealthInfo.builder()
                        .state(cb.getState())
                        .failureRate(cb.getMetrics().getFailureRate())
                        .callsPermitted(cb.getState() != CircuitBreaker.State.OPEN)
                        .healthy(cb.getState() != CircuitBreaker.State.OPEN)
                        .build();
                }
            ));
    }
}
```

### Custom Health Indicators

```java
@Component
public class CustomKafkaHealthIndicators {
    
    @Bean
    public HealthIndicator consumerLagHealthIndicator(KafkaMultiDatacenterClient kafkaClient) {
        return () -> {
            try {
                // Note: This method may need to be implemented based on your consumer metrics
                // For now, using connection pool health as an example
                Map<String, Boolean> poolHealth = kafkaClient.getConnectionPoolHealth();
                boolean allHealthy = poolHealth.values().stream().allMatch(Boolean::booleanValue);
                
                if (!allHealthy) {
                    return Health.down()
                        .withDetail("connectionPoolHealth", poolHealth)
                        .withDetail("status", "Some connection pools are unhealthy")
                        .build();
                }
                
                return Health.up()
                    .withDetail("connectionPoolHealth", poolHealth)
                    .build();
                    
            } catch (Exception e) {
                return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
            }
        };
    }
    
    @Bean
    public HealthIndicator producerThroughputHealthIndicator(KafkaMetricsCollector metricsCollector) {
        return () -> {
            try {
                double currentThroughput = metricsCollector.getCurrentProducerThroughput();
                double expectedThroughput = 1000.0; // records per second
                
                if (currentThroughput < expectedThroughput * 0.1) { // 10% of expected
                    return Health.down()
                        .withDetail("currentThroughput", currentThroughput)
                        .withDetail("expectedThroughput", expectedThroughput)
                        .withDetail("status", "Producer throughput too low")
                        .build();
                }
                
                return Health.up()
                    .withDetail("currentThroughput", currentThroughput)
                    .withDetail("expectedThroughput", expectedThroughput)
                    .withDetail("utilizationPercent", (currentThroughput / expectedThroughput) * 100)
                    .build();
                    
            } catch (Exception e) {
                return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
            }
        };
    }
}
```

## Performance Monitoring

### Real-time Performance Metrics

```java
@Component
public class PerformanceMonitor {
    
    private final MeterRegistry meterRegistry;
    private final KafkaMultiDatacenterClient kafkaClient;
    private final PerformanceAnalyzer performanceAnalyzer;
    
    public PerformanceMonitor(MeterRegistry meterRegistry, KafkaMultiDatacenterClient kafkaClient) {
        this.meterRegistry = meterRegistry;
        this.kafkaClient = kafkaClient;
        this.performanceAnalyzer = new PerformanceAnalyzer(meterRegistry);
    }
    
    @Scheduled(fixedRate = 10000) // Every 10 seconds
    public void collectPerformanceMetrics() {
        collectProducerMetrics();
        collectConsumerMetrics();
        collectConnectionMetrics();
        analyzePerformanceTrends();
    }
    
    private void collectProducerMetrics() {
        ProducerMetrics metrics = kafkaClient.getProducerMetrics();
        
        // Throughput metrics
        meterRegistry.gauge("kafka.producer.throughput.records.per.second", 
                           metrics.getRecordSendRate());
        meterRegistry.gauge("kafka.producer.throughput.bytes.per.second", 
                           metrics.getBytesSendRate());
        
        // Latency metrics
        meterRegistry.gauge("kafka.producer.latency.avg", 
                           metrics.getAverageRecordSendLatency());
        meterRegistry.gauge("kafka.producer.latency.p95", 
                           metrics.getP95RecordSendLatency());
        meterRegistry.gauge("kafka.producer.latency.p99", 
                           metrics.getP99RecordSendLatency());
        
        // Error metrics
        meterRegistry.gauge("kafka.producer.error.rate", 
                           metrics.getRecordErrorRate());
        meterRegistry.gauge("kafka.producer.retry.rate", 
                           metrics.getRecordRetryRate());
        
        // Resource utilization
        meterRegistry.gauge("kafka.producer.buffer.utilization", 
                           metrics.getBufferUtilization());
        meterRegistry.gauge("kafka.producer.connection.count", 
                           metrics.getConnectionCount());
    }
    
    private void collectConsumerMetrics() {
        ConsumerMetrics metrics = kafkaClient.getConsumerMetrics();
        
        // Throughput metrics
        meterRegistry.gauge("kafka.consumer.throughput.records.per.second", 
                           metrics.getRecordsConsumedRate());
        meterRegistry.gauge("kafka.consumer.throughput.bytes.per.second", 
                           metrics.getBytesConsumedRate());
        
        // Latency metrics
        meterRegistry.gauge("kafka.consumer.latency.fetch.avg", 
                           metrics.getAverageFetchLatency());
        meterRegistry.gauge("kafka.consumer.latency.fetch.max", 
                           metrics.getMaxFetchLatency());
        
        // Lag metrics
        meterRegistry.gauge("kafka.consumer.lag.records", 
                           metrics.getConsumerLag());
        meterRegistry.gauge("kafka.consumer.lag.time.ms", 
                           metrics.getConsumerLagTimeMs());
        
        // Offset metrics
        meterRegistry.gauge("kafka.consumer.offset.commit.rate", 
                           metrics.getOffsetCommitRate());
        meterRegistry.gauge("kafka.consumer.offset.commit.latency.avg", 
                           metrics.getAverageOffsetCommitLatency());
    }
    
    private void collectConnectionMetrics() {
        KafkaConnectionMetrics metrics = kafkaClient.getConnectionMetrics();
        
        // Connection pool metrics
        meterRegistry.gauge("kafka.connection.pool.total", 
                           metrics.getTotalConnections());
        meterRegistry.gauge("kafka.connection.pool.active", 
                           metrics.getActiveConnections());
        meterRegistry.gauge("kafka.connection.pool.idle", 
                           metrics.getIdleConnections());
        meterRegistry.gauge("kafka.connection.pool.utilization", 
                           metrics.getPoolUtilization());
        
        // Per-datacenter connection metrics
        metrics.getDatacenterConnectionMetrics().forEach((datacenter, dcMetrics) -> {
            meterRegistry.gauge("kafka.connection.datacenter.count", 
                               Tags.of("datacenter", datacenter), dcMetrics.getConnectionCount());
            meterRegistry.gauge("kafka.connection.datacenter.latency.avg", 
                               Tags.of("datacenter", datacenter), dcMetrics.getAverageLatency());
        });
    }
    
    private void analyzePerformanceTrends() {
        PerformanceAnalysis analysis = performanceAnalyzer.analyze();
        
        if (analysis.hasPerformanceIssues()) {
            logger.warn("Performance issues detected: {}", analysis.getIssues());
            
            // Record performance alerts
            analysis.getIssues().forEach(issue -> {
                meterRegistry.counter("kafka.performance.alerts")
                    .tags("type", issue.getType(), "severity", issue.getSeverity().name())
                    .increment();
            });
        }
        
        // Record performance score
        meterRegistry.gauge("kafka.performance.score", analysis.getOverallScore());
    }
}
```

### Performance Analysis and Recommendations

```java
@Component
public class PerformanceAnalyzer {
    
    private final MeterRegistry meterRegistry;
    private final List<PerformanceRule> performanceRules;
    
    public PerformanceAnalyzer(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.performanceRules = initializePerformanceRules();
    }
    
    public PerformanceAnalysis analyze() {
        List<PerformanceIssue> issues = new ArrayList<>();
        Map<String, Double> metrics = collectCurrentMetrics();
        
        // Apply performance rules
        for (PerformanceRule rule : performanceRules) {
            Optional<PerformanceIssue> issue = rule.evaluate(metrics);
            issue.ifPresent(issues::add);
        }
        
        // Calculate overall performance score
        double overallScore = calculatePerformanceScore(metrics, issues);
        
        return PerformanceAnalysis.builder()
            .issues(issues)
            .overallScore(overallScore)
            .metrics(metrics)
            .recommendations(generateRecommendations(issues))
            .timestamp(Instant.now())
            .build();
    }
    
    private List<PerformanceRule> initializePerformanceRules() {
        return List.of(
            // High latency rule
            new PerformanceRule("high-producer-latency") {
                @Override
                public Optional<PerformanceIssue> evaluate(Map<String, Double> metrics) {
                    double avgLatency = metrics.getOrDefault("kafka.producer.latency.avg", 0.0);
                    if (avgLatency > 1000) { // 1 second
                        return Optional.of(PerformanceIssue.builder()
                            .type("high-latency")
                            .severity(Severity.HIGH)
                            .description("Producer average latency is " + avgLatency + "ms")
                            .recommendation("Check network connectivity and broker performance")
                            .build());
                    }
                    return Optional.empty();
                }
            },
            
            // Low throughput rule
            new PerformanceRule("low-throughput") {
                @Override
                public Optional<PerformanceIssue> evaluate(Map<String, Double> metrics) {
                    double throughput = metrics.getOrDefault("kafka.producer.throughput.records.per.second", 0.0);
                    if (throughput < 100) { // 100 records per second
                        return Optional.of(PerformanceIssue.builder()
                            .type("low-throughput")
                            .severity(Severity.MEDIUM)
                            .description("Producer throughput is " + throughput + " records/sec")
                            .recommendation("Consider increasing batch size or linger time")
                            .build());
                    }
                    return Optional.empty();
                }
            },
            
            // High error rate rule
            new PerformanceRule("high-error-rate") {
                @Override
                public Optional<PerformanceIssue> evaluate(Map<String, Double> metrics) {
                    double errorRate = metrics.getOrDefault("kafka.producer.error.rate", 0.0);
                    if (errorRate > 5.0) { // 5 errors per second
                        return Optional.of(PerformanceIssue.builder()
                            .type("high-error-rate")
                            .severity(Severity.HIGH)
                            .description("Producer error rate is " + errorRate + " errors/sec")
                            .recommendation("Check broker logs and network connectivity")
                            .build());
                    }
                    return Optional.empty();
                }
            },
            
            // High consumer lag rule
            new PerformanceRule("high-consumer-lag") {
                @Override
                public Optional<PerformanceIssue> evaluate(Map<String, Double> metrics) {
                    double consumerLag = metrics.getOrDefault("kafka.consumer.lag.records", 0.0);
                    if (consumerLag > 10000) { // 10,000 records
                        return Optional.of(PerformanceIssue.builder()
                            .type("high-consumer-lag")
                            .severity(Severity.HIGH)
                            .description("Consumer lag is " + consumerLag + " records")
                            .recommendation("Scale consumer instances or optimize processing logic")
                            .build());
                    }
                    return Optional.empty();
                }
            }
        );
    }
    
    private Map<String, Double> collectCurrentMetrics() {
        Map<String, Double> metrics = new HashMap<>();
        
        // Collect metrics from meter registry
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge) {
                Gauge gauge = (Gauge) meter;
                metrics.put(gauge.getId().getName(), gauge.value());
            } else if (meter instanceof Timer) {
                Timer timer = (Timer) meter;
                metrics.put(timer.getId().getName() + ".mean", timer.mean(TimeUnit.MILLISECONDS));
                metrics.put(timer.getId().getName() + ".max", timer.max(TimeUnit.MILLISECONDS));
            }
        });
        
        return metrics;
    }
    
    private double calculatePerformanceScore(Map<String, Double> metrics, List<PerformanceIssue> issues) {
        double baseScore = 100.0;
        
        // Deduct points for issues
        for (PerformanceIssue issue : issues) {
            switch (issue.getSeverity()) {
                case HIGH:
                    baseScore -= 20;
                    break;
                case MEDIUM:
                    baseScore -= 10;
                    break;
                case LOW:
                    baseScore -= 5;
                    break;
            }
        }
        
        return Math.max(0, baseScore);
    }
    
    private List<String> generateRecommendations(List<PerformanceIssue> issues) {
        return issues.stream()
            .map(PerformanceIssue::getRecommendation)
            .distinct()
            .collect(Collectors.toList());
    }
}
```

## Alerting and Notifications

### Alert Configuration

```java
@Configuration
public class AlertingConfiguration {
    
    @Bean
    public AlertManager alertManager(NotificationService notificationService) {
        return AlertManager.builder()
            .notificationService(notificationService)
            .alertRules(createAlertRules())
            .cooldownPeriod(Duration.ofMinutes(5))
            .build();
    }
    
    private List<AlertRule> createAlertRules() {
        return List.of(
            // High latency alert
            AlertRule.builder()
                .name("high-producer-latency")
                .condition("kafka.producer.latency.avg > 2000")
                .severity(AlertSeverity.HIGH)
                .message("Producer latency exceeded 2 seconds")
                .cooldown(Duration.ofMinutes(5))
                .build(),
                
            // Consumer lag alert
            AlertRule.builder()
                .name("high-consumer-lag")
                .condition("kafka.consumer.lag.records > 50000")
                .severity(AlertSeverity.CRITICAL)
                .message("Consumer lag exceeded 50,000 records")
                .cooldown(Duration.ofMinutes(2))
                .build(),
                
            // Error rate alert
            AlertRule.builder()
                .name("high-error-rate")
                .condition("kafka.producer.error.rate > 10")
                .severity(AlertSeverity.HIGH)
                .message("Producer error rate exceeded 10 errors/sec")
                .cooldown(Duration.ofMinutes(3))
                .build(),
                
            // Circuit breaker alert
            AlertRule.builder()
                .name("circuit-breaker-open")
                .condition("kafka.circuit.breaker.state == 'OPEN'")
                .severity(AlertSeverity.CRITICAL)
                .message("Circuit breaker is open")
                .cooldown(Duration.ofMinutes(1))
                .build(),
                
            // Datacenter connectivity alert
            AlertRule.builder()
                .name("datacenter-disconnected")
                .condition("kafka.datacenter.health == 0")
                .severity(AlertSeverity.HIGH)
                .message("Datacenter connection lost")
                .cooldown(Duration.ofMinutes(1))
                .build()
        );
    }
}
```

### Alert Processing and Notification

```java
@Component
public class KafkaAlertProcessor {
    
    private final AlertManager alertManager;
    private final NotificationService notificationService;
    private final MeterRegistry meterRegistry;
    
    public KafkaAlertProcessor(AlertManager alertManager, 
                              NotificationService notificationService,
                              MeterRegistry meterRegistry) {
        this.alertManager = alertManager;
        this.notificationService = notificationService;
        this.meterRegistry = meterRegistry;
    }
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void processAlerts() {
        try {
            List<Alert> activeAlerts = alertManager.checkAlerts();
            
            for (Alert alert : activeAlerts) {
                processAlert(alert);
            }
            
            // Update alert metrics
            meterRegistry.gauge("kafka.alerts.active.count", activeAlerts.size());
            
        } catch (Exception e) {
            logger.error("Failed to process alerts", e);
        }
    }
    
    private void processAlert(Alert alert) {
        logger.warn("Processing alert: {}", alert);
        
        // Send notification
        AlertNotification notification = AlertNotification.builder()
            .alertName(alert.getName())
            .severity(alert.getSeverity())
            .message(alert.getMessage())
            .timestamp(alert.getTimestamp())
            .metadata(alert.getMetadata())
            .build();
        
        try {
            notificationService.sendAlert(notification);
            
            // Record successful alert
            meterRegistry.counter("kafka.alerts.sent")
                .tags("alert.name", alert.getName(), "severity", alert.getSeverity().name())
                .increment();
                
        } catch (Exception e) {
            logger.error("Failed to send alert notification: {}", alert.getName(), e);
            
            // Record failed alert
            meterRegistry.counter("kafka.alerts.failed")
                .tags("alert.name", alert.getName())
                .increment();
        }
    }
    
    @EventListener
    public void handleKafkaEvent(KafkaEvent event) {
        switch (event.getType()) {
            case PRODUCER_ERROR:
                createProducerErrorAlert(event);
                break;
            case CONSUMER_LAG_HIGH:
                createConsumerLagAlert(event);
                break;
            case DATACENTER_DISCONNECTED:
                createDatacenterAlert(event);
                break;
            case CIRCUIT_BREAKER_OPENED:
                createCircuitBreakerAlert(event);
                break;
        }
    }
    
    private void createProducerErrorAlert(KafkaEvent event) {
        Alert alert = Alert.builder()
            .name("producer-error")
            .severity(AlertSeverity.HIGH)
            .message("Producer error: " + event.getMessage())
            .metadata(Map.of(
                "datacenter", event.getDatacenter(),
                "topic", event.getTopic(),
                "error", event.getError()
            ))
            .timestamp(Instant.now())
            .build();
        
        alertManager.addAlert(alert);
    }
    
    private void createConsumerLagAlert(KafkaEvent event) {
        Alert alert = Alert.builder()
            .name("consumer-lag")
            .severity(AlertSeverity.HIGH)
            .message("Consumer lag is high: " + event.getLag() + " records")
            .metadata(Map.of(
                "consumer.group", event.getConsumerGroup(),
                "topic", event.getTopic(),
                "lag", String.valueOf(event.getLag())
            ))
            .timestamp(Instant.now())
            .build();
        
        alertManager.addAlert(alert);
    }
}
```

## Distributed Tracing

### Jaeger Integration

```java
@Configuration
@ConditionalOnProperty(name = "tracing.jaeger.enabled", havingValue = "true")
public class JaegerTracingConfiguration {
    
    @Bean
    public JaegerTracer jaegerTracer(@Value("${spring.application.name}") String serviceName) {
        return Configuration.fromEnv(serviceName)
            .withSampler(Configuration.SamplerConfiguration.fromEnv()
                .withType(ConstSampler.TYPE)
                .withParam(1))
            .withReporter(Configuration.ReporterConfiguration.fromEnv()
                .withLogSpans(true))
            .getTracer();
    }
    
    @Bean
    public KafkaTracingInterceptor kafkaTracingInterceptor(Tracer tracer) {
        return new KafkaTracingInterceptor(tracer);
    }
}
```

### Kafka Operations Tracing

```java
@Component
public class TracedKafkaOperations {
    
    private final Tracer tracer;
    private final KafkaMultiDatacenterClient kafkaClient;
    
    public TracedKafkaOperations(Tracer tracer, KafkaMultiDatacenterClient kafkaClient) {
        this.tracer = tracer;
        this.kafkaClient = kafkaClient;
    }
    
    @Traced(operationName = "kafka-producer-send")
    public CompletableFuture<RecordMetadata> sendWithTracing(ProducerRecord<String, Object> record) {
        Span span = tracer.nextSpan()
            .name("kafka-producer-send")
            .tag("kafka.topic", record.topic())
            .tag("kafka.key", record.key())
            .tag("kafka.partition", String.valueOf(record.partition()))
            .start();
        
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return kafkaClient.producerAsync().sendAsync(record)
                .whenComplete((metadata, throwable) -> {
                    if (throwable != null) {
                        span.tag("error", true);
                        span.tag("error.message", throwable.getMessage());
                    } else {
                        span.tag("kafka.offset", String.valueOf(metadata.offset()));
                        span.tag("kafka.timestamp", String.valueOf(metadata.timestamp()));
                    }
                    span.end();
                });
        }
    }
    
    @Traced(operationName = "kafka-consumer-poll")
    public ConsumerRecords<String, Object> pollWithTracing(Duration timeout) {
        Span span = tracer.nextSpan()
            .name("kafka-consumer-poll")
            .tag("kafka.timeout.ms", String.valueOf(timeout.toMillis()))
            .start();
        
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            ConsumerRecords<String, Object> records = kafkaClient.consumerSync().poll(timeout);
            
            span.tag("kafka.records.count", String.valueOf(records.count()));
            span.tag("kafka.partitions.count", String.valueOf(records.partitions().size()));
            
            return records;
        } catch (Exception e) {
            span.tag("error", true);
            span.tag("error.message", e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    @Traced(operationName = "kafka-schema-registry-lookup")
    public Schema getSchemaWithTracing(String subject, int version) {
        Span span = tracer.nextSpan()
            .name("kafka-schema-registry-lookup")
            .tag("schema.subject", subject)
            .tag("schema.version", String.valueOf(version))
            .start();
        
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            Schema schema = kafkaClient.getSchemaRegistryClient().getBySubjectAndVersion(subject, version);
            
            span.tag("schema.id", String.valueOf(schema.hashCode()));
            span.tag("schema.type", schema.getType().name());
            
            return schema;
        } catch (Exception e) {
            span.tag("error", true);
            span.tag("error.message", e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
```

## Dashboard Configuration

### Grafana Dashboard Setup

```json
{
  "dashboard": {
    "title": "Kafka Multi-Datacenter Client Monitoring",
    "tags": ["kafka", "multi-datacenter"],
    "timezone": "UTC",
    "panels": [
      {
        "title": "Producer Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_producer_records_sent_total[5m])",
            "legendFormat": "Records/sec - {{datacenter}}"
          }
        ]
      },
      {
        "title": "Producer Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_producer_latency_avg",
            "legendFormat": "Avg Latency - {{datacenter}}"
          },
          {
            "expr": "kafka_producer_latency_p95",
            "legendFormat": "P95 Latency - {{datacenter}}"
          }
        ]
      },
      {
        "title": "Consumer Lag",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_consumer_lag_records",
            "legendFormat": "Lag - {{consumer_group}}"
          }
        ]
      },
      {
        "title": "Error Rates",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_errors_total[5m])",
            "legendFormat": "Errors/sec - {{error_type}}"
          }
        ]
      },
      {
        "title": "Datacenter Health",
        "type": "stat",
        "targets": [
          {
            "expr": "kafka_datacenter_health",
            "legendFormat": "{{datacenter}}"
          }
        ]
      },
      {
        "title": "Connection Pool Utilization",
        "type": "gauge",
        "targets": [
          {
            "expr": "kafka_connection_pool_utilization",
            "legendFormat": "Pool Utilization"
          }
        ]
      }
    ]
  }
}
```

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "kafka_alerts.yml"

scrape_configs:
  - job_name: 'kafka-multi-datacenter-client'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 10s

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:9093']

# kafka_alerts.yml
groups:
  - name: kafka_alerts
    rules:
      - alert: KafkaHighProducerLatency
        expr: kafka_producer_latency_avg > 2000
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High Kafka producer latency"
          description: "Producer latency is {{ $value }}ms"

      - alert: KafkaHighConsumerLag
        expr: kafka_consumer_lag_records > 50000
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "High Kafka consumer lag"
          description: "Consumer lag is {{ $value }} records"

      - alert: KafkaDatacenterDown
        expr: kafka_datacenter_health == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "Kafka datacenter is down"
          description: "Datacenter {{ $labels.datacenter }} is not healthy"
```

## Best Practices

### 1. **Metrics Collection Best Practices**

```java
// Use appropriate metric types
@Component
public class MetricsBestPractices {
    
    // Use counters for things that increase
    private final Counter recordsProcessed = Counter.builder("records.processed")
        .description("Total number of records processed")
        .register(meterRegistry);
    
    // Use gauges for current values
    private final Gauge activeConnections = Gauge.builder("connections.active")
        .description("Current number of active connections")
        .register(meterRegistry, this, MetricsBestPractices::getActiveConnections);
    
    // Use timers for duration measurements
    private final Timer processingTime = Timer.builder("processing.time")
        .description("Time taken to process records")
        .register(meterRegistry);
    
    // Use distribution summaries for size measurements
    private final DistributionSummary messageSize = DistributionSummary.builder("message.size")
        .description("Size of messages in bytes")
        .register(meterRegistry);
}
```

### 2. **Performance Monitoring Best Practices**

```java
// Monitor key performance indicators
@Scheduled(fixedRate = 60000)
public void monitorKPIs() {
    // Throughput KPIs
    double producerThroughput = getCurrentProducerThroughput();
    double consumerThroughput = getCurrentConsumerThroughput();
    
    // Latency KPIs
    double p95Latency = getP95Latency();
    double p99Latency = getP99Latency();
    
    // Availability KPIs
    double datacenterAvailability = getDatacenterAvailability();
    double overallAvailability = getOverallAvailability();
    
    // Log KPIs for trending
    logger.info("KPIs: producer_throughput={}, consumer_throughput={}, p95_latency={}, availability={}", 
               producerThroughput, consumerThroughput, p95Latency, overallAvailability);
}
```

### 3. **Alerting Best Practices**

```java
// Configure meaningful alerts with proper thresholds
AlertRule.builder()
    .name("kafka-producer-latency")
    .condition("kafka.producer.latency.p95 > 1000") // 1 second P95
    .for(Duration.ofMinutes(2)) // Sustained for 2 minutes
    .severity(AlertSeverity.HIGH)
    .message("Producer P95 latency exceeded 1 second")
    .cooldown(Duration.ofMinutes(5)) // Avoid alert spam
    .build();

// Use runbooks for alert handling
AlertRule.builder()
    .name("kafka-consumer-lag")
    .condition("kafka.consumer.lag.records > 100000")
    .severity(AlertSeverity.CRITICAL)
    .runbook("https://wiki.company.com/kafka-consumer-lag-runbook")
    .build();
```

### 4. **Dashboard Best Practices**

```java
// Organize dashboards by audience
// - Operations team: Health, alerts, resource utilization
// - Development team: Error rates, latency, throughput
// - Business stakeholders: High-level KPIs

// Use appropriate time ranges
// - Real-time monitoring: 15 minutes to 1 hour
// - Trend analysis: 24 hours to 7 days
// - Capacity planning: 30 days to 1 year

// Include contextual information
// - Version information
// - Deployment timestamps
// - Configuration changes
```

### 5. **Troubleshooting Best Practices**

```java
@Component
public class TroubleshootingAssistant {
    
    public TroubleshootingReport generateReport() {
        return TroubleshootingReport.builder()
            .timestamp(Instant.now())
            .systemHealth(checkSystemHealth())
            .recentErrors(getRecentErrors())
            .performanceMetrics(getPerformanceSnapshot())
            .configurationSummary(getConfigurationSummary())
            .recommendations(generateRecommendations())
            .build();
    }
    
    private List<String> generateRecommendations() {
        List<String> recommendations = new ArrayList<>();
        
        // Analyze current state and provide recommendations
        if (getConsumerLag() > 10000) {
            recommendations.add("Consider scaling consumer instances");
        }
        
        if (getProducerLatency() > 1000) {
            recommendations.add("Check network connectivity to brokers");
        }
        
        if (getErrorRate() > 1.0) {
            recommendations.add("Review error logs for common error patterns");
        }
        
        return recommendations;
    }
}
```

For more examples and detailed implementation, see the [examples directory](../lib/src/main/java/com/kafka/multidc/example/).
