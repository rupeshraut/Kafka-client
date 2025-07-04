# Spring Boot Actuator Health Check Integration

The Kafka Multi-Datacenter Client provides comprehensive health check integration with Spring Boot Actuator for enterprise-grade monitoring and observability.

## Overview

The health indicator provides detailed health information about:
- Datacenter connectivity status
- Connection pool health across all datacenters
- Schema registry availability
- Configuration validation
- Performance metrics

## Quick Start

### Standalone Usage (No Spring Boot)

```java
// Create health indicator
KafkaMultiDatacenterHealthIndicator healthIndicator = 
    new KafkaMultiDatacenterHealthIndicator(client);

// Perform health check
KafkaMultiDatacenterHealthIndicator.HealthStatus health = healthIndicator.health();

// Check status
if (health.isHealthy()) {
    System.out.println("Client is healthy");
} else {
    System.out.println("Client has issues: " + health.getStatus());
}

// Get detailed information
Map<String, Object> details = health.getDetails();
```

### Spring Boot Integration

Add the Spring Boot Actuator dependency:

```gradle
implementation 'org.springframework.boot:spring-boot-starter-actuator'
```

Configure the health indicator:

```java
@Configuration
@ConditionalOnClass(HealthIndicator.class)
public class KafkaHealthConfiguration {
    
    @Bean
    @ConditionalOnBean(KafkaMultiDatacenterClient.class)
    @ConditionalOnProperty(name = "management.health.kafka-multidc.enabled", matchIfMissing = true)
    public HealthIndicator kafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client) {
        return (HealthIndicator) KafkaMultiDatacenterHealthAutoConfiguration
                .createHealthIndicator(client);
    }
}
```

## Configuration

### Application Properties

```properties
# Enable/disable health indicator
management.health.kafka-multidc.enabled=true

# Health check timeout
management.health.kafka-multidc.timeout=5s

# Include detailed checks
management.health.kafka-multidc.detailed-checks=true

# Expose health endpoint
management.endpoints.web.exposure.include=health
management.endpoint.health.show-details=always
```

### Programmatic Configuration

```java
KafkaMultiDatacenterHealthIndicator healthIndicator = 
    new KafkaMultiDatacenterHealthIndicator(
        client,
        Duration.ofSeconds(10),  // Custom timeout
        true                     // Enable detailed checks
    );
```

## Health Check Details

### Response Structure

The health indicator returns a detailed health status with the following structure:

```json
{
  "status": "UP",
  "details": {
    "client.status": "HEALTHY",
    "timestamp": "2024-07-03T10:15:30Z",
    
    "datacenters": {
      "datacenter-1": {
        "healthy": true,
        "endpoint": "localhost:9092",
        "priority": 1,
        "region": "us-east-1",
        "latency.ms": 45,
        "lastHealthCheck": "2024-07-03T10:15:29Z"
      },
      "datacenter-2": {
        "healthy": false,
        "endpoint": "localhost:9093",
        "priority": 2,
        "region": "us-west-2",
        "latency.ms": 1500,
        "lastHealthCheck": "2024-07-03T10:10:15Z"
      }
    },
    "datacenters.healthy": 1,
    "datacenters.total": 2,
    
    "connection.pools": {
      "datacenter-1": {
        "active.connections": 5,
        "created.connections": 10,
        "closed.connections": 5,
        "connection.errors": 0,
        "request.count": 1000,
        "error.count": 5,
        "error.rate": 0.005,
        "average.latency.ms": 45,
        "healthy": true
      }
    },
    "connection.pools.all.healthy": true,
    "connection.pools.total.active": 5,
    "connection.pools.total.errors": 0,
    "connection.pools.total.requests": 1000,
    "connection.pools.overall.error.rate": 0.005,
    
    "schema.registry": {
      "datacenter-1": true,
      "datacenter-2": false
    },
    "schema.registry.healthy.count": 1,
    "schema.registry.total.count": 2,
    
    "configuration": {
      "fallback.enabled": true,
      "dead.letter.enabled": true,
      "client.closed": false
    },
    
    "local.datacenter": {
      "id": "datacenter-1",
      "region": "us-east-1",
      "healthy": true
    }
  }
}
```

### Health Status Values

- **UP**: All systems operational
- **DOWN**: Critical systems failed
- **OUT_OF_SERVICE**: Client is intentionally disabled
- **UNKNOWN**: Unable to determine health status

## Health Check Logic

The health indicator considers the client healthy when:

1. **At least one datacenter is healthy** - The client can route traffic
2. **Connection pools are functional** - Not exhausted and accepting connections
3. **No critical errors** - Exceptions don't prevent basic operations

The client is considered unhealthy when:

1. **No datacenters are reachable** - Cannot route any traffic
2. **All connection pools are exhausted** - Cannot create new connections
3. **Critical configuration errors** - Invalid or missing required configuration

### Optional Components

These components are checked but don't affect overall health:
- **Schema Registry** - Optional service, health check can timeout without failing
- **Dead Letter Queue** - Optional feature, not required for basic operation

## Monitoring Integration

### Periodic Health Monitoring

```java
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

scheduler.scheduleAtFixedRate(() -> {
    KafkaMultiDatacenterHealthIndicator.HealthStatus health = healthIndicator.health();
    
    if (!health.isHealthy()) {
        // Send alert to monitoring system
        alertingService.sendAlert("Kafka client unhealthy: " + health.getStatus());
        
        // Log detailed information
        logger.warn("Health check failed: {}", health.getDetails());
    }
}, 0, 30, TimeUnit.SECONDS);
```

### Metrics Integration

The health indicator automatically integrates with Micrometer metrics:

```java
// Custom metrics based on health status
meterRegistry.gauge("kafka.multidc.health.score", health.isHealthy() ? 1.0 : 0.0);
meterRegistry.gauge("kafka.multidc.datacenters.healthy", health.getDetails().get("datacenters.healthy"));
```

### Alerting Rules

Example Prometheus alerting rules:

```yaml
groups:
  - name: kafka-multidc
    rules:
      - alert: KafkaMultiDCUnhealthy
        expr: kafka_multidc_health_score == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka Multi-DC client is unhealthy"
          
      - alert: KafkaMultiDCNoHealthyDatacenters
        expr: kafka_multidc_datacenters_healthy == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "No healthy Kafka datacenters available"
          
      - alert: KafkaMultiDCConnectionPoolExhausted
        expr: kafka_multidc_connection_pools_all_healthy == 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Kafka connection pools are exhausted"
```

## Troubleshooting

### Common Issues

1. **Health check timeouts**
   - Increase timeout in configuration
   - Check network connectivity to Kafka brokers
   - Review connection pool settings

2. **Schema registry failures**
   - Verify schema registry URLs in configuration
   - Check authentication credentials
   - Consider disabling schema registry if not needed

3. **Connection pool issues**
   - Monitor connection metrics
   - Adjust pool size settings
   - Check for connection leaks

### Debug Logging

Enable debug logging to troubleshoot health check issues:

```properties
logging.level.com.kafka.multidc.actuator=DEBUG
logging.level.com.kafka.multidc.pool=DEBUG
logging.level.com.kafka.multidc.resilience=DEBUG
```

### Health Check Customization

Create a custom health indicator for specific requirements:

```java
@Component
public class CustomKafkaHealthIndicator extends KafkaMultiDatacenterHealthIndicator {
    
    public CustomKafkaHealthIndicator(KafkaMultiDatacenterClient client) {
        super(client, Duration.ofSeconds(3), false);
    }
    
    @Override
    public HealthStatus health() {
        HealthStatus baseHealth = super.health();
        
        // Add custom checks
        Map<String, Object> details = new HashMap<>(baseHealth.getDetails());
        details.put("custom.check", performCustomHealthCheck());
        
        return baseHealth.isHealthy() && isCustomCheckHealthy()
                ? HealthStatus.up().withDetails(details).build()
                : HealthStatus.down().withDetails(details).build();
    }
    
    private boolean performCustomHealthCheck() {
        // Implement custom health logic
        return true;
    }
}
```

## Best Practices

1. **Configure appropriate timeouts** - Balance responsiveness with reliability
2. **Monitor health check performance** - Ensure checks don't impact application performance
3. **Use detailed checks selectively** - Enable only when needed to reduce overhead
4. **Implement proper alerting** - Set up monitoring based on health status
5. **Test failure scenarios** - Verify health checks work during outages
6. **Document custom health logic** - Maintain clear documentation for custom health indicators

## API Reference

### KafkaMultiDatacenterHealthIndicator

Main health indicator class providing comprehensive health checks.

**Constructor**:
- `KafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client)`
- `KafkaMultiDatacenterHealthIndicator(KafkaMultiDatacenterClient client, Duration timeout, boolean detailed)`

**Methods**:
- `HealthStatus health()` - Perform health check and return status

### HealthStatus

Health status result with details and exception information.

**Methods**:
- `Status getStatus()` - Get health status enum
- `Map<String, Object> getDetails()` - Get detailed health information
- `Exception getException()` - Get exception if health check failed
- `boolean isHealthy()` - Check if status indicates healthy state

### Spring Boot Integration

**KafkaMultiDatacenterHealthAutoConfiguration**:
- `createHealthIndicator(KafkaMultiDatacenterClient client)` - Create Spring Boot health indicator
- `createHealthIndicator(client, timeout, detailedChecks)` - Create with custom configuration
