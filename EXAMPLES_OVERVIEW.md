# Kafka Multi-Datacenter Client - Usage Examples

This document provides an overview of the comprehensive, idiomatic Java usage examples for the Kafka Multi-Datacenter Client library. Each example demonstrates real-world enterprise scenarios and best practices.

## Overview

The examples demonstrate all major enterprise features of the Kafka Multi-Datacenter Client:
- **Security**: SSL/TLS encryption, SASL authentication, secure producer operations
- **Resilience**: Health-aware routing, failover mechanisms, circuit breaker patterns, retry logic
- **Schema Registry**: Avro/JSON schema management, evolution, multi-datacenter coordination
- **Observability**: Health monitoring, metrics collection, alerting, performance tracking
- **Reactive Programming**: Reactive producer/consumer patterns, backpressure handling
- **Consumer Patterns**: Sync, async, batch processing with real-world scenarios
- **Dead Letter Queue**: Error handling and message routing for failed processing
- **Connection Management**: Pool monitoring, maintenance, and optimization

## Example Files

### 1. BasicUsageExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/BasicUsageExample.java`

**Purpose**: Demonstrates fundamental usage patterns
- Basic client configuration and setup
- Simple producer and consumer operations
- Synchronous and asynchronous programming models
- Basic error handling patterns

**Key Features**:
- Multi-datacenter configuration
- Producer and consumer operations
- Error handling and logging

### 2. AdvancedSerializationExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/AdvancedSerializationExample.java`

**Purpose**: Comprehensive serialization strategies and data formats
- JSON, Avro, and Protobuf serialization patterns
- Multi-format compression (GZIP, Snappy, LZ4, ZSTD)
- AES-128 and AES-256 encryption for sensitive data
- Schema evolution and compatibility management
- Per-datacenter serialization configuration
- Compliance-based serialization strategies

**Key Features**:
- Multi-format serialization (JSON, Avro simulation)
- 4 compression algorithms with performance characteristics
- Enterprise-grade encryption (AES-128/256)
- Schema evolution (backward/forward compatibility)
- Regional compliance requirements (GDPR, PCI-DSS)
- Custom serializers and deserializers

### 3. SecurityExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/SecurityExample.java`

**Purpose**: Comprehensive security configuration and secure operations
- SSL/TLS encryption setup with certificates
- SASL authentication (PLAIN, SCRAM-SHA-256, GSSAPI)
- Secure producer operations with encryption
- Certificate-based authentication

**Key Features**:
- Complete security configuration
- Multiple authentication mechanisms
- Secure message production
- Certificate management examples

### 4. ResilienceExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/ResilienceExample.java`

**Purpose**: Demonstrates resilience patterns and failure handling
- Health-aware routing between datacenters
- Automatic failover mechanisms
- Circuit breaker patterns with Resilience4j
- Retry logic with exponential backoff
- Bulkhead pattern for resource isolation

**Key Features**:
- Health monitoring and failover
- Circuit breaker configuration
- Retry patterns with backoff
- Resource isolation strategies
- Performance under failure scenarios

### 4. SchemaRegistryExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/SchemaRegistryExample.java`

**Purpose**: Schema management and evolution across datacenters
- Avro schema registration and management
- JSON schema handling
- Schema evolution patterns
- Multi-datacenter schema registry coordination
- Version compatibility management

**Key Features**:
- Schema registration and retrieval
- Schema evolution demonstration
- Multi-format support (Avro, JSON)
- Cross-datacenter schema synchronization
- Compatibility checking

### 5. ObservabilityExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/ObservabilityExample.java`

**Purpose**: Comprehensive monitoring and observability
- Health monitoring with alerts
- Metrics collection and aggregation
- Connection pool monitoring
- Performance tracking
- Alerting on thresholds
- Structured logging with context

**Key Features**:
- Real-time health monitoring
- Detailed metrics collection
- Connection pool analytics
- Performance alerting
- Comprehensive logging strategies

### 6. ReactiveExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/ReactiveExample.java`

**Purpose**: Reactive programming patterns with Project Reactor
- Reactive producer with backpressure
- Reactive consumer with flow control
- Stream processing patterns
- Error handling in reactive streams
- Resource management in reactive contexts

**Key Features**:
- Reactive producer operations
- Backpressure handling
- Stream-based processing
- Non-blocking operations
- Reactive error handling

### 7. ConsumerExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/ConsumerExample.java`

**Purpose**: Comprehensive consumer patterns and best practices
- Synchronous consumer operations
- Asynchronous consumer with callbacks
- Batch processing patterns
- Manual commit strategies
- Consumer group management
- Offset management and recovery

**Key Features**:
- Multiple consumer patterns
- Batch processing optimization
- Manual offset management
- Error handling and recovery
- Performance monitoring
- Consumer group coordination

### 8. DeadLetterQueueExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/DeadLetterQueueExample.java`

**Purpose**: Error handling and message routing for processing failures
- Dead letter queue configuration
- Failed message routing
- Error classification and handling
- Retry mechanisms before DLQ routing
- Message recovery patterns

**Key Features**:
- DLQ configuration and setup
- Automatic error routing
- Message recovery strategies
- Error analysis and logging

### 9. HealthIndicatorExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/HealthIndicatorExample.java`

**Purpose**: Health check implementation and monitoring
- Custom health indicators
- Datacenter health assessment
- Health status aggregation
- Integration with monitoring systems

**Key Features**:
- Health check implementation
- Status aggregation
- Monitoring integration
- Alerting on health changes

### 10. KafkaMultiDatacenterExample.java
**Location**: `lib/src/main/java/com/kafka/multidc/example/KafkaMultiDatacenterExample.java`

**Purpose**: General multi-datacenter usage demonstration
- Multi-datacenter configuration
- Routing strategies
- Basic producer/consumer operations
- Configuration best practices

**Key Features**:
- Multi-datacenter setup
- Routing configuration
- Basic operations
- Configuration examples

## Usage Patterns Demonstrated

### Configuration Patterns
- **Builder Pattern**: All configuration objects use builder patterns for immutability
- **Environment-Specific**: Examples show different configurations for various environments
- **Validation**: Configuration validation and error handling
- **Security Integration**: Secure configuration management

### Programming Models
- **Synchronous**: Blocking operations for simple use cases
- **Asynchronous**: CompletableFuture-based operations for performance
- **Reactive**: Project Reactor-based streams for high-throughput scenarios

### Error Handling
- **Comprehensive**: Multiple levels of error handling and recovery
- **Specific Exceptions**: Proper exception types and handling strategies
- **Resilience Patterns**: Circuit breaker, retry, bulkhead, and timeout patterns
- **Dead Letter Queues**: Failed message routing and recovery

### Observability
- **Metrics**: Comprehensive metrics collection with Micrometer
- **Logging**: Structured logging with correlation IDs and context
- **Health Checks**: Real-time health monitoring and alerting
- **Tracing**: Distributed tracing integration points

### Performance Optimization
- **Connection Pooling**: Efficient connection management
- **Batching**: Batch processing for high throughput
- **Compression**: Message compression configuration
- **Backpressure**: Flow control in reactive scenarios

## Running the Examples

Each example can be run independently and includes:
- Complete configuration setup
- Mock data generation where needed
- Comprehensive logging output
- Error simulation and handling demonstration

To run an example:
```bash
# Build the project
./gradlew build

# Run a specific example (replace with actual main class)
./gradlew run -PmainClass=com.kafka.multidc.example.SecurityExample
```

## Best Practices Demonstrated

1. **Configuration Management**: Immutable configurations with validation
2. **Resource Management**: Proper cleanup and connection management
3. **Error Handling**: Comprehensive error handling with recovery strategies
4. **Monitoring**: Extensive observability and health monitoring
5. **Security**: Complete security setup with multiple authentication methods
6. **Performance**: Optimization techniques for high-throughput scenarios
7. **Resilience**: Enterprise-grade resilience patterns
8. **Testing**: Examples include integration and performance testing patterns

## Integration Examples

The examples demonstrate integration with:
- **Micrometer**: For metrics collection
- **Resilience4j**: For resilience patterns
- **Project Reactor**: For reactive programming
- **SLF4J**: For structured logging
- **Avro/JSON Schema Registry**: For schema management
- **SSL/TLS**: For secure communications
- **SASL**: For authentication

These examples provide a comprehensive foundation for implementing the Kafka Multi-Datacenter Client in production environments with enterprise-grade requirements.
