<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# Kafka Multi-Datacenter Client Library

This is an enterprise-grade Kafka producer and consumer library designed for multi-datacenter deployments with comprehensive resilience patterns and observability features.

## Code Generation Guidelines

### Architecture Patterns
- Use builder patterns for configuration objects
- Implement factory patterns for client creation
- Apply strategy patterns for routing and serialization
- Follow dependency injection patterns for testability

### Programming Models
- Support synchronous, asynchronous (CompletableFuture), and reactive (Project Reactor) programming models
- Ensure all three models provide consistent feature sets
- Use appropriate abstractions for each programming model

### Error Handling
- Implement comprehensive error handling with specific exception types
- Use Resilience4j patterns (circuit breaker, retry, rate limiter, bulkhead, time limiter)
- Provide meaningful error messages and recovery suggestions
- Support automatic retry with exponential backoff

### Configuration
- Use immutable configuration objects with builder patterns
- Support environment-specific configuration presets
- Implement validation for all configuration parameters
- Support runtime configuration updates where appropriate

### Observability
- Integrate Micrometer for metrics collection
- Provide comprehensive logging with structured output
- Support health checks and status monitoring
- Implement distributed tracing integration points

### Testing
- Write comprehensive unit tests with high coverage
- Include integration tests with TestContainers
- Implement performance and load testing capabilities
- Use meaningful test names and clear assertions

### Security
- Support SSL/TLS encryption for all connections
- Implement SASL authentication mechanisms
- Provide secure credential management
- Support certificate-based authentication

### Performance
- Implement efficient connection pooling
- Support batching and compression optimizations
- Provide backpressure handling mechanisms
- Optimize for high-throughput scenarios

### Multi-Datacenter Features
- Support intelligent routing between datacenters
- Implement health-aware failover mechanisms
- Provide data locality and preference controls
- Support cross-datacenter coordination patterns
