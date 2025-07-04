package com.kafka.multidc.routing;

/**
 * Routing strategies for multi-datacenter Kafka operations.
 */
public enum RoutingStrategy {
    
    /**
     * Route to the nearest datacenter based on network latency.
     */
    LATENCY_BASED,
    
    /**
     * Route to the geographically nearest datacenter.
     */
    NEAREST,
    
    /**
     * Round-robin routing across healthy datacenters.
     */
    ROUND_ROBIN,
    
    /**
     * Route to primary datacenter first, fallback to others.
     */
    PRIMARY_PREFERRED,
    
    /**
     * Weighted routing based on datacenter priorities.
     */
    WEIGHTED,
    
    /**
     * Route only to healthy datacenters.
     */
    HEALTH_AWARE,
    
    /**
     * Custom routing logic provided by user.
     */
    CUSTOM,
    
    /**
     * Explicit failover-only routing.
     */
    FAILOVER_ONLY
}
