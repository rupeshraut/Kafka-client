package com.kafka.multidc.model;

/**
 * Datacenter preference for routing operations.
 */
public enum DatacenterPreference {
    
    /**
     * Prefer local datacenter only.
     */
    LOCAL_ONLY,
    
    /**
     * Prefer remote datacenters only.
     */
    REMOTE_ONLY,
    
    /**
     * Prefer local datacenter but fallback to remote.
     */
    LOCAL_PREFERRED,
    
    /**
     * Prefer remote datacenters but fallback to local.
     */
    REMOTE_PREFERRED,
    
    /**
     * Any available datacenter.
     */
    ANY_AVAILABLE,
    
    /**
     * All available datacenters (for broadcast operations).
     */
    ALL_AVAILABLE
}
