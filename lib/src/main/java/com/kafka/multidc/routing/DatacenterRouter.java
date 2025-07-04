package com.kafka.multidc.routing;

import com.kafka.multidc.config.KafkaDatacenterConfiguration;
import com.kafka.multidc.config.KafkaDatacenterEndpoint;
import com.kafka.multidc.model.DatacenterInfo;
import com.kafka.multidc.pool.KafkaConnectionPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Intelligent router for selecting appropriate datacenters based on various strategies.
 */
public class DatacenterRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(DatacenterRouter.class);
    
    private final KafkaDatacenterConfiguration configuration;
    private final KafkaConnectionPoolManager poolManager;
    private final Map<String, DatacenterInfo> datacenterInfoMap;
    private final Map<String, Instant> lastLatencyCheck;
    
    public DatacenterRouter(KafkaDatacenterConfiguration configuration, KafkaConnectionPoolManager poolManager) {
        this.configuration = configuration;
        this.poolManager = poolManager;
        this.datacenterInfoMap = new ConcurrentHashMap<>();
        this.lastLatencyCheck = new ConcurrentHashMap<>();
        
        initializeDatacenterInfo();
    }
    
    private void initializeDatacenterInfo() {
        for (KafkaDatacenterEndpoint endpoint : configuration.getDatacenters()) {
            DatacenterInfo info = new DatacenterInfo(
                endpoint.getId(),
                endpoint.getRegion(),
                endpoint.getBootstrapServers(),
                endpoint.getPriority(),
                true, // Initially assume healthy
                Duration.ofMillis(50), // Default latency
                Instant.now()
            );
            datacenterInfoMap.put(endpoint.getId(), info);
        }
    }
    
    /**
     * Select the best datacenter for the operation based on the configured routing strategy.
     */
    public String selectDatacenter() {
        return selectDatacenter(null);
    }
    
    /**
     * Select the best datacenter for the operation with a preferred datacenter hint.
     */
    public String selectDatacenter(String preferredDatacenter) {
        RoutingStrategy strategy = configuration.getRoutingStrategy();
        
        // Filter healthy datacenters
        List<DatacenterInfo> healthyDatacenters = getHealthyDatacenters();
        
        if (healthyDatacenters.isEmpty()) {
            logger.warn("No healthy datacenters available, falling back to any available datacenter");
            return configuration.getDatacenters().get(0).getId();
        }
        
        // If preferred datacenter is specified and healthy, use it
        if (preferredDatacenter != null) {
            Optional<DatacenterInfo> preferred = healthyDatacenters.stream()
                .filter(dc -> dc.getId().equals(preferredDatacenter))
                .findFirst();
            if (preferred.isPresent()) {
                return preferred.get().getId();
            }
        }
        
        // Apply routing strategy
        switch (strategy) {
            case WEIGHTED:
                return selectByPriority(healthyDatacenters);
            case LATENCY_BASED:
                return selectByLatency(healthyDatacenters);
            case ROUND_ROBIN:
                return selectRoundRobin(healthyDatacenters);
            case NEAREST:
                return selectRandom(healthyDatacenters);
            case PRIMARY_PREFERRED:
                return selectLocalFirst(healthyDatacenters);
            case CUSTOM:
                return selectCustom(healthyDatacenters);
            default:
                return selectByPriority(healthyDatacenters);
        }
    }
    
    private List<DatacenterInfo> getHealthyDatacenters() {
        return datacenterInfoMap.values().stream()
            .filter(dc -> poolManager.isHealthy(dc.getId()))
            .collect(Collectors.toList());
    }
    
    private String selectByPriority(List<DatacenterInfo> datacenters) {
        return datacenters.stream()
            .min(Comparator.comparingInt(DatacenterInfo::getPriority))
            .map(DatacenterInfo::getId)
            .orElse(datacenters.get(0).getId());
    }
    
    private String selectByLatency(List<DatacenterInfo> datacenters) {
        updateLatencyMetrics(datacenters);
        
        return datacenters.stream()
            .min(Comparator.comparing(DatacenterInfo::getLatency))
            .map(DatacenterInfo::getId)
            .orElse(datacenters.get(0).getId());
    }
    
    private String selectRoundRobin(List<DatacenterInfo> datacenters) {
        // Simple round robin based on current time
        int index = (int) (System.currentTimeMillis() % datacenters.size());
        return datacenters.get(index).getId();
    }
    
    private String selectRandom(List<DatacenterInfo> datacenters) {
        int randomIndex = ThreadLocalRandom.current().nextInt(datacenters.size());
        return datacenters.get(randomIndex).getId();
    }
    
    private String selectLocalFirst(List<DatacenterInfo> datacenters) {            String localDatacenter = configuration.getLocalDatacenterId();
        
        if (localDatacenter != null) {
            Optional<DatacenterInfo> local = datacenters.stream()
                .filter(dc -> dc.getId().equals(localDatacenter))
                .findFirst();
            if (local.isPresent()) {
                return local.get().getId();
            }
        }
        
        // Fallback to priority-based selection
        return selectByPriority(datacenters);
    }
    
    private String selectCustom(List<DatacenterInfo> datacenters) {
        // Custom routing logic can be implemented here
        // For now, fallback to priority-based selection
        logger.warn("Custom routing strategy not implemented, falling back to priority-based");
        return selectByPriority(datacenters);
    }
    
    private void updateLatencyMetrics(List<DatacenterInfo> datacenters) {
        Instant now = Instant.now();
        
        for (DatacenterInfo datacenter : datacenters) {
            Instant lastCheck = lastLatencyCheck.get(datacenter.getId());
            
            // Update latency every 30 seconds
            if (lastCheck == null || Duration.between(lastCheck, now).getSeconds() > 30) {
                Duration latency = measureLatency(datacenter.getId());
                datacenter.updateLatency(latency);
                lastLatencyCheck.put(datacenter.getId(), now);
            }
        }
    }
    
    private Duration measureLatency(String datacenterId) {
        // In a real implementation, this would ping the Kafka cluster
        // For now, simulate latency based on connection metrics
        var metrics = poolManager.getMetrics(datacenterId);
        if (metrics != null) {
            return metrics.getAverageLatency();
        }
        
        // Simulate latency based on datacenter priority (lower priority = higher latency)
        DatacenterInfo info = datacenterInfoMap.get(datacenterId);
        if (info != null) {
            return Duration.ofMillis(info.getPriority() * 10 + ThreadLocalRandom.current().nextInt(50));
        }
        
        return Duration.ofMillis(100); // Default latency
    }
    
    /**
     * Get all datacenter information.
     */
    public List<DatacenterInfo> getAllDatacenters() {
        return List.copyOf(datacenterInfoMap.values());
    }
    
    /**
     * Get information about a specific datacenter.
     */
    public DatacenterInfo getDatacenter(String datacenterId) {
        return datacenterInfoMap.get(datacenterId);
    }
    
    /**
     * Get the local (preferred) datacenter.
     */
    public DatacenterInfo getLocalDatacenter() {            String localDatacenterId = configuration.getLocalDatacenterId();
        return localDatacenterId != null ? datacenterInfoMap.get(localDatacenterId) : null;
    }
    
    /**
     * Update health status for a datacenter.
     */
    public void updateDatacenterHealth(String datacenterId, boolean healthy, Duration latency) {
        DatacenterInfo info = datacenterInfoMap.get(datacenterId);
        if (info != null) {
            info.updateHealth(healthy);
            if (latency != null) {
                info.updateLatency(latency);
            }
            info.updateLastHealthCheck(Instant.now());
        }
    }
    
    /**
     * Refresh all datacenter information.
     */
    public void refreshDatacenterInfo() {
        logger.info("Refreshing datacenter information");
        
        for (String datacenterId : datacenterInfoMap.keySet()) {
            boolean healthy = poolManager.isHealthy(datacenterId);
            Duration latency = measureLatency(datacenterId);
            updateDatacenterHealth(datacenterId, healthy, latency);
        }
    }
    
    /**
     * Check if any datacenter is available.
     */
    public boolean hasAvailableDatacenter() {
        return !getHealthyDatacenters().isEmpty();
    }
    
    /**
     * Get count of healthy datacenters.
     */
    public int getHealthyDatacenterCount() {
        return getHealthyDatacenters().size();
    }
    
    /**
     * Get count of total datacenters.
     */
    public int getTotalDatacenterCount() {
        return datacenterInfoMap.size();
    }
    
    public String selectFallbackDatacenter(String failedDatacenterId) {
        List<DatacenterInfo> healthyDatacenters = getHealthyDatacenters();
        
        // Remove the failed datacenter from the list
        healthyDatacenters.removeIf(dc -> dc.getId().equals(failedDatacenterId));
        
        if (healthyDatacenters.isEmpty()) {
            logger.warn("No healthy fallback datacenters available for failed datacenter: {}", failedDatacenterId);
            return null;
        }
        
        // Use same strategy as primary selection for fallback
        switch (configuration.getRoutingStrategy()) {
            case WEIGHTED:
                return selectByPriority(healthyDatacenters);
            case LATENCY_BASED:
                return selectByLatency(healthyDatacenters);
            case ROUND_ROBIN:
                return selectRoundRobin(healthyDatacenters);
            case PRIMARY_PREFERRED:
                return selectLocalFirst(healthyDatacenters);
            case NEAREST:
                return selectRandom(healthyDatacenters);
            case CUSTOM:
                return selectCustom(healthyDatacenters);
            default:
                return healthyDatacenters.get(0).getId(); // Default to first healthy
        }
    }
}
