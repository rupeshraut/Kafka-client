package com.kafka.multidc.observability;

/**
 * Metrics for observability operations and status.
 */
public interface ObservabilityMetrics {
    
    /**
     * Get the total number of metrics recorded.
     */
    long getMetricsRecorded();
    
    /**
     * Get the total number of errors recorded.
     */
    long getErrorsRecorded();
    
    /**
     * Get the total number of traces started.
     */
    long getTracesStarted();
    
    /**
     * Get the total number of traces that completed successfully.
     */
    long getTracesSucceeded();
    
    /**
     * Get the total number of traces that failed.
     */
    long getTracesFailed();
    
    /**
     * Get the total number of alerts configured.
     */
    long getAlertsConfigured();
    
    /**
     * Get the total number of alerts triggered.
     */
    long getAlertsTriggered();
    
    /**
     * Get the success rate of traces (successful traces / total completed traces).
     */
    double getTraceSuccessRate();
    
    /**
     * Get the number of currently active traces.
     */
    long getActiveTraces();
}
