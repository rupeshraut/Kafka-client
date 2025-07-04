package com.kafka.multidc.observability;

/**
 * Configuration for alert thresholds and conditions in the Kafka multi-datacenter client.
 * 
 * <p>This class defines alerting rules for monitoring metrics including:
 * <ul>
 *   <li>Threshold values and comparison conditions</li>
 *   <li>Time window specifications for aggregation</li>
 *   <li>Severity levels for prioritization</li>
 *   <li>Enable/disable flags for runtime control</li>
 * </ul>
 * 
 * <p>Alert configurations are immutable once created and use the builder pattern
 * for construction with sensible defaults.
 * 
 * @author Kafka Multi-DC Team
 * @version 1.0
 * @since 1.0
 */
public class AlertConfiguration {
    private final String metricName;
    private final double threshold;
    private final String condition; // "greater_than", "less_than", "equals"
    private final long windowSizeMs;
    private final String severity; // "low", "medium", "high", "critical"
    private final boolean enabled;
    
    private AlertConfiguration(Builder builder) {
        this.metricName = builder.metricName;
        this.threshold = builder.threshold;
        this.condition = builder.condition;
        this.windowSizeMs = builder.windowSizeMs;
        this.severity = builder.severity;
        this.enabled = builder.enabled;
    }
    
    /**
     * Returns the name of the metric to monitor.
     * 
     * @return the metric name
     */
    public String getMetricName() {
        return metricName;
    }
    
    /**
     * Returns the threshold value for triggering alerts.
     * 
     * @return the threshold value
     */
    public double getThreshold() {
        return threshold;
    }
    
    /**
     * Returns the condition for threshold comparison.
     * 
     * @return the condition string (e.g., "greater_than", "less_than", "equals")
     */
    public String getCondition() {
        return condition;
    }
    
    /**
     * Returns the time window size for metric aggregation in milliseconds.
     * 
     * @return the window size in milliseconds
     */
    public long getWindowSizeMs() {
        return windowSizeMs;
    }
    
    /**
     * Returns the severity level of the alert.
     * 
     * @return the severity level (e.g., "low", "medium", "high", "critical")
     */
    public String getSeverity() {
        return severity;
    }
    
    /**
     * Returns whether this alert configuration is enabled.
     * 
     * @return true if enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Creates a new builder for constructing alert configurations.
     * 
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder class for creating AlertConfiguration instances with fluent API.
     * 
     * <p>Provides default values for all optional parameters:
     * <ul>
     *   <li>condition: "greater_than"</li>
     *   <li>windowSizeMs: 60000 (1 minute)</li>
     *   <li>severity: "medium"</li>
     *   <li>enabled: true</li>
     * </ul>
     */
    public static class Builder {
        private String metricName;
        private double threshold;
        private String condition = "greater_than";
        private long windowSizeMs = 60000; // 1 minute default
        private String severity = "medium";
        private boolean enabled = true;
        
        /**
         * Sets the metric name to monitor.
         * 
         * @param metricName the name of the metric
         * @return this builder instance for method chaining
         */
        public Builder metricName(String metricName) {
            this.metricName = metricName;
            return this;
        }
        
        /**
         * Sets the threshold value for alert triggering.
         * 
         * @param threshold the threshold value
         * @return this builder instance for method chaining
         */
        public Builder threshold(double threshold) {
            this.threshold = threshold;
            return this;
        }
        
        /**
         * Sets the condition for threshold comparison.
         * 
         * @param condition the comparison condition
         * @return this builder instance for method chaining
         */
        public Builder condition(String condition) {
            this.condition = condition;
            return this;
        }
        
        /**
         * Sets the time window size for metric aggregation.
         * 
         * @param windowSizeMs the window size in milliseconds
         * @return this builder instance for method chaining
         */
        public Builder windowSizeMs(long windowSizeMs) {
            this.windowSizeMs = windowSizeMs;
            return this;
        }
        
        /**
         * Sets the severity level of the alert.
         * 
         * @param severity the severity level
         * @return this builder instance for method chaining
         */
        public Builder severity(String severity) {
            this.severity = severity;
            return this;
        }
        
        /**
         * Sets whether the alert is enabled.
         * 
         * @param enabled true to enable, false to disable
         * @return this builder instance for method chaining
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        /**
         * Builds the AlertConfiguration instance.
         * 
         * @return a new AlertConfiguration with the specified settings
         * @throws IllegalArgumentException if metric name is null or empty
         */
        public AlertConfiguration build() {
            if (metricName == null || metricName.trim().isEmpty()) {
                throw new IllegalArgumentException("Metric name is required");
            }
            return new AlertConfiguration(this);
        }
    }
}
