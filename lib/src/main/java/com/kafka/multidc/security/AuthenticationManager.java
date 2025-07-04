/*
 * Copyright 2024 Kafka Multi-Datacenter Client
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kafka.multidc.security;

import com.kafka.multidc.config.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Authentication manager for Kafka Multi-Datacenter Client.
 * Manages authentication credentials and SSL contexts across multiple datacenters.
 */
public class AuthenticationManager {
    
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationManager.class);
    
    private final Map<String, SecurityConfig> datacenterSecurityConfigs;
    private final Map<String, SSLContext> sslContextCache;
    private final SSLContextFactory sslContextFactory;
    
    public AuthenticationManager(SSLContextFactory sslContextFactory) {
        this.datacenterSecurityConfigs = new ConcurrentHashMap<>();
        this.sslContextCache = new ConcurrentHashMap<>();
        this.sslContextFactory = sslContextFactory;
    }
    
    /**
     * Configure security for a specific datacenter.
     */
    public void configureDatacenterSecurity(String datacenterId, SecurityConfig securityConfig) {
        datacenterSecurityConfigs.put(datacenterId, securityConfig);
        
        // Pre-create SSL context if SSL is enabled
        if (securityConfig.isSslEnabled()) {
            try {
                SSLContext sslContext = createSSLContext(securityConfig);
                sslContextCache.put(datacenterId, sslContext);
                logger.info("SSL context created for datacenter: {}", datacenterId);
            } catch (Exception e) {
                logger.error("Failed to create SSL context for datacenter: {}", datacenterId, e);
            }
        }
    }
    
    /**
     * Get security configuration for a datacenter.
     */
    public SecurityConfig getSecurityConfig(String datacenterId) {
        return datacenterSecurityConfigs.get(datacenterId);
    }
    
    /**
     * Get SSL context for a datacenter.
     */
    public SSLContext getSSLContext(String datacenterId) {
        SecurityConfig config = datacenterSecurityConfigs.get(datacenterId);
        if (config == null || !config.isSslEnabled()) {
            return null;
        }
        
        // Return cached context if available
        SSLContext cached = sslContextCache.get(datacenterId);
        if (cached != null) {
            return cached;
        }
        
        // Create new context if not cached
        try {
            SSLContext sslContext = createSSLContext(config);
            sslContextCache.put(datacenterId, sslContext);
            return sslContext;
        } catch (Exception e) {
            logger.error("Failed to create SSL context for datacenter: {}", datacenterId, e);
            return null;
        }
    }
    
    /**
     * Configure producer properties with security settings.
     */
    public Properties configureProducerSecurity(String datacenterId, Properties baseProperties) {
        SecurityConfig config = datacenterSecurityConfigs.get(datacenterId);
        if (config == null) {
            return baseProperties;
        }
        
        Properties secureProperties = new Properties();
        secureProperties.putAll(baseProperties);
        secureProperties.putAll(config.toKafkaProperties());
        
        logger.debug("Configured producer security for datacenter: {}", datacenterId);
        return secureProperties;
    }
    
    /**
     * Configure consumer properties with security settings.
     */
    public Properties configureConsumerSecurity(String datacenterId, Properties baseProperties) {
        SecurityConfig config = datacenterSecurityConfigs.get(datacenterId);
        if (config == null) {
            return baseProperties;
        }
        
        Properties secureProperties = new Properties();
        secureProperties.putAll(baseProperties);
        secureProperties.putAll(config.toKafkaProperties());
        
        logger.debug("Configured consumer security for datacenter: {}", datacenterId);
        return secureProperties;
    }
    
    /**
     * Validate authentication for a datacenter.
     */
    public boolean validateAuthentication(String datacenterId) {
        SecurityConfig config = datacenterSecurityConfigs.get(datacenterId);
        if (config == null) {
            return true; // No security configured, assume valid
        }
        
        try {
            // Validate SSL configuration
            if (config.isSslEnabled()) {
                SSLContext sslContext = getSSLContext(datacenterId);
                if (sslContext == null) {
                    logger.warn("SSL enabled but no valid SSL context for datacenter: {}", datacenterId);
                    return false;
                }
            }
            
            // Validate SASL configuration
            if (config.getSaslMechanism() != null) {
                if (config.getSaslJaasConfig() == null || config.getSaslJaasConfig().trim().isEmpty()) {
                    logger.warn("SASL mechanism configured but no JAAS config for datacenter: {}", datacenterId);
                    return false;
                }
            }
            
            logger.debug("Authentication validation successful for datacenter: {}", datacenterId);
            return true;
            
        } catch (Exception e) {
            logger.error("Authentication validation failed for datacenter: {}", datacenterId, e);
            return false;
        }
    }
    
    /**
     * Refresh authentication credentials for a datacenter.
     */
    public void refreshCredentials(String datacenterId) {
        SecurityConfig config = datacenterSecurityConfigs.get(datacenterId);
        if (config == null) {
            return;
        }
        
        // Clear cached SSL context to force recreation
        sslContextCache.remove(datacenterId);
        
        // Recreate SSL context if SSL is enabled
        if (config.isSslEnabled()) {
            try {
                SSLContext sslContext = createSSLContext(config);
                sslContextCache.put(datacenterId, sslContext);
                logger.info("Refreshed SSL context for datacenter: {}", datacenterId);
            } catch (Exception e) {
                logger.error("Failed to refresh SSL context for datacenter: {}", datacenterId, e);
            }
        }
    }
    
    /**
     * Get authentication status for all datacenters.
     */
    public Map<String, Boolean> getAuthenticationStatus() {
        Map<String, Boolean> status = new ConcurrentHashMap<>();
        for (String datacenterId : datacenterSecurityConfigs.keySet()) {
            status.put(datacenterId, validateAuthentication(datacenterId));
        }
        return status;
    }
    
    /**
     * Close and cleanup resources.
     */
    public void close() {
        sslContextCache.clear();
        datacenterSecurityConfigs.clear();
        logger.info("Authentication manager closed");
    }
    
    private SSLContext createSSLContext(SecurityConfig config) throws Exception {
        if (config.getSslContext() != null) {
            return config.getSslContext();
        }
        
        return SSLContextFactory.createSSLContext(
            config.getKeystorePath(),
            config.getKeystorePassword(),
            "JKS", // keystoreType
            config.getTruststorePath(),
            config.getTruststorePassword(),
            "JKS", // truststoreType
            config.getSslProtocol()
        );
    }
}
