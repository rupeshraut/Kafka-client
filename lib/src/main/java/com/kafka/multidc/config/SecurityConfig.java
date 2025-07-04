package com.kafka.multidc.config;

import javax.net.ssl.SSLContext;
import java.util.Map;
import java.util.Properties;

/**
 * Security configuration for Kafka Multi-Datacenter Client.
 * Provides comprehensive security settings including SSL/TLS, SASL authentication,
 * and credential management across multiple datacenters.
 */
public class SecurityConfig {
    
    private final boolean sslEnabled;
    private final SSLContext sslContext;
    private final String keystorePath;
    private final String keystorePassword;
    private final String truststorePath;
    private final String truststorePassword;
    private final String keyPassword;
    private final SaslMechanism saslMechanism;
    private final String saslUsername;
    private final String saslPassword;
    private final String saslJaasConfig;
    private final String securityProtocol;
    private final Map<String, String> additionalProperties;
    private final boolean clientAuthRequired;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslProtocol;
    private final String sslProvider;
    private final String sslCipherSuites;
    private final String sslEnabledProtocols;
    
    private SecurityConfig(Builder builder) {
        this.sslEnabled = builder.sslEnabled;
        this.sslContext = builder.sslContext;
        this.keystorePath = builder.keystorePath;
        this.keystorePassword = builder.keystorePassword;
        this.truststorePath = builder.truststorePath;
        this.truststorePassword = builder.truststorePassword;
        this.keyPassword = builder.keyPassword;
        this.saslMechanism = builder.saslMechanism;
        this.saslUsername = builder.saslUsername;
        this.saslPassword = builder.saslPassword;
        this.saslJaasConfig = builder.saslJaasConfig;
        this.securityProtocol = builder.securityProtocol;
        this.additionalProperties = Map.copyOf(builder.additionalProperties);
        this.clientAuthRequired = builder.clientAuthRequired;
        this.sslEndpointIdentificationAlgorithm = builder.sslEndpointIdentificationAlgorithm;
        this.sslProtocol = builder.sslProtocol;
        this.sslProvider = builder.sslProvider;
        this.sslCipherSuites = builder.sslCipherSuites;
        this.sslEnabledProtocols = builder.sslEnabledProtocols;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public boolean isSslEnabled() { return sslEnabled; }
    public SSLContext getSslContext() { return sslContext; }
    public String getKeystorePath() { return keystorePath; }
    public String getKeystorePassword() { return keystorePassword; }
    public String getTruststorePath() { return truststorePath; }
    public String getTruststorePassword() { return truststorePassword; }
    public String getKeyPassword() { return keyPassword; }
    public SaslMechanism getSaslMechanism() { return saslMechanism; }
    public String getSaslUsername() { return saslUsername; }
    public String getSaslPassword() { return saslPassword; }
    public String getSaslJaasConfig() { return saslJaasConfig; }
    public String getSecurityProtocol() { return securityProtocol; }
    public Map<String, String> getAdditionalProperties() { return additionalProperties; }
    public boolean isClientAuthRequired() { return clientAuthRequired; }
    public String getSslEndpointIdentificationAlgorithm() { return sslEndpointIdentificationAlgorithm; }
    public String getSslProtocol() { return sslProtocol; }
    public String getSslProvider() { return sslProvider; }
    public String getSslCipherSuites() { return sslCipherSuites; }
    public String getSslEnabledProtocols() { return sslEnabledProtocols; }
    
    /**
     * Convert security configuration to Kafka properties.
     */
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        
        if (securityProtocol != null) {
            props.put("security.protocol", securityProtocol);
        }
        
        if (sslEnabled) {
            if (keystorePath != null) {
                props.put("ssl.keystore.location", keystorePath);
            }
            if (keystorePassword != null) {
                props.put("ssl.keystore.password", keystorePassword);
            }
            if (truststorePath != null) {
                props.put("ssl.truststore.location", truststorePath);
            }
            if (truststorePassword != null) {
                props.put("ssl.truststore.password", truststorePassword);
            }
            if (keyPassword != null) {
                props.put("ssl.key.password", keyPassword);
            }
            if (sslProtocol != null) {
                props.put("ssl.protocol", sslProtocol);
            }
            if (sslProvider != null) {
                props.put("ssl.provider", sslProvider);
            }
            if (sslCipherSuites != null) {
                props.put("ssl.cipher.suites", sslCipherSuites);
            }
            if (sslEnabledProtocols != null) {
                props.put("ssl.enabled.protocols", sslEnabledProtocols);
            }
            if (sslEndpointIdentificationAlgorithm != null) {
                props.put("ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
            }
            props.put("ssl.client.auth", clientAuthRequired ? "required" : "none");
        }
        
        if (saslMechanism != null) {
            props.put("sasl.mechanism", saslMechanism.getValue());
            if (saslJaasConfig != null) {
                props.put("sasl.jaas.config", saslJaasConfig);
            }
        }
        
        // Add any additional properties
        props.putAll(additionalProperties);
        
        return props;
    }
    
    public enum SaslMechanism {
        PLAIN("PLAIN"),
        SCRAM_SHA_256("SCRAM-SHA-256"),
        SCRAM_SHA_512("SCRAM-SHA-512"),
        GSSAPI("GSSAPI"),
        OAUTHBEARER("OAUTHBEARER");
        
        private final String value;
        
        SaslMechanism(String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
    }
    
    public static class Builder {
        private boolean sslEnabled = false;
        private SSLContext sslContext;
        private String keystorePath;
        private String keystorePassword;
        private String truststorePath;
        private String truststorePassword;
        private String keyPassword;
        private SaslMechanism saslMechanism;
        private String saslUsername;
        private String saslPassword;
        private String saslJaasConfig;
        private String securityProtocol = "PLAINTEXT";
        private Map<String, String> additionalProperties = Map.of();
        private boolean clientAuthRequired = false;
        private String sslEndpointIdentificationAlgorithm = "https";
        private String sslProtocol = "TLSv1.3";
        private String sslProvider;
        private String sslCipherSuites;
        private String sslEnabledProtocols;
        
        public Builder enableSsl() {
            this.sslEnabled = true;
            this.securityProtocol = "SSL";
            return this;
        }
        
        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }
        
        public Builder keystore(String path, String password) {
            this.keystorePath = path;
            this.keystorePassword = password;
            return this;
        }
        
        public Builder truststore(String path, String password) {
            this.truststorePath = path;
            this.truststorePassword = password;
            return this;
        }
        
        public Builder keyPassword(String password) {
            this.keyPassword = password;
            return this;
        }
        
        public Builder saslPlain(String username, String password) {
            this.saslMechanism = SaslMechanism.PLAIN;
            this.saslUsername = username;
            this.saslPassword = password;
            this.saslJaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, password
            );
            this.securityProtocol = sslEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            return this;
        }
        
        public Builder saslScram256(String username, String password) {
            this.saslMechanism = SaslMechanism.SCRAM_SHA_256;
            this.saslUsername = username;
            this.saslPassword = password;
            this.saslJaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password
            );
            this.securityProtocol = sslEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            return this;
        }
        
        public Builder saslScram512(String username, String password) {
            this.saslMechanism = SaslMechanism.SCRAM_SHA_512;
            this.saslUsername = username;
            this.saslPassword = password;
            this.saslJaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password
            );
            this.securityProtocol = sslEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            return this;
        }
        
        public Builder saslJaasConfig(String jaasConfig) {
            this.saslJaasConfig = jaasConfig;
            return this;
        }
        
        public Builder securityProtocol(String protocol) {
            this.securityProtocol = protocol;
            return this;
        }
        
        public Builder clientAuthRequired() {
            this.clientAuthRequired = true;
            return this;
        }
        
        public Builder sslProtocol(String protocol) {
            this.sslProtocol = protocol;
            return this;
        }
        
        public Builder sslProvider(String provider) {
            this.sslProvider = provider;
            return this;
        }
        
        public Builder sslCipherSuites(String cipherSuites) {
            this.sslCipherSuites = cipherSuites;
            return this;
        }
        
        public Builder sslEnabledProtocols(String enabledProtocols) {
            this.sslEnabledProtocols = enabledProtocols;
            return this;
        }
        
        public Builder disableEndpointIdentification() {
            this.sslEndpointIdentificationAlgorithm = "";
            return this;
        }
        
        public Builder additionalProperties(Map<String, String> properties) {
            this.additionalProperties = Map.copyOf(properties);
            return this;
        }
        
        public SecurityConfig build() {
            // Validation
            if (sslEnabled && keystorePath == null && sslContext == null) {
                throw new IllegalStateException("SSL enabled but neither keystore path nor SSL context provided");
            }
            
            return new SecurityConfig(this);
        }
    }
}
