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

import javax.net.ssl.SSLContext;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise-grade SSL context factory for secure Kafka connections.
 * Supports custom keystores, truststores, and SSL configurations.
 */
public class SSLContextFactory {
    
    private static final Map<String, SSLContext> SSL_CONTEXT_CACHE = new ConcurrentHashMap<>();
    
    /**
     * Create SSL context from keystore and truststore.
     *
     * @param keystorePath path to keystore file
     * @param keystorePassword keystore password
     * @param keystoreType keystore type (default: JKS)
     * @param truststorePath path to truststore file
     * @param truststorePassword truststore password
     * @param truststoreType truststore type (default: JKS)
     * @param protocol SSL protocol (default: TLSv1.2)
     * @return configured SSL context
     */
    public static SSLContext createSSLContext(
            String keystorePath,
            String keystorePassword,
            String keystoreType,
            String truststorePath,
            String truststorePassword,
            String truststoreType,
            String protocol) {
        
        String cacheKey = String.join("|", 
            keystorePath != null ? keystorePath : "",
            truststorePath != null ? truststorePath : "",
            protocol != null ? protocol : "TLSv1.2"
        );
        
        return SSL_CONTEXT_CACHE.computeIfAbsent(cacheKey, k -> {
            try {
                return buildSSLContext(
                    keystorePath, keystorePassword, keystoreType != null ? keystoreType : "JKS",
                    truststorePath, truststorePassword, truststoreType != null ? truststoreType : "JKS",
                    protocol != null ? protocol : "TLSv1.2"
                );
            } catch (Exception e) {
                throw new SecurityException("Failed to create SSL context", e);
            }
        });
    }
    
    /**
     * Create SSL context with default settings.
     *
     * @param keystorePath path to keystore file
     * @param keystorePassword keystore password
     * @param truststorePath path to truststore file
     * @param truststorePassword truststore password
     * @return configured SSL context
     */
    public static SSLContext createSSLContext(
            String keystorePath,
            String keystorePassword,
            String truststorePath,
            String truststorePassword) {
        return createSSLContext(
            keystorePath, keystorePassword, "JKS",
            truststorePath, truststorePassword, "JKS",
            "TLSv1.2"
        );
    }
    
    private static SSLContext buildSSLContext(
            String keystorePath,
            String keystorePassword,
            String keystoreType,
            String truststorePath,
            String truststorePassword,
            String truststoreType,
            String protocol) throws Exception {
        
        SSLContext sslContext = SSLContext.getInstance(protocol);
        
        // Load keystore if provided
        KeyManagerFactory kmf = null;
        if (keystorePath != null && keystorePassword != null) {
            KeyStore keystore = loadKeyStore(keystorePath, keystorePassword, keystoreType);
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keystore, keystorePassword.toCharArray());
        }
        
        // Load truststore if provided
        TrustManagerFactory tmf = null;
        if (truststorePath != null && truststorePassword != null) {
            KeyStore truststore = loadKeyStore(truststorePath, truststorePassword, truststoreType);
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore);
        }
        
        sslContext.init(
            kmf != null ? kmf.getKeyManagers() : null,
            tmf != null ? tmf.getTrustManagers() : null,
            null
        );
        
        return sslContext;
    }
    
    private static KeyStore loadKeyStore(String path, String password, String type)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore keyStore = KeyStore.getInstance(type);
        try (FileInputStream fis = new FileInputStream(path)) {
            keyStore.load(fis, password.toCharArray());
        }
        return keyStore;
    }
    
    /**
     * Clear SSL context cache.
     */
    public static void clearCache() {
        SSL_CONTEXT_CACHE.clear();
    }
    
    /**
     * Get cache size for monitoring.
     *
     * @return number of cached SSL contexts
     */
    public static int getCacheSize() {
        return SSL_CONTEXT_CACHE.size();
    }
}
