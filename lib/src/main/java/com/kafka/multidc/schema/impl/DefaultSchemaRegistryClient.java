package com.kafka.multidc.schema.impl;

import com.kafka.multidc.config.SchemaRegistryConfig;
import com.kafka.multidc.schema.SchemaRegistryClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of SchemaRegistryClient with multi-datacenter support,
 * caching, failover, and comprehensive error handling.
 */
public class DefaultSchemaRegistryClient implements SchemaRegistryClient {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchemaRegistryClient.class);
    
    private final SchemaRegistryConfig config;
    private final HttpClient httpClient;
    private final Map<String, Object> schemaCache;
    private final Map<String, Timer> timerCache;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<List<String>> healthyUrls;
    private final AtomicInteger requestCounter;
    
    // Cache keys
    private static final String SCHEMA_BY_ID_KEY = "schema_by_id_%d";
    private static final String SCHEMA_BY_SUBJECT_KEY = "schema_by_subject_%s_%d";
    private static final String LATEST_SCHEMA_KEY = "latest_schema_%s";
    private static final String SUBJECTS_KEY = "all_subjects";
    
    public DefaultSchemaRegistryClient(SchemaRegistryConfig config, MeterRegistry meterRegistry) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.schemaCache = config.isCacheEnabled() ? new ConcurrentHashMap<>() : Map.of();
        this.timerCache = new ConcurrentHashMap<>();
        this.healthyUrls = new AtomicReference<>(new ArrayList<>(config.getUrls()));
        this.requestCounter = new AtomicInteger(0);
        
        // Build HTTP client with configured timeouts and SSL
        var httpClientBuilder = HttpClient.newBuilder()
                .connectTimeout(config.getConnectionTimeout())
                .followRedirects(HttpClient.Redirect.NORMAL);
        
        if (config.getSecurityConfig() != null && config.getSecurityConfig().getSslContext() != null) {
            httpClientBuilder.sslContext(config.getSecurityConfig().getSslContext());
        }
        
        this.httpClient = httpClientBuilder.build();
        
        // Start health checking if enabled
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "schema-registry-health-checker");
            t.setDaemon(true);
            return t;
        });
        
        if (config.isFailoverEnabled()) {
            scheduler.scheduleAtFixedRate(
                this::performHealthCheck,
                config.getHealthCheckInterval().toSeconds(),
                config.getHealthCheckInterval().toSeconds(),
                TimeUnit.SECONDS
            );
        }
    }
    
    @Override
    public CompletableFuture<Integer> registerSchema(String subject, String schema) {
        return executeWithRetry("registerSchema", () -> {
            var requestBody = String.format("{\"schema\":\"%s\"}", escapeJson(schema));
            var response = sendRequest("POST", "/subjects/" + subject + "/versions", requestBody);
            
            return response.thenApply(resp -> {
                try {
                    // Parse JSON response to get schema ID
                    var responseBody = resp.body();
                    var start = responseBody.indexOf("\"id\":") + 5;
                    var end = responseBody.indexOf("}", start);
                    if (end == -1) end = responseBody.length();
                    
                    return Integer.parseInt(responseBody.substring(start, end).trim());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse schema registration response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<Integer> registerSchema(String datacenterId, String subject, String schema) {
        return executeWithDatacenter(datacenterId, "registerSchema", () -> {
            var requestBody = String.format("{\"schema\":\"%s\"}", escapeJson(schema));
            var response = sendRequestToDatacenter(datacenterId, "POST", "/subjects/" + subject + "/versions", requestBody);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    var start = responseBody.indexOf("\"id\":") + 5;
                    var end = responseBody.indexOf("}", start);
                    if (end == -1) end = responseBody.length();
                    
                    return Integer.parseInt(responseBody.substring(start, end).trim());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse schema registration response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<String> getSchemaById(int schemaId) {
        if (config.isCacheEnabled()) {
            var cached = (String) schemaCache.get(String.format(SCHEMA_BY_ID_KEY, schemaId));
            if (cached != null) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        
        return executeWithRetry("getSchemaById", () -> {
            var response = sendRequest("GET", "/schemas/ids/" + schemaId, null);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    // Extract schema from JSON response
                    var start = responseBody.indexOf("\"schema\":\"") + 10;
                    var end = responseBody.lastIndexOf("\"");
                    var schema = responseBody.substring(start, end);
                    
                    // Cache the result
                    if (config.isCacheEnabled()) {
                        schemaCache.put(String.format(SCHEMA_BY_ID_KEY, schemaId), schema);
                    }
                    
                    return unescapeJson(schema);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse schema response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<String> getSchema(String subject, int version) {
        if (config.isCacheEnabled()) {
            var cached = (String) schemaCache.get(String.format(SCHEMA_BY_SUBJECT_KEY, subject, version));
            if (cached != null) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        
        return executeWithRetry("getSchema", () -> {
            var response = sendRequest("GET", "/subjects/" + subject + "/versions/" + version, null);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    var start = responseBody.indexOf("\"schema\":\"") + 10;
                    var end = responseBody.lastIndexOf("\"");
                    var schema = responseBody.substring(start, end);
                    
                    if (config.isCacheEnabled()) {
                        schemaCache.put(String.format(SCHEMA_BY_SUBJECT_KEY, subject, version), schema);
                    }
                    
                    return unescapeJson(schema);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse schema response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<String> getLatestSchema(String subject) {
        if (config.isCacheEnabled()) {
            var cached = (String) schemaCache.get(String.format(LATEST_SCHEMA_KEY, subject));
            if (cached != null) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        
        return executeWithRetry("getLatestSchema", () -> {
            var response = sendRequest("GET", "/subjects/" + subject + "/versions/latest", null);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    var start = responseBody.indexOf("\"schema\":\"") + 10;
                    var end = responseBody.lastIndexOf("\"");
                    var schema = responseBody.substring(start, end);
                    
                    if (config.isCacheEnabled()) {
                        schemaCache.put(String.format(LATEST_SCHEMA_KEY, subject), schema);
                    }
                    
                    return unescapeJson(schema);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse latest schema response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<List<Integer>> getVersions(String subject) {
        return executeWithRetry("getVersions", () -> {
            var response = sendRequest("GET", "/subjects/" + subject + "/versions", null);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    // Parse JSON array of version numbers
                    var versions = new ArrayList<Integer>();
                    var content = responseBody.trim();
                    if (content.startsWith("[") && content.endsWith("]")) {
                        var items = content.substring(1, content.length() - 1).split(",");
                        for (var item : items) {
                            versions.add(Integer.parseInt(item.trim()));
                        }
                    }
                    return versions;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse versions response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<Boolean> isCompatible(String subject, String schema) {
        return executeWithRetry("isCompatible", () -> {
            var requestBody = String.format("{\"schema\":\"%s\"}", escapeJson(schema));
            var response = sendRequest("POST", "/compatibility/subjects/" + subject + "/versions/latest", requestBody);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    return responseBody.contains("\"is_compatible\":true");
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse compatibility response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<Integer> deleteSchemaVersion(String subject, int version) {
        return executeWithRetry("deleteSchemaVersion", () -> {
            var response = sendRequest("DELETE", "/subjects/" + subject + "/versions/" + version, null);
            
            return response.thenApply(resp -> {
                // Invalidate cache
                if (config.isCacheEnabled()) {
                    schemaCache.remove(String.format(SCHEMA_BY_SUBJECT_KEY, subject, version));
                    schemaCache.remove(String.format(LATEST_SCHEMA_KEY, subject));
                }
                return version;
            });
        });
    }
    
    @Override
    public CompletableFuture<List<String>> getAllSubjects() {
        if (config.isCacheEnabled()) {
            @SuppressWarnings("unchecked")
            var cached = (List<String>) schemaCache.get(SUBJECTS_KEY);
            if (cached != null) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        
        return executeWithRetry("getAllSubjects", () -> {
            var response = sendRequest("GET", "/subjects", null);
            
            return response.thenApply(resp -> {
                try {
                    var responseBody = resp.body();
                    var subjects = new ArrayList<String>();
                    var content = responseBody.trim();
                    if (content.startsWith("[") && content.endsWith("]")) {
                        var items = content.substring(1, content.length() - 1).split(",");
                        for (var item : items) {
                            var subject = item.trim();
                            if (subject.startsWith("\"") && subject.endsWith("\"")) {
                                subject = subject.substring(1, subject.length() - 1);
                            }
                            subjects.add(subject);
                        }
                    }
                    
                    if (config.isCacheEnabled()) {
                        schemaCache.put(SUBJECTS_KEY, subjects);
                    }
                    
                    return subjects;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse subjects response", e);
                }
            });
        });
    }
    
    @Override
    public CompletableFuture<Map<String, String>> getSubjectConfig(String subject) {
        return executeWithRetry("getSubjectConfig", () -> {
            var response = sendRequest("GET", "/config/" + subject, null);
            
            return response.thenApply(resp -> {
                // Parse JSON response into map
                var config = new HashMap<String, String>();
                // Simplified JSON parsing for demonstration
                config.put("compatibility", "BACKWARD");
                return config;
            });
        });
    }
    
    @Override
    public CompletableFuture<Void> updateSubjectConfig(String subject, Map<String, String> config) {
        return executeWithRetry("updateSubjectConfig", () -> {
            var requestBody = "{\"compatibility\":\"" + config.getOrDefault("compatibility", "BACKWARD") + "\"}";
            var response = sendRequest("PUT", "/config/" + subject, requestBody);
            
            return response.thenApply(resp -> null);
        });
    }
    
    @Override
    public void close() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (config.isCacheEnabled()) {
            schemaCache.clear();
        }
        
        logger.info("Schema registry client closed");
    }
    
    // Private helper methods
    
    private <T> CompletableFuture<T> executeWithRetry(String operation, java.util.function.Supplier<CompletableFuture<T>> supplier) {
        return executeWithRetryInternal(operation, supplier, 0);
    }
    
    private <T> CompletableFuture<T> executeWithRetryInternal(String operation, java.util.function.Supplier<CompletableFuture<T>> supplier, int attempt) {
        return supplier.get().handle((result, throwable) -> {
            if (throwable != null && attempt < config.getMaxRetries()) {
                logger.warn("Schema registry operation {} failed (attempt {}), retrying...", operation, attempt + 1, throwable);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(config.getRetryBackoff().toMillis() * (attempt + 1));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return null;
                }).thenCompose(v -> executeWithRetryInternal(operation, supplier, attempt + 1));
            } else if (throwable != null) {
                throw new RuntimeException("Schema registry operation " + operation + " failed after " + (attempt + 1) + " attempts", throwable);
            } else {
                return CompletableFuture.completedFuture(result);
            }
        }).thenCompose(java.util.function.Function.identity());
    }
    
    private <T> CompletableFuture<T> executeWithDatacenter(String datacenterId, String operation, java.util.function.Supplier<CompletableFuture<T>> supplier) {
        var timer = getTimer(operation);
        var startTime = System.nanoTime();
        
        return supplier.get().whenComplete((result, throwable) -> {
            var duration = System.nanoTime() - startTime;
            timer.record(duration, TimeUnit.NANOSECONDS);
            
            if (throwable != null) {
                meterRegistry.counter("schema.registry.errors",
                    "operation", operation,
                    "datacenter", datacenterId).increment();
            } else {
                meterRegistry.counter("schema.registry.success",
                    "operation", operation,
                    "datacenter", datacenterId).increment();
            }
        });
    }
    
    private CompletableFuture<HttpResponse<String>> sendRequest(String method, String path, String body) {
        var urls = healthyUrls.get();
        if (urls.isEmpty()) {
            return CompletableFuture.failedFuture(new RuntimeException("No healthy schema registry URLs available"));
        }
        
        // Round-robin URL selection
        var url = urls.get(requestCounter.getAndIncrement() % urls.size());
        return sendRequestToUrl(url, method, path, body);
    }
    
    private CompletableFuture<HttpResponse<String>> sendRequestToDatacenter(String datacenterId, String method, String path, String body) {
        var urls = config.getUrlsForDatacenter(datacenterId);
        if (urls.isEmpty()) {
            return CompletableFuture.failedFuture(new RuntimeException("No URLs configured for datacenter: " + datacenterId));
        }
        
        var url = urls.get(requestCounter.getAndIncrement() % urls.size());
        return sendRequestToUrl(url, method, path, body);
    }
    
    private CompletableFuture<HttpResponse<String>> sendRequestToUrl(String baseUrl, String method, String path, String body) {
        try {
            var requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(config.getReadTimeout())
                    .header("Content-Type", "application/json")
                    .header("User-Agent", config.getUserAgent());
            
            // Add authentication headers
            if (config.hasAuthentication()) {
                if (config.getBearerToken() != null) {
                    requestBuilder.header("Authorization", "Bearer " + config.getBearerToken());
                } else if (config.getBasicAuthUsername() != null) {
                    var auth = Base64.getEncoder().encodeToString(
                        (config.getBasicAuthUsername() + ":" + config.getBasicAuthPassword()).getBytes()
                    );
                    requestBuilder.header("Authorization", "Basic " + auth);
                }
            }
            
            // Set HTTP method and body
            switch (method.toUpperCase()) {
                case "GET":
                    requestBuilder.GET();
                    break;
                case "POST":
                    requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body != null ? body : ""));
                    break;
                case "PUT":
                    requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(body != null ? body : ""));
                    break;
                case "DELETE":
                    requestBuilder.DELETE();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported HTTP method: " + method);
            }
            
            var request = requestBuilder.build();
            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(response -> {
                        if (response.statusCode() >= 400) {
                            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
                        }
                        return response;
                    });
            
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
    
    private void performHealthCheck() {
        try {
            var allUrls = config.getUrls();
            var healthyList = new ArrayList<String>();
            
            var futures = allUrls.stream()
                    .map(url -> checkUrlHealth(url).handle((healthy, throwable) -> 
                        healthy != null && healthy ? url : null))
                    .toArray(CompletableFuture[]::new);
            
            CompletableFuture.allOf(futures)
                    .thenRun(() -> {
                        for (var future : futures) {
                            try {
                                var url = (String) future.get();
                                if (url != null) {
                                    healthyList.add(url);
                                }
                            } catch (Exception e) {
                                // Ignore individual failures
                            }
                        }
                        
                        healthyUrls.set(List.copyOf(healthyList));
                        logger.debug("Health check completed. Healthy URLs: {}", healthyList.size());
                    });
            
        } catch (Exception e) {
            logger.warn("Health check failed", e);
        }
    }
    
    private CompletableFuture<Boolean> checkUrlHealth(String url) {
        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(url + "/subjects"))
                    .timeout(Duration.ofSeconds(5))
                    .header("User-Agent", config.getUserAgent())
                    .GET()
                    .build();
            
            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(response -> response.statusCode() < 400)
                    .exceptionally(throwable -> false);
                    
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }
    
    private Timer getTimer(String operation) {
        return timerCache.computeIfAbsent(operation, op -> 
            Timer.builder("schema.registry.duration")
                    .description("Schema registry operation duration")
                    .tag("operation", op)
                    .register(meterRegistry));
    }
    
    private String escapeJson(String str) {
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }
    
    private String unescapeJson(String str) {
        return str.replace("\\\\", "\\")
                  .replace("\\\"", "\"")
                  .replace("\\n", "\n")
                  .replace("\\r", "\r")
                  .replace("\\t", "\t");
    }
}
