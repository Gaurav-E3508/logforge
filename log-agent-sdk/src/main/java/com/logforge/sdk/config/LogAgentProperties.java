package com.logforge.sdk.config;

import com.logforge.common.model.LogEvent;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * All SDK settings — configurable from the importing app's application.properties.
 *
 * Example usage in any Spring Boot app:
 *   logforge.service-name=payment-service
 *   logforge.environment=PRODUCTION
 *   logforge.buffer-size=4096
 *   logforge.excluded-paths=/health,/actuator
 */
@Data
@ConfigurationProperties(prefix = "logforge")
public class LogAgentProperties {

    /** Name of the service using this SDK — appears in every log event */
    private String serviceName = "unknown-service";

    /** Environment tag attached to every event */
    private LogEvent.Environment environment = LogEvent.Environment.DEVELOPMENT;

    /** Kafka bootstrap servers */
    private String kafkaBootstrapServers = "localhost:9092";

    /**
     * Ring buffer size — MUST be a power of 2.
     * 4096 means up to 4096 events can be buffered before the producer
     * catches up. If your service is very high traffic, increase to 8192.
     */
    private int bufferSize = 4096;

    /** How many events to batch before sending to Kafka */
    private int batchSize = 100;

    /** Max milliseconds to wait before flushing a partial batch */
    private long batchFlushMs = 500;

    /** HTTP paths to skip — health checks, actuator endpoints etc. */
    private List<String> excludedPaths = List.of(
            "/health", "/actuator", "/favicon.ico"
    );

    /** Minimum log level to capture — events below this are ignored */
    private LogEvent.LogLevel minimumLevel = LogEvent.LogLevel.INFO;
}