package com.logforge.sdk.service;

import com.logforge.common.model.LogEvent;
import com.logforge.sdk.buffer.LogEventRingBuffer;
import com.logforge.sdk.config.LogAgentProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

/**
 * The main entry point for developers using the SDK.
 *
 * Usage in any Spring Boot service that imports log-agent-sdk:
 *
 *   @Autowired LogCaptureService logger;
 *
 *   // Log a custom event
 *   logger.info("Order placed successfully", Map.of("orderId", "ord_123"));
 *
 *   // Log an exception
 *   logger.error("Payment failed", exception, Map.of("userId", "u_991"));
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LogCaptureService {

    private final LogEventRingBuffer ringBuffer;
    private final LogAgentProperties properties;

    /** Capture any pre-built LogEvent */
    public void capture(LogEvent event) {
        // Stamp the ingestion time
        LogEvent stamped = LogEvent.builder()
                .eventId(event.getEventId() != null ? event.getEventId() : UUID.randomUUID().toString())
                .timestamp(event.getTimestamp() > 0 ? event.getTimestamp() : System.currentTimeMillis())
                .ingestedAt(System.currentTimeMillis())
                .level(event.getLevel())
                .serviceName(event.getServiceName() != null
                        ? event.getServiceName() : properties.getServiceName())
                .hostName(getHostName())
                .environment(event.getEnvironment() != null
                        ? event.getEnvironment() : properties.getEnvironment())
                .message(event.getMessage())
                .stackTrace(event.getStackTrace())
                .traceId(event.getTraceId())
                .spanId(event.getSpanId())
                .sourceIp(event.getSourceIp())
                .category(event.getCategory())
                .tags(event.getTags())
                .build();

        ringBuffer.tryPublish(stamped);
    }

    // Convenience methods for developers

    public void info(String message, Map<String, String> tags) {
        capture(buildEvent(LogEvent.LogLevel.INFO, message, null, tags));
    }

    public void warn(String message, Map<String, String> tags) {
        capture(buildEvent(LogEvent.LogLevel.WARN, message, null, tags));
    }

    public void error(String message, Throwable ex, Map<String, String> tags) {
        capture(buildEvent(LogEvent.LogLevel.ERROR, message, ex, tags));
    }

    public void error(String message, Map<String, String> tags) {
        capture(buildEvent(LogEvent.LogLevel.ERROR, message, null, tags));
    }

    private LogEvent buildEvent(LogEvent.LogLevel level,
                                String message,
                                Throwable ex,
                                Map<String, String> tags) {
        return LogEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .timestamp(System.currentTimeMillis())
                .level(level)
                .serviceName(properties.getServiceName())
                .environment(properties.getEnvironment())
                .message(message)
                .stackTrace(ex != null ? getStackTrace(ex) : null)
                .category(ex != null ? "EXCEPTION" : "CUSTOM_EVENT")
                .tags(tags)
                .build();
    }

    private String getStackTrace(Throwable ex) {
        java.io.StringWriter sw = new java.io.StringWriter();
        ex.printStackTrace(new java.io.PrintWriter(sw));
        return sw.toString();
    }

    private String getHostName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}