package com.logforge.ingestion.pipeline.steps;

import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.pipeline.PipelineStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 4: Classify the log into a category based on content patterns.
 *
 * Categories help with:
 * - Routing (DB errors go to a different alert channel than security events)
 * - Dashboards (show me all SECURITY events in the last hour)
 * - Pattern detection in the Stream Processor
 */
@Slf4j
@Component
public class LogClassificationStep implements PipelineStep {

    @Override
    public LogEvent process(LogEvent event) {
        // Skip if already classified (e.g., SDK set it to "HTTP_REQUEST")
        if (event.getCategory() != null && !event.getCategory().isBlank()) {
            return event;
        }

        String category = classify(event);

        return LogEvent.builder()
                .eventId(event.getEventId())
                .timestamp(event.getTimestamp())
                .ingestedAt(event.getIngestedAt())
                .level(event.getLevel())
                .serviceName(event.getServiceName())
                .hostName(event.getHostName())
                .environment(event.getEnvironment())
                .category(category)
                .message(event.getMessage())
                .stackTrace(event.getStackTrace())
                .traceId(event.getTraceId())
                .spanId(event.getSpanId())
                .sourceIp(event.getSourceIp())
                .country(event.getCountry())
                .city(event.getCity())
                .tags(event.getTags())
                .kafkaPartition(event.getKafkaPartition())
                .kafkaOffset(event.getKafkaOffset())
                .storageTier(event.getStorageTier())
                .build();
    }

    private String classify(LogEvent event) {
        String msg = event.getMessage() != null
                ? event.getMessage().toLowerCase() : "";
        String trace = event.getStackTrace() != null
                ? event.getStackTrace().toLowerCase() : "";
        String combined = msg + " " + trace;

        // Database errors
        if (combined.contains("sql") || combined.contains("jdbc")
                || combined.contains("hibernate") || combined.contains("datasource")
                || combined.contains("connection pool")) {
            return "DATABASE_ERROR";
        }

        // Network / external call failures
        if (combined.contains("timeout") || combined.contains("connection refused")
                || combined.contains("socket") || combined.contains("http client")) {
            return "EXTERNAL_CALL_FAILURE";
        }

        // Security events
        if (combined.contains("unauthorized") || combined.contains("403")
                || combined.contains("authentication failed")
                || combined.contains("jwt") || combined.contains("token expired")) {
            return "SECURITY_EVENT";
        }

        // Out of memory / JVM errors
        if (combined.contains("outofmemory") || combined.contains("stackoverflow")
                || combined.contains("gc overhead")) {
            return "JVM_ERROR";
        }

        // NullPointerException and friends
        if (combined.contains("nullpointerexception") || combined.contains("npe")) {
            return "NULL_POINTER";
        }

        // Generic exception
        if (event.getStackTrace() != null && !event.getStackTrace().isBlank()) {
            return "EXCEPTION";
        }

        // Default
        return "APPLICATION_LOG";
    }

    @Override public String stepName() { return "LOG_CLASSIFICATION"; }
}