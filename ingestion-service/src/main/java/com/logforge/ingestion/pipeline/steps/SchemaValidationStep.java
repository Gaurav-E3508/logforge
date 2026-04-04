package com.logforge.ingestion.pipeline.steps;

import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.pipeline.PipelineStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 2: Validate that the event has all required fields.
 * Malformed events go to the dead-letter topic (handled by the consumer).
 */
@Slf4j
@Component
public class SchemaValidationStep implements PipelineStep {

    @Override
    public LogEvent process(LogEvent event) {

        // Required: serviceName
        if (event.getServiceName() == null || event.getServiceName().isBlank()) {
            log.warn("Dropping event — missing serviceName. eventId: {}", event.getEventId());
            return null;
        }

        // Required: message
        if (event.getMessage() == null || event.getMessage().isBlank()) {
            log.warn("Dropping event — missing message. eventId: {}", event.getEventId());
            return null;
        }

        // Required: level
        if (event.getLevel() == null) {
            log.warn("Dropping event — missing level. eventId: {}", event.getEventId());
            return null;
        }

        // Required: timestamp must be reasonable (after year 2020)
        if (event.getTimestamp() < 1_577_836_800_000L) {
            log.warn("Dropping event — invalid timestamp: {}. eventId: {}",
                    event.getTimestamp(), event.getEventId());
            return null;
        }

        return event;
    }

    @Override public String stepName() { return "SCHEMA_VALIDATION"; }
}