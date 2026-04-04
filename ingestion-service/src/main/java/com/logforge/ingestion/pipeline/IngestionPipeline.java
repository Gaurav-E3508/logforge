package com.logforge.ingestion.pipeline;

import com.logforge.common.model.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Orchestrates all pipeline steps in sequence.
 *
 * Flow: DeduplicationStep → SchemaValidationStep
 *       → GeoIpEnrichmentStep → LogClassificationStep
 *
 * If any step returns null, the event is dropped and pipeline stops.
 * This is the classic Chain of Responsibility pattern.
 */
@Slf4j
@Component
public class IngestionPipeline {

    private final List<PipelineStep> steps;

    // Spring injects all PipelineStep beans in declaration order
    public IngestionPipeline(List<PipelineStep> steps) {
        this.steps = steps;
        log.info("Ingestion pipeline initialized with {} steps: {}",
                steps.size(),
                steps.stream().map(PipelineStep::stepName).toList());
    }

    /**
     * Run the event through all pipeline steps.
     * @return processed event, or empty if dropped by any step
     */
    public java.util.Optional<LogEvent> process(LogEvent event) {
        LogEvent current = event;

        for (PipelineStep step : steps) {
            current = step.process(current);
            if (current == null) {
                log.debug("Event dropped at step: {} — eventId: {}",
                        step.stepName(), event.getEventId());
                return java.util.Optional.empty();
            }
        }

        return java.util.Optional.of(current);
    }
}